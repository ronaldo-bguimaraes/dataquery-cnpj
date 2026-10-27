"""Manual Raw -> Bronze DAG for one CNPJ competence.

The DAG does no work at import time. Domain modules are imported inside the task
callables, which only run after a manual trigger. Each selected ZIP gets a mapped
pair ``baixar_raw -> converter_bronze``; a final consolidation task fails the run
when any file failed, without cancelling independent files.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from airflow.sdk import DAG, Param, task, task_group

log = logging.getLogger(__name__)

DOC_MD = """
# Download e conversão CNPJ (Raw → Bronze)

Dispara manualmente uma competência (`AAAA-MM`) da Receita Federal. Para cada ZIP
selecionado, a DAG executa duas tarefas independentes:

1. `baixar_raw`: baixa o ZIP para `raw/<AAAA-MM>/<nome-original>` no armazenamento
   configurado ou reutiliza um objeto já existente e não vazio.
2. `converter_bronze`: lê o ZIP do Raw e publica
   `bronze/<AAAA-MM>/<nome-base>.parquet`.

## Como disparar

```shell
docker compose exec airflow-scheduler \\
  airflow dags test cnpj_download_manual 2026-10-27 \\
  -c '{"competencia": "2026-02", "limite": 1}'
```

Na UI: **DAGs → cnpj_download_manual → Trigger DAG** com:

```json
{"competencia": "2026-02", "limite": 1}
```

`limite` é opcional e seleciona os primeiros N nomes em ordem estável **antes** de
verificar o Raw. Um objeto Raw existente conta no limite e ainda é convertido.

## Resultado

O XCom e o log contêm apenas nomes, chaves, estados, bytes, linhas e durações. URL
da RFB, token, credenciais e conteúdo binário nunca entram no XCom.

Uma falha de download impede somente a conversão daquele ZIP; arquivos independentes
continuam. Se qualquer arquivo falhar, o run termina `failed`, preservando Raw e
Bronze já publicados para retry.

## Configuração

O backend é escolhido por `CNPJ_STORAGE_BACKEND`:

- `filesystem` (default local): grava em `CNPJ_DATA_ROOT` (`/data` nos containers,
  montado a partir de `data/` na raiz do repositório).
- `s3`/`r2`: usa um endpoint compatível com S3 (Cloudflare R2 em produção), com
  `CNPJ_S3_ENDPOINT`, `CNPJ_S3_BUCKET`, `CNPJ_S3_ACCESS_KEY`,
  `CNPJ_S3_SECRET_KEY` e, opcionalmente, `CNPJ_S3_REGION`.

A troca de backend não altera a DAG nem as chaves lógicas. No Compose local, apenas
`.env` na raiz (ignorado pelo git) é necessário.

A DAG não agenda execuções (`schedule=None`), não faz catchup e limita a uma
execução ativa por vez (`max_active_runs=1`).
"""


def _get_store():
    """Build the configured backend inside the task callable.

    Importing ``build_store_from_env`` lazily keeps DAG parsing free of boto3 and
    of any filesystem access.
    """

    from src.storage.factory import build_store_from_env

    return build_store_from_env()


def _listar_arquivos(
    competencia: str | None,
    limite: int | None,
    *,
    store=None,
    client=None,
) -> dict[str, Any]:
    from src.pipeline.raw_bronze import listar_arquivos_payload

    return listar_arquivos_payload(
        competencia,
        limite=limite,
        store=store if store is not None else _get_store(),
        client=client,
    )


def _preparar_arquivos(listagem: dict[str, Any]) -> list[dict[str, str]]:
    competencia = listagem["competencia"]
    return [
        {"nome": item["nome"], "competencia": competencia}
        for item in listagem.get("arquivos", [])
    ]


def _nao_executado(nome: str, erro: str) -> dict[str, Any]:
    return {
        "nome": nome,
        "raw_key": None,
        "bronze_key": None,
        "status_conversao": "não executado",
        "bytes": 0,
        "linhas": None,
        "duracao_segundos": 0.0,
        "erro": erro,
    }


def _baixar_raw(
    arquivo: dict[str, Any],
    *,
    store=None,
    client=None,
) -> dict[str, Any]:
    from src.download_utils.redaction import describe_exception
    from src.extracao_rfb.download_raw import download_raw_file

    nome = arquivo["nome"]
    competencia = arquivo["competencia"]
    try:
        result = download_raw_file(
            competencia,
            nome,
            store=store if store is not None else _get_store(),
            client=client,
        )
        payload = result.to_dict()
    except Exception as exc:  # noqa: BLE001 - isolate per-file failures
        payload = {
            "nome": nome,
            "raw_key": None,
            "status_download": "falhou",
            "bytes": 0,
            "duracao_segundos": 0.0,
            "erro": describe_exception(exc),
        }

    log.info(
        "Arquivo %s: status_download=%s bytes=%s",
        nome,
        payload.get("status_download"),
        payload.get("bytes"),
    )
    return payload


def _converter_bronze(
    arquivo: dict[str, Any],
    download: dict[str, Any] | None,
    *,
    store=None,
) -> dict[str, Any]:
    from src.conversao.bronze import convert_raw_file
    from src.download_utils.redaction import describe_exception

    nome = arquivo["nome"]
    competencia = arquivo["competencia"]
    if (
        not download
        or download.get("status_download") not in {"baixado", "reutilizado"}
        or not download.get("raw_key")
    ):
        erro = (download or {}).get("erro") or "download falhou"
        log.info("Arquivo %s: conversão não executada", nome)
        return _nao_executado(nome, erro)

    try:
        result = convert_raw_file(
            competencia,
            nome,
            download["raw_key"],
            store=store if store is not None else _get_store(),
        )
        payload = result.to_dict()
    except Exception as exc:  # noqa: BLE001 - isolate per-file failures
        payload = {
            "nome": nome,
            "raw_key": download.get("raw_key"),
            "bronze_key": None,
            "status_conversao": "falhou",
            "bytes": 0,
            "linhas": None,
            "duracao_segundos": 0.0,
            "erro": describe_exception(exc),
        }

    log.info(
        "Arquivo %s: status_conversao=%s linhas=%s",
        nome,
        payload.get("status_conversao"),
        payload.get("linhas"),
    )
    return payload


def _consolidar_resumo(
    listagem: dict[str, Any] | None,
    downloads: list[dict[str, Any]] | None,
    conversoes: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    from src.pipeline.raw_bronze import (
        PipelineFailedError,
        combinar_resultados,
        consolidar_resumo,
        raise_if_failed,
    )

    if not listagem:
        raise PipelineFailedError("resumo não pode ser consolidado sem a listagem")

    resultados = combinar_resultados(downloads or [], conversoes or [])
    summary = consolidar_resumo(listagem, resultados)
    log.info("Resumo do run: %s", json.dumps(summary))
    raise_if_failed(summary)
    return summary


@task
def listar_arquivos(**context) -> dict[str, Any]:
    params = context.get("params") or {}
    payload = _listar_arquivos(params.get("competencia"), params.get("limite"))
    log.info(
        "Competência %s: encontrados=%s selecionados=%s parcial=%s",
        payload.get("competencia"),
        payload.get("encontrados"),
        payload.get("selecionados"),
        payload.get("parcial"),
    )
    return payload


@task
def preparar_arquivos(listagem: dict[str, Any]) -> list[dict[str, str]]:
    return _preparar_arquivos(listagem)


@task
def baixar_raw(arquivo: dict[str, Any]) -> dict[str, Any]:
    return _baixar_raw(arquivo)


@task
def converter_bronze(
    arquivo: dict[str, Any], download: dict[str, Any]
) -> dict[str, Any]:
    return _converter_bronze(arquivo, download)


@task_group
def pipeline_arquivo(arquivo: dict[str, Any]) -> None:
    download = baixar_raw(arquivo)
    conversion = converter_bronze(arquivo, download)
    download >> conversion


@task(trigger_rule="all_done")
def consolidar_resumo(
    listagem: dict[str, Any],
    downloads: list[dict[str, Any]],
    conversoes: list[dict[str, Any]],
) -> dict[str, Any]:
    return _consolidar_resumo(listagem, downloads, conversoes)


with DAG(
    dag_id="cnpj_download_manual",
    description=(
        "Baixa manualmente os arquivos CNPJ de uma competência e publica "
        "Raw e Bronze no armazenamento configurado (filesystem local ou R2/S3)."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["cnpj", "download", "bronze", "manual"],
    params={
        "competencia": Param(
            default=None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Competência AAAA-MM (mês de 01 a 12).",
        ),
        "limite": Param(
            default=None,
            type=["null", "integer"],
            minimum=1,
            description="Máximo de arquivos selecionados (null = todos).",
        ),
    },
    doc_md=DOC_MD,
) as dag:
    listagem = listar_arquivos()
    arquivos = preparar_arquivos(listagem)
    pipeline_arquivo.expand(arquivo=arquivos)

    # The mapped tasks are created after ``expand``; their outputs are small lists
    # of per-file payloads, never file contents.
    baixar_mapeado = dag.get_task("pipeline_arquivo.baixar_raw")
    converter_mapeado = dag.get_task("pipeline_arquivo.converter_bronze")
    consolidar_resumo(
        listagem,
        baixar_mapeado.output,
        converter_mapeado.output,
    )
