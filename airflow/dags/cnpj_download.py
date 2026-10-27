"""DAG manual que baixa os arquivos de uma competência CNPJ da Receita Federal.

A DAG não faz nenhum trabalho no import: o código de domínio é importado apenas
dentro da callable da task, que só roda quando o usuário dispara a DAG.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

log = logging.getLogger(__name__)

DOC_MD = """
# Download manual de arquivos CNPJ

Dispara manualmente o download dos arquivos de uma competência (`AAAA-MM`) da
Receita Federal para o diretório persistente configurado em `CNPJ_DOWNLOAD_DIR`.

## Como disparar

```shell
docker compose exec airflow-scheduler \\
  airflow dags test cnpj_download_manual 2026-10-27 \\
  -c '{"competencia": "2026-02"}'
```

Na UI: **DAGs → cnpj_download_manual → Trigger DAG** com:

```json
{"competencia": "2026-02"}
```

## Saída

Os arquivos são gravados em `<CNPJ_DOWNLOAD_DIR>/<AAAA-MM>/`. O resumo da execução
fica no XCom `return_value` e no log da task, sem URL ou token.

## Reexecução e concorrência

A DAG não agenda execuções (`schedule=None`) e não tenta novamente automaticamente
(`retries=0`). Rodar a mesma competência de novo ignora os arquivos já baixados.

Além de `max_active_runs=1`, um lock exclusivo (`.download.lock`) por competência
protege chamadas concorrentes feitas pela CLI ou por `airflow dags test` fora do
scheduler. Se outra execução estiver em andamento, a segunda falha com
`ExecutionInProgressError` após 5 segundos, sem tocar nos arquivos.
"""


def _baixar_arquivos(**context):
    # Import tardio: o parse da DAG não importa o pipeline nem toca a rede.
    from src.extracao_rfb.download_competencia import download_competencia

    params = context["params"]
    output_dir = os.environ.get("CNPJ_DOWNLOAD_DIR")
    if not output_dir:
        raise RuntimeError(
            "CNPJ_DOWNLOAD_DIR não definido; configure o diretório persistente de download"
        )

    summary = download_competencia(
        competencia=params.get("competencia"),
        output_dir=output_dir,
        limite=params.get("limite"),
    )

    payload = summary.to_dict()
    log.info("Resumo do download: %s", json.dumps(payload))
    return payload


with DAG(
    dag_id="cnpj_download_manual",
    description="Baixa manualmente os arquivos CNPJ de uma competência AAAA-MM.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["cnpj", "download", "manual"],
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
    baixar_arquivos = PythonOperator(
        task_id="baixar_arquivos",
        python_callable=_baixar_arquivos,
    )
