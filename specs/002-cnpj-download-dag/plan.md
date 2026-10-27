# Implementation Plan: Primeira DAG de Download de Dados CNPJ

**Branch**: `002-cnpj-download-dag` | **Date**: 2026-10-27 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-cnpj-download-dag/spec.md`

## Summary

Entregar uma DAG manual do Airflow (`cnpj_download_manual`) que recebe uma competência
`AAAA-MM`, lista os arquivos publicados pela RFB, baixa os arquivos para um diretório
persistente e termina com um resumo observável (encontrados, selecionados, baixados,
ignorados, falhas, bytes e duração). Qualquer falha de download faz a task falhar, preservando
os arquivos concluídos.

A lógica deixa de rodar no import: o script atual vira uma função de domínio
(`download_competencia`) com validação explícita, cliente RFB injetável e downloader assíncrono
com escrita atômica. A DAG é uma casca fina, com import tardio do código de pipeline, para que
o parser do Airflow não faça rede. O container ganha uma imagem com `webdavclient3`, o código
montado somente leitura e um bind mount persistente para a saída.

## Technical Context

**Language/Version**: Python 3.13 (imagem `apache/airflow:3.3.2`, verificada em 3.13.15)

**Primary Dependencies**: Airflow 3.3.2 (CeleryExecutor, Postgres e Redis do compose atual);
`webdavclient3==3.14.7` (já declarado no `requirements.txt`, ausente na imagem oficial);
`aiohttp`, `requests`, `python-dotenv`, `psutil` (já fornecidos pela imagem); `fcntl` da
biblioteca padrão para o lock de execução.

**Storage**: sistema de arquivos do host via bind mount, com um subdiretório por competência:
`<CNPJ_DOWNLOAD_DIR>/<AAAA-MM>/<arquivo>`. Nenhum object storage nesta etapa.

**Testing**: `pytest` + `pytest-asyncio` + `pytest-cov`; servidor HTTP local (`aiohttp.web`)
para downloads; teste de importação da DAG sem rede; integração com a RFB opt-in por
`RUN_NETWORK_TESTS=1`; validação ponta a ponta com `airflow dags test`.

**Target Platform**: Linux, containers do `docker compose` versionado no repositório.

**Project Type**: pipeline de dados em lote (single project) com orquestração Airflow; sem
frontend, API web ou serviço adicional nesta etapa.

**Performance Goals**: download em streaming com blocos de 1 MiB e concorrência 3; memória
proporcional a `concorrência × chunk_size`, nunca ao tamanho do arquivo; registrar bytes e
duração por execução para formar baseline do estágio.

**Constraints**: sem agendamento, Parquet, R2, web ou API (FR-020); token WebDAV nunca em log
nem no resumo; escrita atômica por arquivo; diretório de saída persistente e configurável;
`schedule=None`; `max_active_runs=1`; nenhum trabalho de rede no import da DAG.

**Scale/Scope**: uma DAG, uma task, uma competência por execução; a competência `2026-02` tem
37 arquivos (38 entradas na listagem, incluindo o diretório), podendo chegar a gigabytes. O
limite opcional existe para testes baratos, não para produção.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio / Restrição | Verificação | Resultado |
|---|---|---|
| I. Estágios Desacoplados e Independentes | O estágio de download passa a ser `src.extracao_rfb.download_competencia`, chamável com parâmetros explícitos; competência, limite e diretório vêm da DAG/ambiente, não do código. O import não executa trabalho. `read_files.py` (conversão) permanece fora do escopo desta feature e é débito registrado para a spec de Parquet. | ✅ PASS |
| II. Contrato de Dados Explícito e Schema-First | Esta etapa só transporta arquivos brutos; não interpreta CSV nem cria/duplica schema. `schema.py` não é tocado. | ✅ PASS (N/A ao escopo) |
| III. Streaming e Uso Mensurável de Recursos | `download_files` grava cada chunk direto em arquivo temporário, sem carregar o arquivo em memória; concorrência e `chunk_size` são explícitos. | ✅ PASS |
| IV. Falhas Visíveis e Execução Observável | Remove-se o `except:` genérico; cada arquivo vira `baixado`, `ignorado` ou `falhou` com motivo sanitizado. Falha agrega e levanta `DownloadError`; arquivos concluídos permanecem. Escrita em `.part` + `os.replace`. | ✅ PASS |
| V. Testes Primeiro (NON-NEGOTIABLE) | O plano define a suíte antes da implementação (unitários, integração opt-in e e2e local). A implementação deve começar por testes falhando e ter aprovação do usuário, conforme o princípio. | ✅ PASS (gate de implementação) |
| VI. Simplicidade e Dependências Disciplinadas | Nenhuma dependência nova além de `webdavclient3`, já usada e declarada. Remove-se `pyspark` não usado e a duplicata de `numpy`; `aiohttp` passa a ser declarado. Sem abstração sem segundo uso. | ✅ PASS |
| Segurança e privacidade | O token aparece no path da URL; URLs não são logadas nem persistidas. Erros são sanitizados. A pasta de saída é local e não é servida por web; contém CPF bruto de `socios` e MUST NOT ser publicada. | ✅ PASS |
| Contrato externo da RFB e idempotência | Competência é a chave da execução; listagem ordenada; arquivo existente não vazio é ignorado; reprocessar é idempotente. | ✅ PASS |
| Infraestrutura via `docker compose` | Imagem customizada, código e saída montados no compose; nenhum passo manual fora do compose para executar a DAG. | ✅ PASS |
| Performance / baseline | O estágio registra bytes e duração; não há meta de throughput porque o gargalo é a rede da RFB. O baseline de conversão não é afetado. | ✅ PASS |
| Fluxo Spec Kit | Spec `002`, pesquisa, modelo, contratos e quickstart versionados; `tasks.md` virá de `/speckit.tasks`. | ✅ PASS |

**Resultado**: aprovado, sem violações que exijam `Complexity Tracking`.

**Reavaliação pós-design (2026-10-27)**: os artefatos de Fase 0/1 mantêm o resultado. O
tratamento de `RemoteResourceNotFound` como competência vazia (e não como falha de listagem)
foi incorporado ao cliente, ao modelo e aos contratos; o lock cobre a concorrência; a
sanitização cobre URL/token em log, XCom e exceção. Nenhum gate novo foi violado.

## Project Structure

### Documentation (this feature)

```text
specs/002-cnpj-download-dag/
├── plan.md              # Este arquivo
├── spec.md              # Requisitos
├── research.md          # Fase 0: decisões e evidências
├── data-model.md        # Fase 1: entidades, estados e layout
├── quickstart.md        # Fase 1: roteiro de validação
├── contracts/           # Fase 1: contrato da DAG, API chamável e CLI
│   ├── README.md
│   ├── dag-params.md
│   └── download-api.md
└── checklists/
    └── requirements.md
```

### Source Code (repository root)

```text
src/
├── extracao_rfb/
│   ├── __init__.py
│   ├── competencia.py            # validação AAAA-MM e seleção determinística
│   ├── download_competencia.py   # orquestração, resumo e erros de domínio
│   ├── extract_cnpj_files.py     # CLI fina (argparse), sem trabalho no import
│   └── rfb_client.py             # listagem ordenada, cliente injetável, erros sanitizados
├── download_utils/
│   ├── __init__.py
│   ├── download_files.py         # downloader async, atômico e com resultado estruturado
│   └── redaction.py              # URLs/tokens fora de log e de mensagens de erro
├── webdav/
│   ├── auto_token.py             # timeout e erro explícito
│   └── auto_webdav_client.py     # remove except genérico
└── storage/minio.py              # inalterado (fora do escopo)

airflow/
├── Dockerfile                    # imagem base + alvo de testes
├── requirements.txt              # overlay: somente o que a imagem Airflow não traz
├── requirements-dev.txt          # pytest, pytest-asyncio, pytest-cov
├── docker-compose.yaml           # build, PYTHONPATH, mount do código e da saída, serviço de teste
├── dags/
│   ├── smoke_test.py             # existente
│   └── cnpj_download.py          # nova DAG manual
└── README.md                     # operação da stack e da DAG

tests/
├── conftest.py
├── unit/
│   ├── test_competencia.py
│   ├── test_selection.py
│   ├── test_download_files.py
│   ├── test_download_competencia.py
│   ├── test_redaction.py
│   ├── test_rfb_client.py
│   └── test_dag_import.py
├── integration/
│   └── test_rfb_listing.py       # opt-in RUN_NETWORK_TESTS=1
└── e2e/
    └── test_download_local_server.py

pytest.ini                        # testpaths, asyncio_mode=auto, marker network, sem cache
data/cnpj/                        # saída no host (gitignored), montada em /data/cnpj
requirements.txt                  # lista canônica do pipeline (limpeza: pyspark, duplicata numpy)
```

**Structure Decision**: projeto único de pipeline Python, com o Airflow como orquestrador
externo e uma DAG fina. O código de domínio continua em `src/`, compartilhado entre CLI e DAG;
o container monta a raiz do repositório somente leitura em `/opt/dataquery-cnpj` e usa
`PYTHONPATH=/opt/dataquery-cnpj`. A saída fica fora da árvore de código, em `data/cnpj`, e é
montada em `/data/cnpj`. Não há novo serviço de aplicação: Postgres, Redis e os componentes do
Airflow já existentes bastam.

## Decisões-Chave

| # | Decisão | Efeito |
|---|---|---|
| D1 | Refatorar `extract_cnpj_files.py` em função + CLI; DAG importa a função dentro da task | Cumpre FR-011 e elimina trabalho no import |
| D2 | Layout `data/cnpj/<AAAA-MM>/` com lock e limpeza de `.part` | Isola competências, permite idempotência e evita corrupção |
| D3 | `download_files` retorna `DownloadReport` com resultado por arquivo; falhas agregadas ao final | Cumpre FR-012/FR-013 e Princípio IV |
| D4 | Ordenação lexicográfica estável e `limite` = N primeiros; existentes contam como ignorados e não liberam vaga | Cumpre FR-006 e SC-004 |
| D5 | Competência inválida, limite inválido, listagem vazia (inclusive 404 de competência não publicada) e falha de download falham a task | Cumpre FR-003/FR-007/FR-016 e deixa o erro visível |
| D6 | Sanitização de URLs em toda mensagem de erro/log; `DownloadTarget.__repr__` esconde a URL | Cumpre FR-019 e SC-008 |
| D7 | Imagem customizada só com o que falta (`webdavclient3`), código read-only e saída bind mount | Containers com código, dependências e diretório persistente |
| D8 | Testes unitários com servidor local; integração de rede opt-in; DAG testada por import e `airflow dags test` | Cumpre Princípio V sem tornar a suíte dependente da RFB |

## Complexity Tracking

> Sem violações de constituição. O refactor dos módulos existentes é a correção do contrato
> violado (execução no import e falhas silenciosas), prevista no Princípio I.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| — | — | — |

## Referências

- Spec: [spec.md](./spec.md)
- Pesquisa e evidências: [research.md](./research.md)
- Modelo de dados: [data-model.md](./data-model.md)
- Contratos: [contracts/](./contracts/)
- Validação: [quickstart.md](./quickstart.md)
- Constituição: `.specify/memory/constitution.md` v1.0.0
