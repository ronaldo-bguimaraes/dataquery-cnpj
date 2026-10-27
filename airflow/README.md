# Airflow — Download de Dados CNPJ

Stack local do Airflow 3.3.2 (`CeleryExecutor` + Postgres + Redis) que executa a DAG
manual de download dos arquivos CNPJ publicados pela Receita Federal.

A DAG **não agenda** execuções (`schedule=None`) e não transforma os arquivos nesta
etapa: ela apenas baixa a competência escolhida para um diretório persistente e
publica um resumo observável.

## Pré-requisitos

- Docker + Docker Compose v2;
- `airflow/.env` (não versionado) com `AIRFLOW_UID`; `FERNET_KEY` é opcional e, se
  ausente, o Airflow sobe sem criptografia de conexões/variáveis (esta feature não usa
  nenhuma);
- acesso de rede à RFB para execuções reais.

```shell
mkdir -p data/cnpj
docker compose build
docker compose up -d
docker compose ps
```

A UI fica em <http://localhost:8080> (login local padrão `airflow` / `airflow`).
O `airflow-init` cria `/data/cnpj` e ajusta o dono para `AIRFLOW_UID`.

## DAG `cnpj_download_manual`

| Item | Valor |
|---|---|
| Task | `baixar_arquivos` |
| Agendamento | `None` (somente disparo manual) |
| `max_active_runs` | `1` |
| `retries` | `0` |

### Parâmetros

| Nome | Tipo | Obrigatório | Default | Regra |
|---|---|---|---|---|
| `competencia` | string | sim, na prática | `null` | `AAAA-MM`, mês `01`–`12` |
| `limite` | integer | não | `null` | `>= 1`; `null` = todos |

O limite seleciona os **N primeiros** nomes em ordem lexicográfica. Arquivos já
presentes entre os selecionados contam como ignorados e **não** liberam vaga para os
seguintes.

### Disparo pela UI

`DAGs → cnpj_download_manual → Trigger DAG` com:

```json
{"competencia": "2026-02", "limite": 1}
```

### Disparo pela CLI

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-02", "limite": 1}'
```

### Saída

```text
data/cnpj/
└── 2026-02/
    ├── .download.lock
    ├── Cnaes.zip
    ├── Empresas0.zip
    └── ...
```

No container o caminho é `/data/cnpj/<AAAA-MM>/`, configurado por
`CNPJ_DOWNLOAD_DIR`. O diretório é um bind mount do host
(`${CNPJ_DATA_HOST_DIR:-../data/cnpj}`); ele sobrevive a `docker compose down` e
**não** é apagado por `down -v`.

### Resumo

O resumo JSON é gravado no XCom `return_value` e no log da task:

```json
{
  "competencia": "2026-02",
  "output_dir": "/data/cnpj/2026-02",
  "encontrados": 37,
  "selecionados": 1,
  "baixados": 1,
  "ignorados": 0,
  "falhas": 0,
  "bytes_baixados": 123456,
  "duracao_segundos": 2.41,
  "parcial": true,
  "arquivos": [
    {"nome": "Cnaes.zip", "status": "baixado", "bytes": 123456, "erro": null}
  ]
}
```

### Reexecução e concorrência

- Arquivos finais não vazios são ignorados sem requisição HTTP; arquivos de 0 byte
  são baixados de novo.
- Um lock exclusivo (`.download.lock`) por competência cobre chamadas concorrentes
  feitas pela CLI ou por `airflow dags test` fora do scheduler. A segunda execução
  falha com `ExecutionInProgressError` após 5 s, sem tocar nos arquivos.
- Arquivos `.part` remanescentes de uma interrupção são removidos sob o lock antes
  da listagem.

### Falhas

- Um arquivo com erro HTTP/tamanho ⇒ `DownloadError`; a task falha, mas os arquivos
  concluídos permanecem.
- Competência sem arquivos (inclusive 404 de competência não publicada) ⇒
  `EmptyCompetenciaError`.
- Competência ou limite inválido ⇒ falha antes de qualquer acesso à fonte.
- Diretório não gravável ⇒ `OutputDirectoryError` antes de listar/baixar.

## Variáveis de ambiente

| Variável | Onde | Default | Efeito |
|---|---|---|---|
| `CNPJ_DOWNLOAD_DIR` | containers | `/data/cnpj` (compose) | diretório base persistente |
| `CNPJ_DATA_HOST_DIR` | host/compose | `../data/cnpj` | origem do bind mount |
| `CNPJ_CODE_HOST_DIR` | host/compose | `..` | código montado em `/opt/dataquery-cnpj` |
| `AIRFLOW_UID` | `airflow/.env` | `50000` | dono dos arquivos no bind mount |
| `RUN_NETWORK_TESTS` | testes | ausente | habilita o teste real da RFB |

## Testes

A imagem de teste é o alvo `test` do `airflow/Dockerfile` e roda com o código montado
em `/opt/dataquery-cnpj`:

```shell
docker compose --profile test build
docker compose --profile test run --rm airflow-tests tests -q
```

Cobertura (baseline registrado em 2026-10-27):

```shell
docker compose --profile test run --rm airflow-tests \
  tests --cov=src --cov-report=term-missing -q
```

| Módulo | Cobertura |
|---|---|
| `src/extracao_rfb/competencia.py` | 100% |
| `src/extracao_rfb/download_competencia.py` | 93% |
| `src/download_utils/download_files.py` | 94% |
| `src/download_utils/redaction.py` | 94% |
| `src/extracao_rfb/rfb_client.py` | 77% |
| **Total** | **75%** (78 passed, 1 skipped) |

O teste de integração real com a RFB é opt-in:

```shell
docker compose --profile test run --rm -e RUN_NETWORK_TESTS=1 \
  airflow-tests tests/integration -q
```

## Segurança

- URLs e tokens da RFB **nunca** vão para log, XCom, `repr` ou mensagem de erro.
- `data/cnpj/` contém dados brutos da RFB (incluindo CPF de sócios) e é
  **gitignored**. Não publique nem versione esse diretório.

## Estrutura

```text
airflow/
├── Dockerfile              # estágios base (DAGs) e test (pytest)
├── docker-compose.yaml     # stack, mounts, env e serviço airflow-tests
├── dags/cnpj_download.py   # DAG manual
├── requirements.txt        # webdavclient3
└── requirements-dev.txt    # pytest, pytest-asyncio, pytest-cov
```
