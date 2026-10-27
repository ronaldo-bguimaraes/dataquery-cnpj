# Airflow — Raw → Bronze de dados CNPJ

Stack local do Airflow 3.3.2 (`CeleryExecutor` + Postgres + Redis) que executa a DAG
manual de download e conversão dos arquivos CNPJ publicados pela Receita Federal.

A DAG **não agenda** execuções (`schedule=None`) e trabalha em duas camadas sobre uma
interface genérica de armazenamento:

```text
RFB ──baixar_raw──▶ raw/<AAAA-MM>/<nome-original>.zip
                         │
                         └──converter_bronze──▶ bronze/<AAAA-MM>/<nome-base>.parquet
```

O backend é escolhido só por configuração:

- **local (default)**: filesystem em `data/` na raiz do repositório, montado como
  `/data` nos containers;
- **produção**: Cloudflare R2 via API compatível com S3.

Download e conversão não conhecem filesystem, bucket nem provedor. Cada ZIP
selecionado ganha um par de tarefas independente
`pipeline_arquivo.baixar_raw[n] → pipeline_arquivo.converter_bronze[n]`. Um ZIP Raw
existente e não vazio é reutilizado sem nova transferência; a conversão continua
sendo executada e substitui o Parquet anterior somente depois da publicação atômica.

## Pré-requisitos

- Docker + Docker Compose v2;
- `.env` na raiz (não versionado), criado a partir de `.env.example`;
- acesso de rede à RFB para execuções reais.

Execute os comandos a partir da raiz do repositório. O `docker-compose.yaml` raiz
inclui apenas `airflow/docker-compose.yaml`; **MinIO não é pré-requisito** do caminho
local.

```shell
cp .env.example .env
# default local:
# CNPJ_STORAGE_BACKEND=filesystem
# CNPJ_DATA_ROOT=/data
docker compose build
docker compose up -d
docker compose ps
```

Os serviços Airflow montam `${CNPJ_DATA_HOST_DIR:-../data}` em `/data`. No host, Raw e
Bronze aparecem em `data/raw/` e `data/bronze/`. A UI fica em
<http://localhost:8080> (login local padrão `airflow` / `airflow`).

> Um endpoint S3-compatível local (por exemplo MinIO) pode ser iniciado à parte, mas
> não faz parte do caminho feliz nem dos pré-requisitos da suíte.

## DAG `cnpj_download_manual`

| Item | Valor |
|---|---|
| `listar_arquivos` | lista, ordena e aplica `limite` em runtime |
| `preparar_arquivos` | expande um item pequeno por ZIP selecionado |
| `pipeline_arquivo.baixar_raw` | baixa ou reutiliza o ZIP em `raw/` |
| `pipeline_arquivo.converter_bronze` | converte o mesmo ZIP para `bronze/` |
| `consolidar_resumo` | `trigger_rule=ALL_DONE`; falha o run se algum arquivo falhou |
| Agendamento | `None` (somente disparo manual) |
| `max_active_runs` | `1` |
| `retries` | `0` |

A DAG não faz I/O no import: o domínio e o backend são resolvidos apenas dentro das
callables via `build_store_from_env`.

### Parâmetros

| Nome | Tipo | Obrigatório | Default | Regra |
|---|---|---|---|---|
| `competencia` | string | sim, na prática | `null` | `AAAA-MM`, mês `01`–`12` |
| `limite` | integer | não | `null` | `>= 1`; `null` = todos |

O limite é aplicado **antes** de consultar o Raw: seleciona os **N primeiros** nomes
em ordem lexicográfica. Um Raw já existente entre os N **conta** no limite, não é
baixado de novo e ainda é convertido. `limite=1` é o caminho mais barato para
validar o fluxo.

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

Modo local (`CNPJ_STORAGE_BACKEND=filesystem`):

```text
data/
├── raw/2026-02/
│   └── Cnaes.zip                       # ZIP original preservado
└── bronze/2026-02/
    └── Cnaes.parquet                   # Parquet com o schema central
```

No modo S3/R2, as mesmas chaves lógicas ficam no bucket configurado:

```text
s3://<CNPJ_S3_BUCKET>/
├── raw/<AAAA-MM>/<nome-original>
└── bronze/<AAAA-MM>/<nome-base>.parquet
```

Inspeção local no host:

```shell
ls data/raw/2026-02/
ls data/bronze/2026-02/
```

### Resumo

O resumo JSON fica no XCom `return_value` de `consolidar_resumo` e no log da task:

```json
{
  "competencia": "2026-02",
  "encontrados": 37,
  "selecionados": 1,
  "limite": 1,
  "parcial": true,
  "arquivos": [
    {
      "nome": "Cnaes.zip",
      "status_download": "baixado",
      "status_conversao": "publicado",
      "bytes": 123456,
      "bytes_parquet": 45678,
      "linhas": 1300,
      "raw_key": "raw/2026-02/Cnaes.zip",
      "bronze_key": "bronze/2026-02/Cnaes.parquet",
      "duracao_segundos": 2.41,
      "erro": null
    }
  ],
  "totais": {
    "baixados": 1,
    "reutilizados": 0,
    "falhas_download": 0,
    "publicados": 1,
    "falhas_conversao": 0,
    "nao_executados": 0,
    "bytes": 123456,
    "bytes_parquet": 45678,
    "linhas": 1300
  },
  "falhas": 0,
  "duracao_segundos": 2.41
}
```

Estados possíveis: download `baixado`, `reutilizado` ou `falhou`; conversão
`publicado`, `falhou` ou `não executado`.

## Reexecução e concorrência

- A DAG não agenda execuções, não faz catchup e permite no máximo um run ativo.
- Raw existente e não vazio ⇒ `reutilizado`, sem nova requisição de conteúdo à RFB.
- Raw de `0` byte é baixado novamente.
- Bronze é sempre reconstruído a partir do Raw selecionado; a versão anterior é
  substituída só depois de uma publicação completa.
- `.part` locais (filesystem) e uploads multipart incompletos (S3) são
  removidos/abortados.
- Para retentar, corrija a causa (rede, credencial, raiz/bucket) e faça **Clear** das
  tarefas falhadas na UI ou dispare a DAG novamente. O Raw preservado evita novo
  download dos arquivos já concluídos.

## Falhas

- Falha de download ⇒ apenas a conversão daquele ZIP fica `não executado`. Os
  outros pares continuam.
- Falha de conversão ⇒ o Raw permanece, o Bronze anterior (se existir) fica
  íntegro e nenhum objeto parcial aparece na chave final.
- Qualquer falha por arquivo ⇒ `consolidar_resumo` levanta `PipelineFailedError` no
  fim e o run termina `failed`, sem interromper os arquivos independentes.
- Competência sem arquivos ⇒ `EmptyCompetenciaError`.
- Competência, `limite` ou nome inválidos ⇒ falha antes de qualquer acesso à RFB ou
  ao armazenamento.
- Raiz ausente/sem escrita ou bucket ausente ⇒ `StorageConfigError` ou
  `StorageOperationError` sanitizado.

## Variáveis de ambiente

| Variável | Onde | Default | Efeito |
|---|---|---|---|
| `CNPJ_STORAGE_BACKEND` | `.env` | `filesystem` | `filesystem` (aliases `local`/`fs`) ou `s3`/`r2` |
| `CNPJ_DATA_ROOT` | containers | `/data` | raiz do backend filesystem |
| `CNPJ_DATA_HOST_DIR` | host/compose | `../data` | diretório do host montado em `/data` |
| `CNPJ_S3_ENDPOINT` | `.env` | — | endpoint S3/R2; só com backend `s3` |
| `CNPJ_S3_BUCKET` | `.env` | — | bucket privado das camadas Raw/Bronze; só com `s3` |
| `CNPJ_S3_ACCESS_KEY` | `.env` | — | credencial de acesso; só com `s3` |
| `CNPJ_S3_SECRET_KEY` | `.env` | — | segredo de acesso; só com `s3` |
| `CNPJ_S3_REGION` | `.env` | `us-east-1` | região do cliente S3 |
| `CNPJ_CODE_HOST_DIR` | host/compose | `..` | código montado em `/opt/dataquery-cnpj` |
| `AIRFLOW_UID` | `.env` | `50000` | dono dos arquivos montados |
| `RUN_NETWORK_TESTS` | testes | ausente | habilita o teste real da RFB |

`.env` na raiz é a fonte única de configuração; é ignorado pelo git e não deve conter
valores reais em exemplos versionados. Não crie `airflow/.env`.

## Testes

A imagem de teste é o alvo `test` do `airflow/Dockerfile` e roda com o código montado
em `/opt/dataquery-cnpj`:

```shell
docker compose --profile test build
docker compose --profile test run --rm airflow-tests tests -q
```

Cobertura (baseline registrado em 2026-10-27, backend filesystem):

```shell
docker compose --profile test run --rm airflow-tests \
  tests --cov=src --cov-report=term-missing -q
```

| Módulo | Cobertura |
|---|---|
| `src/conversao/bronze.py` | 90% |
| `src/download_utils/download_files.py` | 94% |
| `src/download_utils/redaction.py` | 94% |
| `src/extracao_rfb/competencia.py` | 100% |
| `src/extracao_rfb/download_competencia.py` | 93% |
| `src/extracao_rfb/download_raw.py` | 99% |
| `src/extracao_rfb/extract_cnpj_files.py` | 0% (CLI local legado, sem teste próprio) |
| `src/extracao_rfb/rfb_client.py` | 77% |
| `src/pipeline/raw_bronze.py` | 95% |
| `src/storage/factory.py` | 100% |
| `src/storage/filesystem.py` | 92% |
| `src/storage/minio.py` | 89% |
| `src/storage/protocol.py` | 94% |
| `src/webdav/auto_token.py` | 35% |
| `src/webdav/auto_webdav_client.py` | 47% |
| **Total** | **86%** (228 passed, 3 skipped) |

O baseline anterior era **75%**; a cobertura total não regrediu.

### Integração filesystem

`tests/integration/test_filesystem_pipeline.py` faz o round-trip Raw → Bronze em uma
raiz temporária, sem RFB, sem boto3 e sem endpoint externo. É executado pela suíte
padrão.

### Integração S3/R2 (opt-in)

O round-trip S3 usa o mesmo backend de produção e o bucket reais, sem chamar a RFB.
Ele só roda quando o backend S3 está explicitamente selecionado e o bucket responde:

```shell
CNPJ_STORAGE_BACKEND=s3 \
CNPJ_S3_ENDPOINT=... CNPJ_S3_BUCKET=... \
CNPJ_S3_ACCESS_KEY=... CNPJ_S3_SECRET_KEY=... \
docker compose --profile test run --rm airflow-tests \
  tests/integration/test_minio_pipeline.py -q
```

Quando `CNPJ_STORAGE_BACKEND` não é `s3`/`r2`, ou o endpoint/bucket não responde, os
testes são **skipped** com mensagem explícita. Eles usam a competência sentinela
`2099-12` e removem `raw/2099-12/Cnaes.zip` e `bronze/2099-12/Cnaes.parquet` antes e
depois de cada caso.

O teste real da RFB continua opt-in:

```shell
docker compose --profile test run --rm -e RUN_NETWORK_TESTS=1 \
  airflow-tests tests/integration -q
```

## Segurança e observabilidade

Evidências verificadas por testes automatizados (`tests/unit/test_filesystem_store.py`,
`tests/unit/test_store_factory.py`, `tests/unit/test_minio_store.py`,
`tests/unit/test_convert_raw.py`, `tests/unit/test_dag_import.py`,
`tests/unit/test_pipeline_raw_bronze.py` e `tests/e2e/test_raw_bronze_local.py`):

- **Backend por configuração**: `build_store_from_env` é a única fábrica; a DAG não
  importa `S3ObjectStore` nem filesystem no parse. Valor inválido falha antes de
  qualquer I/O de dados.
- **Sem credenciais em `repr`, log ou exceção**: `S3ObjectStore.__repr__` expõe apenas
  endpoint/bucket; erros S3 são sanitizados inclusive para `BotoCoreError`. Há
  asserções explícitas para access key e secret key.
- **Chaves lógicas apenas**: `validate_key` rejeita path absoluto, `..`, `\` e URI
  (`s3://`, `file://`) nos dois backends.
- **Sem criação implícita de bucket**: `ping` apenas verifica o bucket; o R2 é
  assumido provisionado e privado. O código não torna bucket público.
- **Sem URL/token da RFB**: `describe_exception` substitui URLs por `<url>`. Logs das
  tasks registram apenas nome, status, bytes e linhas; resumo e XCom passam por
  asserções que rejeitam `http://` e `https://`.
- **Sem conteúdo de arquivo ou CPF**: resultados de conversão não incluem o CSV.
  Exceções do PyArrow (que podem citar o valor inválido) são reduzidas a
  `falha na conversão CSV para Parquet (<Tipo>)`. O teste E2E injeta
  `123.456.789-00` numa mensagem do PyArrow e confirma que o CPF não aparece no
  resumo, no XCom, na exceção consolidada ou no log.
- **XCom pequeno**: somente `RawListing.to_dict()`, `RawFileResult.to_dict()`,
  `BronzeFileResult.to_dict()` e o resumo consolidado trafegam; nenhum byte de ZIP
  ou Parquet. O E2E limita o resumo serializado a menos de 4 KB e confirma que o
  payload binário não aparece.
- **Atomicidade e temporários**: filesystem usa `.part` sibling + `os.replace`; S3 usa
  PUT completo ou multipart com `complete`. Testes confirmam que a chave final não
  recebe objeto parcial e que o scratch da task é removido em sucesso e falha.

`data/` contém dados brutos da RFB quando usado localmente, incluindo CPF de sócios, e
é **gitignored** (`/data/*`, exceto `.gitkeep`). Não publique nem versione esse
diretório.

## Estrutura

```text
.env.example                # placeholders, sem segredos reais (raiz)

airflow/
├── Dockerfile              # estágios base (DAGs) e test (pytest)
├── docker-compose.yaml     # stack Airflow, mounts e airflow-tests
├── dags/cnpj_download.py   # DAG manual Raw → Bronze
├── requirements.txt        # boto3 + pyarrow + webdavclient3
└── requirements-dev.txt    # pytest, pytest-asyncio, pytest-cov

data/                       # backend local (gitignored, mantém .gitkeep)
├── raw/<AAAA-MM>/
└── bronze/<AAAA-MM>/

src/storage/
├── protocol.py             # ObjectStore/ObjectMetadata
├── filesystem.py           # backend local
├── minio.py                # backend S3-compatible (R2/MinIO)
└── factory.py              # seleção por CNPJ_STORAGE_BACKEND
```
