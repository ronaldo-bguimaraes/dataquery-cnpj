# Teste com input comprimido
 - tamanho real do csv: 2047 MB
 - tamanho do output em parquet: 811 MB
 - quantidade de linhas: 26,228,745.00

## Resultados da conversao bruta de csv -> parquet

### Pandas para leitura de csv (read_csv com chunksize) e PyArrow para escrita de parquet
 - iterações: 26
 - tempo: 162.00s

### PyArrow para leitura de csv (open_csv com block_size) e escrita de parquet
 - iterações: 2000
 - tempo: 83.14s
 - 48% mais rápido em relação a leitura com pandas

### Polars para leitura de csv (scan_csv modo lazy) e PyArrow para escrita de parquet
 - Sem resultado, Polars suporta apenas os encodings utf8 e utf8-lossy

# Levanta Infra
## Requisitos
Docker + Docker Compose v2.

## Configuração
```shell
cp .env.example .env
# default local: RustFS (S3-compatível) em http://localhost:9000
# o arquivo é ignorado pelo git; troque as credenciais antes de expor o RustFS
```

## Comando
```shell
docker compose build
docker compose up -d
docker compose ps
```

- Airflow: <http://localhost:8080>
- Console RustFS: <http://localhost:9001> (API S3 em <http://localhost:9000>)
- Raw e Bronze: bucket `cnpj-data` no RustFS (volume Docker `rustfs-data`)

O Compose inclui o **RustFS** como armazenamento do pipeline. O serviço `rustfs-init`
cria o bucket privado `cnpj-data` uma única vez, antes das tarefas do Airflow e do
`query-loader`. Não há mais volume local de dados: as chaves lógicas `raw/` e
`bronze/` vivem no bucket. Para produção, o mesmo código grava em Cloudflare R2 via
API compatível com S3, trocando apenas variáveis de ambiente.

Existe apenas um `.env`, na raiz do repositório; não crie `airflow/.env`.

# Pipeline CNPJ Raw → Bronze

## Dados cobertos e melhorias futuras

O inventário do que os Parquets atuais permitem consultar e das evoluções planejadas
está em [docs/roadmap-dados.md](docs/roadmap-dados.md).

A DAG manual `cnpj_download_manual` processa uma competência (`AAAA-MM`) da Receita
Federal. Para cada ZIP selecionado, ela executa um par independente:

```text
baixar_raw ──▶ raw/<AAAA-MM>/<nome-original>.zip
     │
     └──▶ converter_bronze ──▶ bronze/<AAAA-MM>/<nome-base>.parquet
```

- `baixar_raw` baixa o ZIP ou **reutiliza** um Raw existente e não vazio;
- `converter_bronze` lê o ZIP do Raw, converte o CSV com o schema central em
  PyArrow e publica o Parquet;
- ZIPs Raw são preservados por competência;
- a publicação é atômica: a chave final só aparece após a escrita completa;
- download e conversão falam apenas com a interface `ObjectStore`; filesystem e
  S3/R2 são backends selecionados por configuração;
- não há agendamento automático (`schedule=None`), catchup ou DAG por arquivo.

## Disparo

Pela UI: **DAGs → cnpj_download_manual → Trigger DAG** com:

```json
{"competencia": "2026-02", "limite": 1}
```

Pela CLI:

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_download_manual 2026-10-27 \
  -c '{"competencia": "2026-02", "limite": 1}'
```

`limite` é opcional e seleciona os **N primeiros** nomes em ordem lexicográfica
**antes** de verificar o Raw. Um Raw existente conta no limite, não é baixado de
novo e ainda é convertido. Use `limite=1` para validar o fluxo completo gastando
pouco tempo e espaço.

## Inspeção

Os objetos ficam no bucket `cnpj-data` do RustFS. Use o console em
<http://localhost:9001> ou `mc` num container efêmero:

```shell
docker compose run --rm --entrypoint sh rustfs-init -c \
  'mc alias set rustfs http://rustfs:9000 "$CNPJ_S3_ACCESS_KEY" "$CNPJ_S3_SECRET_KEY" >/dev/null && mc ls -r rustfs/cnpj-data/raw/2026-02 && mc ls -r rustfs/cnpj-data/bronze/2026-02'
```

O Parquet pode ser lido com PyArrow; o ZIP Raw permanece por competência.

Para uma inspeção local (sem o Compose), o backend filesystem continua disponível:
basta definir `CNPJ_STORAGE_BACKEND=filesystem` e `CNPJ_DATA_ROOT`.

## Reexecução e falhas

- Rodar novamente com Raw presente **não** baixa o ZIP de novo, mas reconstrói o
  Bronze a partir do Raw.
- Raw de `0` byte é baixado novamente.
- Falha de download bloqueia somente a conversão daquele arquivo; os demais pares
  continuam.
- Falha de conversão preserva o Raw e o Parquet anterior; nenhum objeto parcial fica
  na chave final.
- Se qualquer arquivo falhar, a task `consolidar_resumo` falha o run ao final, sem
  cancelar os arquivos independentes.
- Para retentar, corrija a causa e faça **Clear** das tarefas falhadas na UI (ou
  dispare a DAG novamente).

O resumo no XCom/log contém apenas nomes, chaves, estados, bytes, linhas e durações.
URL/token da RFB, credenciais, CPF e conteúdo binário nunca entram no XCom.

## Configuração de armazenamento

| Variável | Efeito |
|---|---|
| `CNPJ_STORAGE_BACKEND` | `s3` (default no Compose, gravando no RustFS) ou `filesystem` (testes/CLI offline) |
| `CNPJ_S3_ENDPOINT` | endpoint S3-compatível; default `http://rustfs:9000` |
| `CNPJ_S3_BUCKET` | bucket privado com prefixos `raw/` e `bronze/`; default `cnpj-data` |
| `CNPJ_S3_ACCESS_KEY` | credencial de acesso (default local: `rustfsadmin`) |
| `CNPJ_S3_SECRET_KEY` | segredo de acesso (default local: `rustfsadmin`) |
| `CNPJ_S3_REGION` | região do cliente S3 (default `us-east-1`) |
| `RUSTFS_API_PORT` | porta local da API S3 (default `9000`) |
| `RUSTFS_CONSOLE_PORT` | porta local do console RustFS (default `9001`) |
| `CNPJ_DATA_ROOT` | raiz do backend filesystem (somente fora do Compose) |
| `CNPJ_DATA_HOST_DIR` | diretório do host para o backend filesystem (somente fora do Compose) |

O bucket é criado por `rustfs-init` no `docker compose up`; as credenciais default
são públicas e servem apenas para desenvolvimento local. `.env.example` na raiz
contém apenas placeholders; nunca versione valores reais. Para R2 em produção,
troque `CNPJ_S3_ENDPOINT` e as credenciais — o bucket deve existir previamente.

# CLI local de download (opcional)

O módulo abaixo continua disponível para baixar uma competência para o filesystem
local (`<output-dir>/<AAAA-MM>/`). Ele **não** é usado pela DAG Raw → Bronze.

```shell
python -m src.extracao_rfb.extract_cnpj_files \
  --competencia 2026-02 \
  --output-dir data/cnpj \
  --limite 1
```

| Argumento | Obrigatório | Default | Regra |
|---|---|---|---|
| `--competencia` | sim | — | `AAAA-MM`, mês `01`–`12` |
| `--output-dir` | não | `CNPJ_DOWNLOAD_DIR` | falha se ambos ausentes |
| `--limite` | não | `null` (todos) | inteiro `>= 1`, primeiros nomes em ordem lexicográfica |
| `--concorrencia` | não | `3` | inteiro `>= 1` |

A saída padrão é o resumo JSON (`DownloadSummary.to_dict()`), uma linha. Códigos de
saída: `0` sucesso total, `1` uma ou mais falhas de download (resumo ainda é
impresso) e `2` validação/configuração/diretório/listagem/competência vazia.

Arquivos já existentes e não vazios são ignorados; um arquivo de `0` byte é baixado
de novo.

# Testes

Suíte completa (filesystem/fakes, sem RustFS e sem endpoint S3):

```shell
docker compose --profile test run --rm airflow-tests tests -q
```

Cobertura da camada `src/`:

```shell
docker compose --profile test run --rm airflow-tests \
  tests --cov=src --cov-report=term-missing -q
```

Baseline em 2026-10-27: **228 passed, 3 skipped**, cobertura total **86%**
(anterior: 75%).

Integração S3/R2 é opt-in e só roda com backend explicitamente configurado. Com o
RustFS do Compose já no ar, o endpoint é `http://rustfs:9000`:

```shell
docker compose --profile test run --rm \
  -e CNPJ_STORAGE_BACKEND=s3 \
  -e CNPJ_S3_ENDPOINT=http://rustfs:9000 \
  -e CNPJ_S3_BUCKET=cnpj-data \
  -e CNPJ_S3_ACCESS_KEY=rustfsadmin \
  -e CNPJ_S3_SECRET_KEY=rustfsadmin \
  airflow-tests tests/integration/test_minio_pipeline.py -q
```

Quando o backend não é `s3`/`r2`, ou o endpoint/bucket está indisponível, esses testes
são **skipped** com mensagem explícita. O round-trip local em filesystem roda sempre
em raiz temporária (`tests/integration/test_filesystem_pipeline.py`).

# Segurança

- Download e conversão usam apenas chaves lógicas; paths absolutos, `..` e URIs são
  rejeitados.
- A fábrica de backend é a única que lê a configuração; valor inválido falha antes de
  I/O de dados.
- O bucket é privado e provisionado pelo `rustfs-init`; `ping` não cria bucket.
- URLs/tokens da RFB são removidos de logs, erros e XCom.
- Credenciais nunca aparecem em `repr`, log ou mensagem de exceção.
- Erros de conversão de bibliotecas externas são reduzidos ao tipo da exceção para
  não ecoar valores do CSV (incluindo CPF).
- O XCom carrega somente metadados pequenos, nunca bytes de ZIP/Parquet.
- O volume Docker `rustfs-data` e a cópia local opcional em `data/` podem conter dados
  brutos da RFB, incluindo CPF de sócios. Não publique, versione nem envie esse
  conteúdo para storage público. `data/` é **gitignored** (`/data/*`, exceto
  `.gitkeep`).

# Portal de consulta (explorador visual)

O portal consome os Parquets Bronze da competência `2026-10` — que ficam no RustFS —
e publica um explorador web somente leitura com filtros, contagem, paginação e detalhe
de estabelecimento/empresa.

```shell
docker compose --profile query up -d query-db
docker compose --profile query run --rm query-loader
docker compose --profile query up -d query-web
```

O `query-loader` baixa a competência de `bronze/<AAAA-MM>/` no RustFS para um scratch
temporário (`CNPJ_QUERY_SCRATCH_DIR`, default `/tmp/cnpj-query`) antes de carregar o
PostgreSQL. Reserve espaço local equivalente ao Bronze (~8,3 GB na competência cheia).

- Interface: <http://localhost:8000>
- Operação e variáveis: [query/README.md](query/README.md)
- Arquitetura: PostgreSQL 16 + FastAPI/Jinja2; loader separado; read model
  `estabelecimentos_listagem`
- Segurança: CPF mascarado na carga, papel PostgreSQL somente leitura na web, sem
  exportação/download/autenticação no MVP
- Testes do portal: `docker compose --profile query-test run --rm query-tests pytest tests/query -q -m "not e2e"`

API para terceiros e BI (por exemplo, Superset) são evoluções futuras: podem reutilizar o
PostgreSQL/read model, mas terão contratos, autenticação e operação próprios.
