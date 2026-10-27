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
# default local: filesystem em ./data (montado como /data)
# edite .env somente se for usar R2/S3; o arquivo é ignorado pelo git
```

## Comando
```shell
docker compose build
docker compose up -d
docker compose ps
```

- Airflow: <http://localhost:8080>
- Artefatos locais: `data/raw/` e `data/bronze/`

O Compose local inclui apenas o Airflow. **MinIO não é pré-requisito** do caminho
feliz: o backend default é filesystem em `data/`, montado como `/data` nos containers.
Para produção, o mesmo código grava em Cloudflare R2 via API compatível com S3,
trocando apenas variáveis de ambiente.

Existe apenas um `.env`, na raiz do repositório; não crie `airflow/.env`.

# Pipeline CNPJ Raw → Bronze

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

Modo local (default):

```shell
ls data/raw/2026-02/
ls data/bronze/2026-02/
```

O Parquet pode ser lido com PyArrow; o ZIP Raw permanece por competência.

Modo S3/R2: as mesmas chaves lógicas ficam no bucket configurado.

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
| `CNPJ_STORAGE_BACKEND` | `filesystem` (default, aliases `local`/`fs`) ou `s3`/`r2` |
| `CNPJ_DATA_ROOT` | raiz do backend filesystem (containers: `/data`) |
| `CNPJ_DATA_HOST_DIR` | diretório do host montado em `/data` (default `../data`) |
| `CNPJ_S3_ENDPOINT` | endpoint S3/R2; obrigatório só com backend `s3` |
| `CNPJ_S3_BUCKET` | bucket privado com prefixos `raw/` e `bronze/` |
| `CNPJ_S3_ACCESS_KEY` | credencial de acesso |
| `CNPJ_S3_SECRET_KEY` | segredo de acesso |
| `CNPJ_S3_REGION` | região do cliente S3 (default `us-east-1`) |

`.env.example` na raiz contém apenas placeholders; nunca versione valores reais. O
`.env` real fica na raiz e é ignorado pelo git. O bucket R2 é assumido já
provisionado e privado (provisionamento fora do escopo desta feature).

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

Suíte completa (filesystem/fakes, sem MinIO e sem endpoint S3):

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

Integração S3/R2 é opt-in e só roda com backend explicitamente configurado:

```shell
CNPJ_STORAGE_BACKEND=s3 \
CNPJ_S3_ENDPOINT=... CNPJ_S3_BUCKET=... \
CNPJ_S3_ACCESS_KEY=... CNPJ_S3_SECRET_KEY=... \
docker compose --profile test run --rm airflow-tests \
  tests/integration/test_minio_pipeline.py -q
```

Quando o backend não é `s3`/`r2`, ou o endpoint/bucket está indisponível, esses testes
são **skipped** com mensagem explícita. O round-trip local em filesystem roda sempre
em raiz temporária (`tests/integration/test_filesystem_pipeline.py`).

# Segurança

- Download e conversão usam apenas chaves lógicas; paths absolutos, `..` e URIs são
  rejeitados.
- A fábrica de backend é a única que lê a configuração; valor inválido falha antes de
  I/O de dados.
- O bucket R2 é assumido privado e provisionado fora do código; `ping` não cria
  bucket.
- URLs/tokens da RFB são removidos de logs, erros e XCom.
- Credenciais nunca aparecem em `repr`, log ou mensagem de exceção.
- Erros de conversão de bibliotecas externas são reduzidos ao tipo da exceção para
  não ecoar valores do CSV (incluindo CPF).
- O XCom carrega somente metadados pequenos, nunca bytes de ZIP/Parquet.
- `data/` é **gitignored** (`/data/*`, exceto `.gitkeep`) e pode conter dados brutos
  da RFB, incluindo CPF de sócios. Não publique, versione nem envie esse diretório
  para storage público.
