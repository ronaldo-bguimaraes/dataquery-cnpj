# Portal de consulta — Explorador de Dados CNPJ

Aplicação web somente leitura para explorar os Parquets Bronze da competência `2026-10`.

```text
Parquets Bronze ──query-loader──▶ PostgreSQL ──query-web──▶ navegador
```

- **`query-loader`**: lê as 37 famílias de Parquets, valida o schema, normaliza os campos,
  mascara CPF e publica o modelo relacional de consulta.
- **`query-db`**: PostgreSQL 16 com as tabelas de consulta e o read model
  `estabelecimentos_listagem`.
- **`query-web`**: FastAPI + Jinja2, páginas server-rendered, sem autenticação, sem
  API pública de dados. O explorador é somente leitura e sem download; a página de
  **prospecção B2B** permite gerar um CSV limitado do recorte filtrado.
- **`query-tests`**: suíte pytest com PostgreSQL de teste.

## Pré-requisitos

- Docker + Docker Compose v2;
- `.env` na raiz, criado a partir de `.env.example`;
- Parquets Bronze da competência no bucket `cnpj-data` do RustFS, em
  `bronze/<AAAA-MM>/` (37 arquivos), produzidos pela DAG `cnpj_download_manual`.

## Subir e carregar

```shell
docker compose --profile query build query-web query-loader
docker compose --profile query up -d query-db
docker compose --profile query run --rm query-loader
docker compose --profile query up -d query-web
```

Acesse <http://localhost:8000>. O endpoint operacional é `GET /health`.

Páginas:

| Rota | Uso |
|---|---|
| `/estabelecimentos` | explorador cadastral somente leitura |
| `/prospeccao` | recorte comercial com segmento, idade, filiais e contato |
| `/prospeccao.csv` | lista CSV do recorte (até `CNPJ_QUERY_EXPORT_LIMIT` linhas) |

Se a porta 8000 estiver ocupada, use outra porta:

```shell
CNPJ_QUERY_PORT=8003 docker compose --profile query up -d query-web
```

Para carregar manualmente fora do Compose, baixe a competência do RustFS e use o
backend filesystem:

```shell
CNPJ_S3_ENDPOINT=http://127.0.0.1:9000 CNPJ_S3_BUCKET=cnpj-data \
CNPJ_S3_ACCESS_KEY=rustfsadmin CNPJ_S3_SECRET_KEY=rustfsadmin \
  python -m src.query.ingest.source --competencia 2026-10 --dest data/bronze/2026-10

CNPJ_QUERY_DATABASE_URL=postgresql://cnpj_query_loader:...@127.0.0.1:55432/cnpj_query \
CNPJ_STORAGE_BACKEND=filesystem CNPJ_DATA_ROOT="$PWD/data" \
  python -m src.query.ingest.load --competencia 2026-10
```

O loader é idempotente: reexecutar a mesma competência substitui o retrato de forma atômica.
Falhas fazem rollback e preservam a publicação anterior.

## Variáveis

| Variável | Uso |
|---|---|
| `CNPJ_QUERY_COMPETENCIA` | competência exibida; atual `2026-10` |
| `CNPJ_QUERY_SOURCE` | fonte exibida; default `Receita Federal` |
| `CNPJ_QUERY_PAGE_SIZE` | registros por página; default `25` |
| `CNPJ_QUERY_EXPORT_LIMIT` | teto de linhas do CSV de prospecção; default `50000`, faixa 1–100000 |
| `CNPJ_QUERY_DATABASE_URL` | DSN do banco; loader usa escrita, web usa role read-only |
| `CNPJ_QUERY_SCRATCH_DIR` | diretório temporário para o download do Bronze; default `/tmp/cnpj-query` |
| `CNPJ_DATA_ROOT` | raiz do storage filesystem (somente backend `filesystem`); default `/data` |
| `CNPJ_STORAGE_BACKEND` | `s3` (default no Compose, RustFS/R2) ou `filesystem` |
| `CNPJ_S3_*` | endpoint, bucket e credenciais do backend S3/R2, usados somente pelo loader |
| `POSTGRES_QUERY_*` | banco/usuário/senha do PostgreSQL do portal |
| `POSTGRES_QUERY_TEST_DB` | banco isolado usado por `query-tests`; default `cnpj_query_test` |
| `CNPJ_QUERY_APP_PASSWORD` | senha do papel `cnpj_query_app` (somente leitura) |

A web **não** recebe credenciais de storage; somente a URL do banco com o papel
`cnpj_query_app`. O loader baixa os 37 Parquets do bucket para o scratch antes de
validar o inventário; reserve espaço local equivalente ao Bronze (~8,3 GB).

## Testes

```shell
docker compose --profile query-test run --rm query-tests pytest tests/query -q -m "not e2e"
```

E2E opcional (Playwright + aplicação em execução):

```shell
CNPJ_QUERY_E2E_URL=http://127.0.0.1:8000 \
  docker compose --profile query-test run --rm query-tests pytest tests/query -q -m e2e
```

Benchmark:

```shell
docker compose --profile query run --rm query-web \
  python -m src.query.benchmark --requisicoes 50
```

## Segurança

- CPF de sócio é mascarado no loader; o banco de consulta não guarda CPF em claro.
- A web usa um papel PostgreSQL somente leitura.
- Logs registram rota/status/tempo, nunca valores de filtros, contatos ou CPF.
- Não há rotas de escrita ou upload. A única exportação é o CSV de prospecção,
  limitado e sem CPF/sócios, com neutralização de fórmulas de planilha.
- O MVP é local/interno; não exponha publicamente sem proxy/controle de acesso.

## Prospecção comercial (feature 004)

A página `/prospeccao` usa o mesmo read model e adiciona:

- **segmento comercial** derivado da divisão CNAE (catálogo em
  `src/query/domain/segmentos.py`, publicado em `segmentos`/`segmento_divisoes`);
- **indicadores**: idade até o fim da competência, quantidade de CNAEs, de
  estabelecimentos e de filiais do CNPJ raiz;
- **contatos cadastrais** (telefone/e-mail da Receita Federal, sem enriquecimento);
- **filtros** por segmento, idade, filiais, presença de contato, além dos filtros
  cadastrais existentes; a situação cadastral começa em **Ativa**;
- **agregações** por segmento e por UF no mesmo recorte;
- **CSV** em streaming com CNPJ raiz, uma linha por estabelecimento.

Alterar o catálogo de segmentos exige recarregar a competência (`query-loader`) para
republicar o read model.

## Evolução futura

Uma API para terceiros e dashboards de BI (por exemplo, Apache Superset) podem consumir o
mesmo PostgreSQL e o read model `estabelecimentos_listagem`. Esses consumidores terão
features e contratos próprios; a API exigirá autenticação/rate limiting e o Superset deverá
usar banco de metadados próprio e, em produção, réplica de leitura/base analítica.

O catálogo de segmentos é uma decisão de produto: novos segmentos ou realocações de CNAE
são mudanças de código, cobertas por teste e publicadas na próxima carga.

## Benchmark inicial

Com a base sintética de fixtures (10 requisições, ambiente local):

| Operação | p95 | Mediana |
|---|---:|---:|
| Listagem filtrada | 4,54 ms | 3,42 ms |
| Detalhe | 7,45 ms | 6,28 ms |

A competência completa contém 70.517.438 empresas, 73.809.379 estabelecimentos,
28.495.961 sócios e 50.806.102 registros de Simples. O baseline completo deve ser
registrado após a carga integral no PostgreSQL:

```shell
python -m src.query.benchmark --base-url http://127.0.0.1:8000 --requisicoes 50
```

### Prospecção (feature 004)

Medição local com **200.000 estabelecimentos sintéticos** no PostgreSQL 16 (uma
execução de cada operação, incluindo serialização; não é p95):

| Recorte | Listagem | Agregações | Contagem | CSV (5.000 linhas) |
|---|---:|---:|---:|---:|
| Situação ativa | 2,6 ms | 2,1 ms | 0,4 ms | 36,7 ms |
| Segmento + UF | 5,7 ms | 0,8 ms | 0,5 ms | — |
| Idade + filiais + contato | 33,0 ms | 74,1 ms | 26,4 ms | — |

Os recortes cobertos pelo cubo `estabelecimentos_filtros_stats` (segmento, UF, porte,
situação, matriz/filial, Simples, MEI) usam contagem e agregação pré-computadas. Filtros
de idade, filiais, contato, município, CNAE ou texto caem no caminho de varredura
indexada e são mais custosos; por isso a contagem é limitada (`COUNT_CAP`) e o CSV tem
teto. Em 73,8 M de linhas, meça novamente após a carga integral.
