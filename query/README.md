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
  exportação/download, sem API pública de dados.
- **`query-tests`**: suíte pytest com PostgreSQL de teste.

## Pré-requisitos

- Docker + Docker Compose v2;
- `.env` na raiz, criado a partir de `.env.example`;
- Parquets Bronze em `data/bronze/2026-10/` (37 arquivos).

## Subir e carregar

```shell
docker compose --profile query build query-web query-loader
docker compose --profile query up -d query-db
docker compose --profile query run --rm query-loader
docker compose --profile query up -d query-web
```

Acesse <http://localhost:8000>. O endpoint operacional é `GET /health`.

Se a porta 8000 estiver ocupada, use outra porta:

```shell
CNPJ_QUERY_PORT=8003 docker compose --profile query up -d query-web
```

Para carregar manualmente fora do Compose:

```shell
CNPJ_QUERY_DATABASE_URL=postgresql://cnpj_query_loader:...@127.0.0.1:55432/cnpj_query \
  python -m src.query.ingest.load --source-dir data/bronze/2026-10 --competencia 2026-10
```

O loader é idempotente: reexecutar a mesma competência substitui o retrato de forma atômica.
Falhas fazem rollback e preservam a publicação anterior.

## Variáveis

| Variável | Uso |
|---|---|
| `CNPJ_QUERY_COMPETENCIA` | competência exibida; atual `2026-10` |
| `CNPJ_QUERY_SOURCE` | fonte exibida; default `Receita Federal` |
| `CNPJ_QUERY_PAGE_SIZE` | registros por página; default `25` |
| `CNPJ_QUERY_DATABASE_URL` | DSN do banco; loader usa escrita, web usa role read-only |
| `CNPJ_DATA_ROOT` | raiz do storage filesystem no container; default `/data` |
| `CNPJ_STORAGE_BACKEND` | `filesystem` ou `s3`/`r2` |
| `CNPJ_S3_*` | credenciais do backend S3/R2, usadas somente pelo loader |
| `POSTGRES_QUERY_*` | banco/usuário/senha do PostgreSQL do portal |
| `POSTGRES_QUERY_TEST_DB` | banco isolado usado por `query-tests`; default `cnpj_query_test` |
| `CNPJ_QUERY_APP_PASSWORD` | senha do papel `cnpj_query_app` (somente leitura) |

A web **não** recebe credenciais de storage; somente a URL do banco com o papel
`cnpj_query_app`.

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
- Logs registram rota/status/tempo, nunca valores de filtros ou CPF.
- Não há rotas de exportação, download, escrita ou upload.
- O MVP é local/interno; não exponha publicamente sem proxy/controle de acesso.

## Evolução futura

Uma API para terceiros e dashboards de BI (por exemplo, Apache Superset) podem consumir o
mesmo PostgreSQL e o read model `estabelecimentos_listagem`. Esses consumidores terão
features e contratos próprios; a API exigirá autenticação/rate limiting e o Superset deverá
usar banco de metadados próprio e, em produção, réplica de leitura/base analítica.

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
