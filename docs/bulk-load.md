# Carga completa (223M) — procedimento operacional

Runner paralelo para carregar uma competência inteira (~223,6M linhas) com staging
`UNLOGGED` + swap atômico, sem bloquear leituras durante a carga.

## Pré-requisitos

- Bronze completo em `data/bronze/<competencia>/` (37 Parquet, ~8,3 GB)
- Postgres dedicado com espaço em disco (≈ 45 GB para dados + índices)
- Python com dependências do projeto (`uv sync`)

Subir um Postgres dedicado (dados em `/home`, ~200 GB livres):

```shell
mkdir -p /home/ronaldo/cnpj-bulk-pg
docker run -d --name cnpj-bulk-db \
  -e POSTGRES_DB=cnpj_bulk \
  -e POSTGRES_USER=cnpj_query_loader \
  -e POSTGRES_PASSWORD=change-me \
  -p 127.0.0.1:55433:5432 \
  -v /home/ronaldo/cnpj-bulk-pg:/var/lib/postgresql/data \
  --shm-size=2g postgres:16 \
  -c shared_buffers=1GB -c work_mem=32MB -c maintenance_work_mem=1GB \
  -c max_wal_size=8GB -c synchronous_commit=off -c autovacuum=off

docker exec -i cnpj-bulk-db psql -U cnpj_query_loader -d cnpj_bulk \
  < src/query/db/schema.sql
```

## Execução

```shell
# carga completa (8 processos; fatiada por row-group)
python -m src.ingest_parallel.bulk \
  --dsn postgresql://cnpj_query_loader:change-me@127.0.0.1:55433/cnpj_bulk \
  --bronze-dir data/bronze/2026-10 --competencia 2026-10 --workers 8
```

Fases: staging → COPY paralelo → dedupe (`keep_last`) → PKs/índices → listagem
derivada → validação de contagens → swap em uma transação → **SET LOGGED** → ANALYZE.

> A staging é `UNLOGGED` por velocidade; após o swap o pipeline converte as tabelas
> publicadas para `LOGGED` (durabilidade/WAL e FKs entre tabelas permanentes).

## Retomada

Se a staging já está completa (falha após o COPY), não recarregue:

```shell
python -m src.ingest_parallel.bulk ... --reuse-staging
```

## Verificação

```sql
SELECT competencia, total_empresas, total_estabelecimentos, total_socios
FROM query_metadata WHERE ativo;

SELECT count(*) FROM estabelecimentos;                -- 73.809.378
SELECT count(*) FROM socios;                           -- 28.495.547 após dedupe
SELECT count(*) FROM (SELECT cnpj_basico FROM empresas
                      GROUP BY 1 HAVING count(*) > 1) d;  -- 0
```

## Web

```shell
docker run -d --name cnpj-bulk-web --network host \
  -e CNPJ_QUERY_DATABASE_URL=postgresql://cnpj_query_loader:change-me@127.0.0.1:55433/cnpj_bulk \
  -e CNPJ_QUERY_COMPETENCIA=2026-10 \
  dataquery-cnpj-query-web:latest \
  uvicorn src.query.web.app:app --host 127.0.0.1 --port 8004
```

## Limpeza

```shell
docker rm -f cnpj-bulk-db cnpj-bulk-web
sudo rm -rf /home/ronaldo/cnpj-bulk-pg
```
