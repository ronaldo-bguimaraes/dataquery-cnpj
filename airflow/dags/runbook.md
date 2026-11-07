# Runbook — Ingestão paralela (feature 003)

## 1. Pré-requisitos (uma vez por ambiente)

```shell
# Pool dedicado de 8 slots (isola as tasks do pipeline de outros DAGs)
docker compose exec airflow-scheduler \
  airflow pools set ingest_parallel 8 "Carga CNPJ feature-003"
```

## 2. Disparar a carga

```shell
docker compose exec airflow-scheduler \
  airflow dags test cnpj_ingestion 2026-11-06 \
  -c '{"competencia": "2026-10", "max_workers": 8}'
```

Na UI: **DAGs → cnpj_ingestion → Trigger** com `{"competencia": "2026-10"}`.

## 3. Retomada manual (fora do Airflow)

```shell
python -m src.ingest_parallel.orchestrate --resume 2026-10-<sha8>
```

## 4. Diagnóstico

Estado por partição e histórico de runs (correlação `load_id`):

```sql
SELECT partition_id, table_name, status, retry_count, completed_at
FROM load_partition WHERE load_id = :load_id ORDER BY partition_id;

SELECT run_id, status, error_summary, started_at FROM ingestion_run
WHERE competencia = :competencia ORDER BY started_at DESC;

SELECT * FROM serving_publication WHERE competencia = :competencia;

SELECT reject_type, count(*) FROM ingestion_reject      -- quando existir ledger físico
WHERE load_id = :load_id GROUP BY reject_type;
```

## 5. Rollback

`ServingPublication.previous_active` aponta a versão anterior; o procedimento de
restauração está documentado em `specs/003-reliable-ingestion/quickstart.md` §4.6.
