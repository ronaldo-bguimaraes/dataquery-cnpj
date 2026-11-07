# Quickstart — Ingestão Confiável e Escalável

**Feature**: `003-reliable-ingestion` · **Data**: 2026-11-06

---

## 1. Pré-requisitos

| Item | Verificação |
|---|---|
| Docker + Compose v2 | `docker compose version` |
| Python 3.13+ | `python --version` |
| Bronze disponível | `find data/bronze/2026-10 -maxdepth 1 -name '*.parquet' \| wc -l` → **37** |
| Postgres serving disponível | `docker compose --profile query ps query-db` |
| Catálogo Iceberg REST disponível | healthcheck duckdb ou curl |

### Variáveis mínimas

```dotenv
CNPJ_STORAGE_BACKEND=filesystem
CNPJ_DATA_ROOT=/data
CNPJ_QUERY_DATABASE_URL=postgresql://cnpj_query_loader:...@query-db:5432/cnpj_query
CNPJ_QUERY_COMPETENCIA=2026-10
AIRFLOW_UID=1000
```

---

## 2. Executar no Airflow

Após implementação da DAG:

```shell
docker compose --profile query up -d airflow-scheduler
airflow dags trigger cnpj_ingestion
```

Se os containers Airflow não estiverem em uso:

```shell
airflow dags test cnpj_ingestion 2026-11-06 \
  --conf '{"competencia": "2026-10", "workers": 8}'
```

---

## 3. Executar localmente (com Postgres em Docker)

```shell
docker compose --profile query up -d query-db
python -m src.query.ingest.load --competencia 2026-10
```

---

## 4. Validar resultado esperado

### 4.1 Contagem coerente

```sql
SELECT
  total_empresas,
  total_estabelecimentos,
  total_socios,
  fonte,
  carregado_em
FROM query_metadata WHERE ativo;
```

Esperado (base de referência):

- empresas: ~70.5M
- estabelecimentos: ~73.8M
- sócios: ~28.5M
- fonte: `Receita Federal`

### 4.2 Unique keys

```sql
SELECT count(*) FROM (
  SELECT cnpj_basico, count(*)
  FROM empresas
  GROUP BY cnpj_basico
  HAVING count(*) > 1
) AS d;
```

Esperado: 0

### 4.3 CPF sempre mascarado

```sql
SELECT count(*) FROM socios
WHERE documento_socio_mascarado ~ '^[0-9]{11}$';
```

Esperado: 0

### 4.4 FK referencial

```sql
SELECT count(*) FROM estabelecimentos
LEFT JOIN empresas USING (cnpj_basico)
WHERE empresas.cnpj_basico IS NULL;
```

Esperado: 0

### 4.5 Sem registros rejeitados não contabilizados

```sql
SELECT reject_type, count(*) FROM reject_record
WHERE load_id = :load_id
GROUP BY reject_type;
```

Esperado: linhas presentes, `reject_type` classificado e `reason_detail` sanitizado, sem CPF ou PII.

### 4.6 Rollback de teste

```sql
-- Após publicação, o registro de ServingPublication fornece o rollback:
SELECT publication_id, competencia, status, prev;
```

---

## 5. Benchmark

```shell
python -m src.query.benchmark --base-url http://127.0.0.1:8000 --requisicoes 50
```

Esperado: duração total da carga ≤ 2h com 8 workers; listagem p95 ≤ 5s; detail p95 ≤ 3s.

### Resultado medido — competência `2026-10` (2026-11-06)

Carga completa via `src/ingest_parallel.bulk` (8 processos, Postgres dedicado):

| Etapa | Tempo |
|---|---|
| COPY paralelo (223.636.289 linhas; 84 faixas de row-group) | ~48 min |
| Dedupe business keys (415 duplicatas removidas) | ~25 min |
| PKs/índices na staging | ~10 min |
| Listagem derivada (73.809.378 linhas) | ~14 min |
| Swap atômico + derivados + ANALYZE | ~6 min |
| **Total (wall clock)** | **~1h43** (SC-012 ≤ 2h ✅) |

Contagens publicadas: `empresas=70.517.437`, `estabelecimentos=73.809.378`,
`sócios=28.495.547`, `simples=50.806.102`; base 81 GB.

Latências medidas no bulk web (73,8M/200M, cache quente):

| Consulta | Latência |
|---|---|
| listagem sem filtro | 54 ms |
| UF | 15–63 ms |
| UF + situação | 15 ms |
| CNAE | 20 ms (cubo CNAE×UF) |
| CNAE + UF | 67 ms |
| busca por nome comum (`LTDA`) | 311 ms |
| detalhe | 33 ms |

Obs.: caminho de serving em Postgres (bulk), não o writer Iceberg — o catálogo REST
ainda não está provisionado. No bulk web, detail p95 ficou < 15 ms; a listagem com
filtro amplo (UF, ~21M resultados) depende de `VACUUM` + índice para ficar na meta.

---

## 6. Rejeições e modelo idempotência

Para testar idempotência do loader:

```shell
python -m src.query.ingest.load --competencia 2026-11 --dry-run
```

Confirmar que a execução repetida não duplica linhas:
- counts (`SELECT count(*) FROM empresas WHERE competencia = :competencia`) são idem.
- `SELECT count(*) FROM reject_record` não aumenta.
- `SELECT count(*) FROM query_metadata WHERE ativo` = 1.

Se um arquivo for removido ou hash diferente, a carga de carga completa do reprocesso de
bruto valida. `SELECT count(*) FROM query_metadata WHERE ativo` = 1 antes e após.

---

## 7. Referências

- [Contrato da fonte](./contracts/source_manifest.md)
- [Contrato Silver](./contracts/lakehouse_silver.md)
- [Contrato Serving](./contracts/serving_publication.md)
- [Spec](./spec.md)
- [Plano](./plan.md)
