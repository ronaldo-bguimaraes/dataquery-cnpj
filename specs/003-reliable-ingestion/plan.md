# Implementation Plan: Ingestão Confiável e Escalável

**Branch**: `003-reliable-ingestion` | **Date**: 2026-11-06 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/003-reliable-ingestion/spec.md`

## Summary

Evoluir a ingestão de CNPJ para uma camada canônica Iceberg com carga em staging Postgres
e swap atômico, usando 8 workers Airflow com 64–128 partições independentes de trabalho.
A fonte pode ser Bronze Parquet (atual) ou Raw, determinada no estágio de varredura do
manifest. Cada partição/batch é idempotente, observável e retentável de forma
isolada. A publicação ocorre apenas depois de todos os validadores de qualidade
(uniqueness, referências, contagens, PII sanitizada) passarem.

## Technical Context

**Language/Version**: Python 3.13+ (Airflow image) · local venv 3.15

**Primary Dependencies**:
- PyIceberg
- REST Catalog (Postgres-backed)
- PostgreSQL 16 serving
- PyArrow
- DuckDB
- psycopg 3 (`psycopg[binary,pool]`)
- Airflow 3.3+

**Storage**:
- Silver: Apache Iceberg com object storage em R2/S3-compatible
- Serving: PostgreSQL 16
- Bronze/raw: filesystem local ou object storage

**Testing**: pytest + unit/integration/contract tests; fixture sinthetic; benchmark e2e

**Target Platform**: Linux container/VM orquestrado por Airflow

**Project Type**: batch ingest pipeline + serving loader

**Performance Goals**:
- competência completa: ~223M registros
- 8 workers em 64–128 partições
- wall clock ≤ 2 horas no ambiente alvo
- sem work re-executado desnecessariamente
- `list` p95 ≤ 5s e `detail` p95 ≤ 3s no serving

**Constraints**:
- ACID: Iceberg transaction + Postgres staging/swap
- Idempotência: reload do mesmo `load_id` é semanticamente idempotente
- Sem carregamento full-in-memory
- CPF mascarado; logs sem PII
- Iceberg Silver particionado por competência
- Sem full rewrite de camadas anteriores (apenas partição competência)
- WAP: branch `load_x` → main sem leitura ambígua

**Scale/Scope**: 223M rows/competência, 37 Bronze files, 64–128 partisiones, 8 workers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Princípio | Garantia no desenho | Resultado |
|---|---|---|
| 1. ACID Transactions | Iceberg atomic commit no catálogo; Postgres em transação com swap | ✅ PASS |
| 2. Idempotency | `load_id` determinístico; staging sobrescrito por partição; deram reload sem duplicar | ✅ PASS |
| 3. Data Deduplication and Integrity | business keys documentadas no Dataset Identity Registry; reject audit e ledger único | ✅ PASS |
| 4. Incremental Processing | Silver partitioned by `competencia`; overwrites apenas partição alvo; comparado snapshot anterior | ✅ PASS |
| 5. Fault Tolerance | checkpoint/lease por partição; retries idempotentes isolados; retomada apenas com hash garantido | ✅ PASS |
| 6. Atomic Batch Processing | batch validado e publicado como unidade atômica (bloco só com sucesso) | ✅ PASS |
| 7. Distributed Execution | 8 mapped tasks no Airflow; pool dedicado; sem memória compartilhada | ✅ PASS |
| 8. Bounded Resource | streaming/chunking; DuckDB/Arrow; target file size 512 MB; no unbounded buffers | ✅ PASS |
| 9. Observability & Auditability | manifests + structured logs; sem CPF/PII; versioned linkage | ✅ PASS |
| 10. Versioning & Reproducibility | Iceberg snapshot id + source hash + code version; raw immutable | ✅ PASS |

**Resultado**: aprovado sem violações que exijam justificativa.

## Project Structure

### Documentation (this feature)

```text
specs/003-reliable-ingestion/
├── plan.md
├── spec.md
├── research.md
├── data-model.md
├── quickstart.md
├── checklists/
│   └── requirements.md
├── contracts/
│   ├── source_manifest.md
│   ├── lakehouse_silver.md
│   └── serving_publication.md
└── tasks.md
```

### Source Code

```text
src/ingest_parallel/
├── __init__.py
├── config.py               # env vars + load_id + concurrency target
├── contracts.py            # data identity rules, business keys, dedup policy
├── inventory.py            # source manifest + hash discovery on Bronze/raw
├── partitioning.py         # 64–128 partitions and AIRflow mapped inputs
├── normalize.py            # Bronze → typed Silver → Arrow batches
├── lakehouse.py            # Iceberg Silver writer + branch/write/commit
├── serving.py              # Postgres staging tables + COPY
├── publish.py              # swap atômico da versão ativa
├── orchestrate.py          # per-partition run / lease / retry
├── quality.py              # validators (unique, FK, count, schema)
├── observability.py        # audit manifest + metrics + sanitized logging
├── reject.py               # reject ledger + PII sanitization
└── benchmark.py            # end-to-end benchmark (p95/load)

airflow/dags/
└── cnpj_ingestion.py       # DAG principal com dynamic task mapping

tests/ingest_parallel/
├── unit/
│   ├── test_config.py
│   ├── test_contracts.py
│   ├── test_inventory.py
│   ├── test_partitioning.py
│   ├── test_normalize.py
│   ├── test_lakehouse_wap.py
│   ├── test_serving_staging.py
│   ├── test_observability.py
│   └── test_quality.py
├── integration/
│   ├── test_happy_path.py
│   ├── test_retry_idempotency.py
│   ├── test_worker_failure.py
│   ├── test_incremental.py
│   └── test_benchmark.py
└── contract/
    ├── test_source_manifest.py
    ├── test_lakehouse_silver.py
    └── test_serving_publication.py
```

### Reuse from existing code

- `schema.py` como fonte de tipos canônicos
- `src/query/domain/masking.py` para PII sanitization
- `src/storage/` para object store abstraction
- `src/query/ingest/` como referência do loader mono-thread que esta feature evolui

**Structure Decision**: `src/ingest_parallel/` é um package separado das camadas
existentes, evitando breaking changes no portal/deploy atuais. A DAG em
`airflow/dags/cnpj_ingestion.py` é nova e não altera a DAG Raw→Bronze da feature 002.

## Complexity Tracking

| Aspecto | Justificativa | Alternativa rejeitada |
|---|---|---|
| REST Catalog para Iceberg | commit atômico + branches em objetos não-atomicos | Delta / DuckDB Iceberg write (limitado) |
| WAP com branch | publicação exclusiva sem leitura mista | `overwrite` na main (sem isolamento) |
| Staging por `load_id` | swaps atômicos + retry sem duplicar | cópia direta na tabela ativa (bloqueia letores) |
| Airflow dynamic task mapping | coordenação sem custom scheduler | custom worker pool (mais código) |

## Next Steps

1. Gerar `tasks.md` com `/speckit.tasks`, começando pelos testes falhando.
2. Implementar módulo `src/ingest_parallel/` e DAG.
3. Validar com sample Bronze/pincial até Silver + serving.
4. Implementar benchmark e validar carga completa.
5. Atualizar roadmap `docs/roadmap-dados.md` com links para o novo pipeline.

## Post-Design Constitution Check

Após o desenho de data model e contracts, os gates continuam atendidos sem violações
a gravar em Complexity Tracking: Iceberg + staging Postgres + parsed Airflow garantem
os princípios ACID, idempotência, deduplicação, incremental, tolerância a falha,
batch atômico, execução distribuída, consumo limitado, observabilidade e versionamento.
