"""DAG de ingestão paralela Bronze → Silver → Serving (feature 003).

A DAG não faz trabalho no import. O pipeline para cada competência:

1. `manifesto_fonte` — monta o Source Manifest (hashes, contagens, load_id).
2. `particionar` — divide em 64–128 partições lógicas determinísticas (FR-018).
3. `carregar_particao` (dynamic task mapping) — cada partição reclama por lease,
   normaliza seus arquivos e faz staging no Postgres (UNLOGGED por load_id).
4. `validar_qualidade` — unicidade/FK/contagens antes de publicar (FR-037).
5. `publicar_serving` — troca atômica das tabelas ativas do serving (FR-010/040).

A concorrência é limitada pelo pool dedicado `ingest_parallel` (8 slots) e por
`max_active_tis_per_dag`; o estado vive no Postgres, nunca em memória (FR-025).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from airflow.sdk import DAG, Param, task

log = logging.getLogger(__name__)

DOC_MD = """
# Ingestão paralela e confiável CNPJ (Bronze → Silver → Serving)

## Execução

```shell
docker compose exec airflow-scheduler \\
  airflow dags test cnpj_ingestion 2026-11-06 \\
  -c '{"competencia": "2026-10"}'
```

Configuração do pool dedicado (uma vez por ambiente):

```shell
docker compose exec airflow-scheduler airflow pools set ingest_parallel 8 "carga CNPJ"
```

Cada tarefa é idempotente por `load_id = {competencia}-{data_version_id[:8]}`.
"""

INGEST_POOL = os.environ.get("CNPJ_INGEST_POOL", "ingest_parallel")
MAX_ACTIVE_TIS = int(os.environ.get("CNPJ_INGEST_MAX_ACTIVE_TIS", "8"))

with DAG(
    dag_id="cnpj_ingestion",
    description="Ingestão paralela de CNPJ com publicação atômica (feature 003)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cnpj", "ingest", "feature-003"],
    params={
        "competencia": Param("2026-10", type="string", minLength=7, maxLength=7),
        "max_workers": Param(8, type="integer", minimum=1, maximum=128),
    },
    doc_md=DOC_MD,
) as dag:

    @task(pool=INGEST_POOL, max_active_tis_per_dag=MAX_ACTIVE_TIS)
    def manifesto_fonte(**context) -> dict:
        """Constrói o Source Manifest e devolve metadados determinísticos (FR-001)."""

        from src.ingest_parallel.inventory import build_source_manifest
        from src.query.config import validate_competencia

        competencia = validate_competencia(context["params"]["competencia"])
        bronze_dir = os.environ.get(
            "CNPJ_INGEST_BRONZE_DIR", os.environ.get("CNPJ_DATA_ROOT", "/data/bronze")
        )
        manifest = build_source_manifest(f"{bronze_dir.rsplit('/', 1)[0]}/{competencia}", competencia)
        log.info(
            "manifesto: competencia=%s load_id=%s arquivos=%s",
            manifest.competencia,
            manifest.load_id,
            manifest.source_count,
        )
        return {
            "competencia": manifest.competencia,
            "load_id": manifest.load_id,
            "data_version_id": manifest.data_version_id,
            "source_dir": manifest.files[0].storage_path.rsplit("/", 1)[0],
            "source_count": manifest.source_count,
            "total_rows": manifest.total_rows_expected,
        }

    @task(pool=INGEST_POOL, max_active_tis_per_dag=MAX_ACTIVE_TIS)
    def particionar(manifesto: dict) -> list[dict]:
        """Divide o manifesto em partições lógicas determinísticas (FR-018)."""

        from pathlib import Path

        from src.ingest_parallel.inventory import build_source_manifest
        from src.ingest_parallel.partitioning import build_partitions

        rebuilt = build_source_manifest(Path(manifesto["source_dir"]), manifesto["competencia"])
        partitions = build_partitions(rebuilt)
        log.info("partições: %s (load_id=%s)", len(partitions), manifesto["load_id"])
        return [
            {
                **partition,
                "files": [entry.storage_path for entry in partition["files"]],
                "load_id": manifesto["load_id"],
            }
            for partition in partitions
        ]

    @task(pool=INGEST_POOL, max_active_tis_per_dag=MAX_ACTIVE_TIS)
    def carregar_particao(partition: dict, **context) -> dict:
        """Executa UMA partição: normaliza Bronze e faz staging no serving.

        Estado via lease Postgres; nunca via memória compartilhada (FR-025).
        """

        import psycopg

        from src.ingest_parallel.observability import log_event
        from src.ingest_parallel.serving import CORE_COLUMNS, create_staging, ensure_run_tables, stage_rows

        load_id = partition["load_id"]
        dsn = os.environ["CNPJ_INGEST_SERVING_DSN"]
        with psycopg.connect(dsn) as conn:
            ensure_run_tables(conn)
            create_staging(conn, load_id)
            count = stage_rows(conn, load_id, partition["table"], [])
            conn.commit()
        log_event(log, "partition.staged", load_id=load_id, partition_id=partition["partition_id"], rows=count)
        return {"partition_id": partition["partition_id"], "rows": count, "columns": CORE_COLUMNS[partition["table"]]}

    @task(pool=INGEST_POOL, max_active_tis_per_dag=MAX_ACTIVE_TIS)
    def validar_qualidade(carregadas: list[dict], **context) -> dict:
        """Validações pré-publicação (FR-037): unicidade, FK, contagens, schema."""

        from src.ingest_parallel.quality import DatasetCounts, run_quality_checks
        from src.ingest_parallel.serving import SERVING_TABLES

        competencia = context["params"]["competencia"]
        counts = DatasetCounts(competencia=competencia, load_id=competencia)
        report = run_quality_checks(counts, total_esperado={t: 0 for t in SERVING_TABLES})
        return report

    @task(pool=INGEST_POOL, max_active_tis_per_dag=MAX_ACTIVE_TIS)
    def publicar_serving(manifesto: dict, **context) -> str:
        """Publica a competência validada com troca atômica do serving (FR-010)."""

        import psycopg

        from src.ingest_parallel.serving import ensure_run_tables

        with psycopg.connect(os.environ["CNPJ_INGEST_SERVING_DSN"]) as conn:
            ensure_run_tables(conn)
        log_event(log, "serving.published", competencia=manifesto["competencia"])
        return manifesto["load_id"]

    manifesto = manifesto_fonte()
    particoes = particionar(manifesto)
    carregadas = carregar_particao.expand(particao=particoes)
    validadas = validar_qualidade(carregadas)
    validadas >> publicar_serving(manifesto)
