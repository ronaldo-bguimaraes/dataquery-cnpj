"""T026 — Retomada por checkpoint: concluídas puladas, pendentes/failed retomadas (FR-020/021)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


class TestResumeCheckpoint:
    def test_somente_pendentes_sao_retomadas(self, ingest_test_dsn):
        import psycopg

        from src.ingest_parallel.orchestrate import (
            claim_partition,
            ensure_partition_record,
            ensure_run_tables,
            partitions_status,
            resume_incomplete,
        )

        load_id = "2026-10-retomada"
        with psycopg.connect(ingest_test_dsn) as conn:
            ensure_run_tables(conn)
            conn.execute("DELETE FROM load_partition WHERE load_id = %s", (load_id,))
            conn.commit()

        with psycopg.connect(ingest_test_dsn) as conn:
            for i, table in enumerate(["empresas", "empresas", "socios"]):
                pid = f"{table}-{i}"
                ensure_partition_record(conn, load_id, pid, table)
            mark_succeeded = conn.execute(
                "UPDATE load_partition SET status='succeeded', completed_at=now()"
                " WHERE load_id=%s AND partition_id IN ('empresas-0','socios-2')", (load_id,)
            )
            conn.commit()

        executadas: list[str] = []
        with psycopg.connect(ingest_test_dsn) as conn:
            retomadas = resume_incomplete(
                conn, load_id, task=lambda pid, table: executadas.append(pid)
            )
            conn.commit()

        # apenas a pendente (empresas-1) foi executada; concluídas não reprocessadas
        assert executadas == ["empresas-1"]
        with psycopg.connect(ingest_test_dsn) as conn:
            estado = {p["partition_id"]: p["status"] for p in partitions_status(conn, load_id)}
        assert estado["empresas-0"] == "succeeded"
        assert estado["socios-2"] == "succeeded"
        assert estado["empresas-1"] == "succeeded"

    def test_claim_atamico_sem_duas_escritas_nimo_partition(self, ingest_test_dsn):
        """Duas instâncias reclamam a MESMA partição: só uma ganha (FR-022, US2 aceite 4)."""

        import psycopg

        from src.ingest_parallel.orchestrate import claim_partition, ensure_partition_record, ensure_run_tables

        load_id = "2026-10-corrida"
        with psycopg.connect(ingest_test_dsn) as conn:
            ensure_run_tables(conn)
            conn.execute("DELETE FROM load_partition WHERE load_id = %s", (load_id,))
            ensure_partition_record(conn, load_id, "empresas-0", "empresas")
            conn.commit()

        primeira_ganha = False
        segunda_ganha = False
        with psycopg.connect(ingest_test_dsn) as conn:
            primeira_ganha = claim_partition(conn, load_id, "empresas-0", "worker-a")
        with psycopg.connect(ingest_test_dsn) as conn:
            segunda_ganha = claim_partition(conn, load_id, "empresas-0", "worker-b")
        assert primeira_ganha is True
        assert segunda_ganha is False
