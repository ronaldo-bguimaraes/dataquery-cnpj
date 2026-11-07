"""Registro do estado de lote (T023): succeeded só depois do commit durável."""

from __future__ import annotations

import pytest

from src.ingest_parallel.orchestrate import (
    ensure_partition_record,
    mark_partition_failed,
    mark_partition_succeeded,
    partitions_status,
)

pytestmark = pytest.mark.integration


@pytest.fixture(name="load_state")
def load_state_fixture(ingest_test_dsn):
    import psycopg

    from src.ingest_parallel.serving import ensure_run_tables

    with psycopg.connect(ingest_test_dsn) as conn:
        ensure_run_tables(conn)
        conn.execute("DELETE FROM load_partition WHERE load_id = %s", ("2026-10-lote",))
        conn.commit()
    return ingest_test_dsn


class TestEstadoDeLoteCoordinate:
    def test_lote_nao_sucesso_antes_do_commit(self, load_state):
        """Um lote pending nunca vira succeeded automaticamente (FR-008)."""

        import psycopg

        with psycopg.connect(load_state) as conn:
            ensure_partition_record(conn, "2026-10-lote", "empresas-00", "empresas")
            conn.commit()
            estado = partitions_status(conn, "2026-10-lote")
        assert estado == [{"partition_id": "empresas-00", "table_name": "empresas", "status": "pending"}]

    def test_succeeded_apenas_baixa_explicita(self, load_state):
        import psycopg

        with psycopg.connect(load_state) as conn:
            ensure_partition_record(conn, "2026-10-lote", "empresas-01", "empresas")
            # simula crash: nada de mark_partition_succeeded
            conn.commit()
        with psycopg.connect(load_state) as conn:
            mark_partition_succeeded(conn, "2026-10-lote", "empresas-01")
            conn.commit()
            estado = partitions_status(conn, "2026-10-lote")[0]
        assert estado["status"] == "succeeded"

    def test_falha_incrementa_retry_nao_completa(self, load_state):
        import psycopg

        with psycopg.connect(load_state) as conn:
            ensure_partition_record(conn, "2026-10-lote", "socios-00", "socios")
            conn.commit()
        for _ in range(2):
            with psycopg.connect(load_state) as conn:
                mark_partition_failed(conn, "2026-10-lote", "socios-00")
                conn.commit()
        with psycopg.connect(load_state) as conn:
            row = conn.execute(
                "SELECT status, retry_count, completed_at FROM load_partition"
                " WHERE load_id='2026-10-lote' AND partition_id='socios-00'"
            ).fetchone()
        assert row[0] == "failed"
        assert row[1] == 2
        assert row[2] is None  # nunca marcado como concluído
