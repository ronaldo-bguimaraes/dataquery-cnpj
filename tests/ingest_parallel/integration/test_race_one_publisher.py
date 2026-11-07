"""T033 — Duas execuções da mesma competência: só uma publica (FR-016/017, US3 aceite 4)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


class TestCorrida:
    def test_segunda_exec_rejeitada_com_RUN_ALREADY_ACTIVE(self, ingest_test_dsn):
        import psycopg

        from src.ingest_parallel.orchestrate import ConcurrencyGuardError, acquire_competencia_guard

        with psycopg.connect(ingest_test_dsn) as conn:
            from src.ingest_parallel.serving import ensure_run_tables

            ensure_run_tables(conn)
            conn.execute("DELETE FROM ingestion_run WHERE competencia = '2026-10'")
            conn.execute(
                "UPDATE ingestion_run SET status='aborted' WHERE competencia='2026-10' AND status='running'"
            )
            conn.commit()

        with psycopg.connect(ingest_test_dsn) as conn:
            acquire_competencia_guard(conn, "2026-10-um", "2026-10")
        # outra execução, mesma competência → rejeitada com código explícito
        with psycopg.connect(ingest_test_dsn) as conn:
            with pytest.raises(ConcurrencyGuardError) as exc:
                acquire_competencia_guard(conn, "2026-10-dois", "2026-10")
        assert exc.value.code == "RUN_ALREADY_ACTIVE"

    def test_apos_concluir_outra_pode_entrar(self, ingest_test_dsn):
        import psycopg

        from src.ingest_parallel.orchestrate import (
            acquire_competencia_guard,
            release_competencia_guard,
        )

        # limpa corridas remanescentes de execuções anteriores no banco de teste
        with psycopg.connect(ingest_test_dsn) as conn:
            conn.execute(
                "UPDATE ingestion_run SET status='aborted' WHERE competencia='2026-10' AND status='running'"
            )
            conn.commit()

        with psycopg.connect(ingest_test_dsn) as conn:
            acquire_competencia_guard(conn, "2026-10-a", "2026-10")
            release_competencia_guard(conn, "2026-10-a")
            # agora a próxima entrada é aceita sem corrida
            acquire_competencia_guard(conn, "2026-10-b", "2026-10")
