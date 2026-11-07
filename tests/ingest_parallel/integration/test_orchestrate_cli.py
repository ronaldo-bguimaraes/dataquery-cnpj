"""T029/T030/T031 — CLI resume, registro idempotente de partições e run com erro sanitizado."""

from __future__ import annotations

import json

import pytest

from src.ingest_parallel.serving import ensure_run_tables

pytestmark = pytest.mark.integration


class TestResumeCli:
    def test_resumo_json_da_retomada(self, ingest_test_dsn, monkeypatch, capsys):
        import psycopg

        from src.ingest_parallel.orchestrate import ensure_partition_record, main

        monkeypatch.setenv(
            "CNPJ_INGEST_SERVING_DSN",
            ingest_test_dsn,
        )
        load_id = "2026-10-cli"
        with psycopg.connect(ingest_test_dsn) as conn:
            ensure_run_tables(conn)
            conn.execute("DELETE FROM load_partition WHERE load_id = %s", (load_id,))
            ensure_partition_record(conn, load_id, "empresas-0", "empresas")
            conn.execute(
                "UPDATE load_partition SET status='succeeded', completed_at=now()"
                " WHERE load_id=%s AND partition_id='empresas-0'", (load_id,)
            )
            conn.commit()

        code = main(["--resume", load_id])
        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["load_id"] == load_id
        assert payload["retomadas"] == []


class TestRegistroParticao:
    def test_mesmo_hash_e_skip(self, ingest_test_dsn):
        import psycopg

        from src.ingest_parallel.lakehouse_records import register_partition

        load_id = "2026-10-regs"
        with psycopg.connect(ingest_test_dsn) as conn:
            ensure_run_tables(conn)
            conn.commit()
            primeiro = register_partition(conn, load_id, "empresas-0", "hash-ABC")
            conn.commit()
            segundo = register_partition(conn, load_id, "empresas-0", "hash-ABC")
            conn.commit()
            substituicao = register_partition(conn, load_id, "empresas-0", "hash-XYZ")
            conn.commit()
        assert primeiro == "registered"
        assert segundo == "skipped"  # conteúdo igual → não reescreve (FR-032)
        assert substituicao == "replaced"

    def test_particoes_totais_registradas_sem_duplicidade(self, ingest_test_dsn):
        import psycopg

        from src.ingest_parallel.lakehouse_records import register_partition

        load_id = "2026-10-regs2"
        with psycopg.connect(ingest_test_dsn) as conn:
            ensure_run_tables(conn)
            conn.commit()
            for i in range(5):
                register_partition(conn, load_id, f"empresas-{i}", f"hash-{i}")
            conn.commit()
            total = conn.execute(
                "SELECT count(*) FROM silver_partition_registry WHERE load_id = %s",
                (load_id,),
            ).fetchone()[0]
        assert total == 5


class TestRunErrorSanitizado:
    def test_erro_com_cpf_nao_fica_no_historico(self, ingest_test_dsn):
        import psycopg

        from src.ingest_parallel.observability import record_run_error
        from src.ingest_parallel.serving import ensure_run_tables

        with psycopg.connect(ingest_test_dsn) as conn:
            ensure_run_tables(conn)
            conn.commit()
            record_run_error(
                conn,
                run_id="r1",
                load_id="2026-10-err",
                competencia="2026-10",
                status="failed",
                error_summary="falha no CPF 123.456.789-09 registro",
            )
            conn.commit()
            (resumo,) = conn.execute(
                "SELECT error_summary FROM ingestion_run WHERE run_id = 'r1'"
            ).fetchone()
        assert "123.456.789-09" not in resumo
