"""T025 — Retry idempotente: mesma carga 2× → contagens iguais, 0 duplicatas (FR-013/015)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.quality import DatasetCounts

pytestmark = pytest.mark.integration


def _linhas(load_id: str = "2026-10-retry") -> DatasetCounts:
    from tests.ingest_parallel.integration.test_happy_path import _linhas_exemplo

    return _linhas_exemplo(load_id)


class TestRetryIdempotente:
    def test_publicacao_duas_vezes_mesmo_resultado(self, clean_test_schema):
        import psycopg

        from src.ingest_parallel.serving import publish_serving

        primeira = publish_serving(clean_test_schema, _linhas(), competencia="2026-10")
        segunda = publish_serving(clean_test_schema, _linhas(), competencia="2026-10")

        assert primeira["counts"] == segunda["counts"]
        with psycopg.connect(clean_test_schema) as conn:
            empresas = conn.execute("SELECT count(*) FROM empresas").fetchone()[0]
            duplicatas = conn.execute(
                "SELECT count(*) FROM (SELECT cnpj_basico FROM empresas"
                " GROUP BY cnpj_basico HAVING count(*) > 1) d"
            ).fetchone()[0]
            metadata_ativos = conn.execute(
                "SELECT count(*) FROM query_metadata WHERE ativo"
            ).fetchone()[0]
        assert empresas == 2
        assert duplicatas == 0
        assert metadata_ativos == 1

    def test_publication_e_um_registro_unico(self, clean_test_schema):
        import psycopg

        from src.ingest_parallel.serving import publish_serving

        publish_serving(clean_test_schema, _linhas(), competencia="2026-10")
        publish_serving(clean_test_schema, _linhas(), competencia="2026-10")
        with psycopg.connect(clean_test_schema) as conn:
            linhas = conn.execute(
                "SELECT count(*) FROM serving_publication WHERE load_id = %s",
                ("2026-10-retry",),
            ).fetchone()[0]
        assert linhas == 1

    def test_particoes_concluidas_nao_reprocessadas_desnecessariamente(self, clean_test_schema):
        """Evidence no ledger: reexecução idempotente não reabre partições concluídas."""

        import psycopg

        from src.ingest_parallel.orchestrate import (
            ensure_partition_record,
            mark_partition_succeeded,
            partitions_status,
        )
        from src.ingest_parallel.serving import ensure_run_tables

        with psycopg.connect(clean_test_schema) as conn:
            ensure_run_tables(conn)
            conn.commit()
        with psycopg.connect(clean_test_schema) as conn:
            ensure_partition_record(conn, "2026-10-retry", "empresas-00", "empresas")
            mark_partition_succeeded(conn, "2026-10-retry", "empresas-00")
            conn.commit()
        # Segunda passada: partição continua única e succeeded (sem duplicar)
        with psycopg.connect(clean_test_schema) as conn:
            ensure_partition_record(conn, "2026-10-retry", "empresas-00", "empresas")
            conn.commit()
            estado = partitions_status(conn, "2026-10-retry")
        assert len(estado) == 1
        assert estado[0]["status"] == "succeeded"
