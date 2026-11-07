"""T041/T042 — Rastreabilidade do resultado e rollback de versão (FR-042/045/047)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.quality import DatasetCounts

pytestmark = pytest.mark.integration


def _dataset(load_id: str, razao: str = "VERSAO") -> DatasetCounts:
    from tests.ingest_parallel.integration.test_happy_path import _linhas_exemplo

    dados = _linhas_exemplo(load_id)
    for row in dados.empresas:
        row["razao_social"] = f"{razao}-{load_id}"
    return dados


@pytest.fixture(name="duas_versoes")
def duas_versoes_fixture(clean_test_schema):
    from src.ingest_parallel.serving import publish_serving

    resumo_v1 = publish_serving(clean_test_schema, _dataset("2026-10-v1"), competencia="2026-10")
    resumo_v2 = publish_serving(clean_test_schema, _dataset("2026-10-v2", "ALTERADO"), competencia="2026-10")
    return clean_test_schema, resumo_v1, resumo_v2


class TestLineage:
    def test_metadados_apontam_fonte_execucao_e_publicacao(self, duas_versoes):
        import psycopg

        dsn, resumo_v1, resumo_v2 = duas_versoes
        load_id = resumo_v2["load_id"]
        with psycopg.connect(dsn) as conn:
            pub = conn.execute(
                "SELECT publication_id, competencia, load_id, previous_active, status"
                " FROM serving_publication WHERE load_id = %s",
                (load_id,),
            ).fetchone()
            run = conn.execute(
                "SELECT load_id, competencia FROM ingestion_run WHERE load_id = %s LIMIT 1", (load_id,)
            ).fetchone()
        assert pub is not None
        assert pub[1] == "2026-10"  # competência da fonte (FR-042)
        assert pub[3] == "2026-10-v1"  # versão anterior acessível (FR-047)

    def test_versao_anterior_continua_identificavel(self, duas_versoes):
        import psycopg

        dsn, resumo_v1, _ = duas_versoes
        with psycopg.connect(dsn) as conn:
            pub_v1 = conn.execute(
                "SELECT publication_id, previous_active, status FROM serving_publication WHERE load_id = %s",
                (resumo_v1["load_id"],),
            ).fetchone()
        assert pub_v1 is not None
        assert pub_v1[2] == "published"


class TestRollback:
    def test_rollback_restaura_versao_anterior(self, duas_versoes):
        import psycopg

        from src.ingest_parallel.serving import publish_serving, rollback_serving

        dsn, resumo_v1, resumo_v2 = duas_versoes
        rollback_serving(dsn, resumo_v2["load_id"])

        with psycopg.connect(dsn) as conn:
            empresas = {
                row[0]
                for row in conn.execute("SELECT razao_social FROM empresas WHERE cnpj_basico='00000001'").fetchall()
            }
            publique = conn.execute(
                "SELECT status FROM serving_publication WHERE load_id = %s", (resumo_v2["load_id"],)
            ).fetchone()[0]
        assert empresas == {f"VERSAO-{resumo_v1['load_id']}"}
        assert publique == "rolled_back"
