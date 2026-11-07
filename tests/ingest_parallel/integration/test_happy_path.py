"""T017/T024 — Integração do serving com Postgres de teste (ciclos happy-path e abort)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.publish import staging_table_name
from src.ingest_parallel.quality import DatasetCounts

pytestmark = pytest.mark.integration


def _linhas_exemplo(load_id: str = "2026-10-integra") -> DatasetCounts:
    return DatasetCounts(
        competencia="2026-10",
        load_id=load_id,
        empresas=[
            {"cnpj_basico": "00000001", "razao_social": "ALPHA", "competencia": "2026-10", "load_id": load_id},
            {"cnpj_basico": "19131243", "razao_social": "BETA", "competencia": "2026-10", "load_id": load_id},
        ],
        estabelecimentos=[
            {
                "cnpj_completo": "00000001000191",
                "cnpj_basico": "00000001",
                "competencia": "2026-10",
                "load_id": load_id,
            }
        ],
        socios=[
            {
                "cnpj_basico": "00000001",
                "identificador_socio": "1",
                "documento_socio_mascarado": "***.***.***-**",
                "nome_socio": "JOAO",
                "qualificacao_socio_codigo": "10",
                "data_entrada_sociedade": "2020-01-01",
                "competencia": "2026-10",
                "load_id": load_id,
            }
        ],
        simples=[
            {"cnpj_basico": "00000001", "competencia": "2026-10", "load_id": load_id}
        ],
    )


class TestHappyPath:
    def test_publicacao_atomica_e_unico_ativo(self, clean_test_schema):
        import psycopg

        from src.ingest_parallel.serving import publish_serving

        summary = publish_serving(
            clean_test_schema,
            _linhas_exemplo(),
            competencia="2026-10",
        )
        assert summary["published"] is True
        assert summary["counts"]["empresas"] == 2

        with psycopg.connect(clean_test_schema) as conn:
            ativos = conn.execute(
                "SELECT competencia, total_empresas FROM query_metadata WHERE ativo"
            ).fetchall()
            assert len(ativos) == 1
            assert ativos[0][0] == "2026-10"
            assert conn.execute("SELECT count(*) FROM empresas").fetchone()[0] == 2

    def test_versao_anterior_mantida_ate_troca(self, clean_test_schema):
        """Consumidores vêm a versão anterior até a troca (US1 aceite 1, FR-010)."""

        import psycopg

        from src.ingest_parallel.serving import publish_serving

        with psycopg.connect(clean_test_schema) as conn:
            conn.execute(
                "INSERT INTO query_metadata (competencia, fonte, total_empresas, ativo)"
                " VALUES ('2026-05', 'Receita Federal', 1, true)"
            )
            conn.commit()

            antes = conn.execute("SELECT count(*) FROM empresas").fetchone()[0]
        assert antes == 0  # nada publicado ainda

        publish_serving(clean_test_schema, _linhas_exemplo(), competencia="2026-10")

        with psycopg.connect(clean_test_schema) as conn:
            novas = conn.execute("SELECT count(*) FROM empresas").fetchone()[0]
            ativo_antigo = conn.execute(
                "SELECT count(*) FROM query_metadata WHERE competencia='2026-05' AND ativo"
            ).fetchone()[0]
            ativo_novo = conn.execute(
                "SELECT count(*) FROM query_metadata WHERE competencia='2026-10' AND ativo"
            ).fetchone()[0]
        assert novas == 2
        assert ativo_antigo == 0
        assert ativo_novo == 1

    def test_staging_nunca_vira_publisher_sem_swap(self, clean_test_schema):
        """Staging existente, mesmo com linhas, não publica sozinha (FR-009)."""

        import psycopg

        from src.ingest_parallel.serving import create_staging, stage_rows

        load_id = "2026-10-staging-only"
        with psycopg.connect(clean_test_schema) as conn:
            create_staging(conn, load_id)
            stage_rows(conn, load_id, "empresas", [
                {"cnpj_basico": "00000001", "razao_social": "STG", "competencia": "2026-10", "load_id": load_id},
            ])
            conn.commit()
        with psycopg.connect(clean_test_schema) as conn:
            base = conn.execute("SELECT count(*) FROM empresas").fetchone()[0]
            staging = conn.execute(
                f'SELECT count(*) FROM "{staging_table_name("empresas", load_id)}"'
            ).fetchone()[0]
        assert base == 0
        assert staging == 1
