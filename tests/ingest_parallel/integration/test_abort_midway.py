"""T024 — Falha no meio da carga: versão anterior preservada, nada parcial (US1 aceite 3)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.quality import DatasetCounts

pytestmark = pytest.mark.integration


def _linhas_exemplo(load_id: str = "2026-10-integra") -> DatasetCounts:
    from tests.ingest_parallel.integration.test_happy_path import _linhas_exemplo

    return _linhas_exemplo(load_id)


class TestAbortMidway:
    def test_falha_impede_publicacao_e_versao_anterior_continua(self, clean_test_schema):
        """US1 aceite 3 (SC-003): falha pré-publicação → anterior ativa, nada parcial."""

        import psycopg

        from src.ingest_parallel.serving import ServingAbort, publish_serving

        with psycopg.connect(clean_test_schema) as conn:
            conn.execute(
                "INSERT INTO query_metadata (competencia, fonte, total_empresas, ativo)"
                " VALUES ('2026-05', 'Receita Federal', 1, true)"
            )
            conn.execute("INSERT INTO empresas (cnpj_basico, razao_social) VALUES ('a', 'antiga')")
            conn.commit()

        dados = _linhas_exemplo()
        dados.empresas.append(dict(dados.empresas[0]))  # duplicata induzida
        with pytest.raises(ServingAbort):
            publish_serving(clean_test_schema, dados, competencia="2026-10")

        with psycopg.connect(clean_test_schema) as conn:
            anterior_ativa = conn.execute(
                "SELECT count(*) FROM query_metadata WHERE competencia='2026-05' AND ativo"
            ).fetchone()[0]
            dados_novos = conn.execute(
                "SELECT count(*) FROM empresas WHERE cnpj_basico='00000001'"
            ).fetchone()[0]
        assert anterior_ativa == 1
        assert dados_novos == 0
