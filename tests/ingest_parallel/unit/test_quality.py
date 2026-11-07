"""T014 — Validadores de qualidade pré-publicação (FR-037, FR-012)."""

from __future__ import annotations

import pytest

from src.ingest_parallel.quality import (
    DatasetCounts,
    QualityError,
    run_quality_checks,
)


@pytest.fixture(name="estrutura_dados")
def estrutura_dados_fixture():
    return DatasetCounts(
        competencia="2026-10",
        load_id="2026-10-",
        empresas=[
            {"cnpj_basico": "00000001", "competencia": "2026-10"},
            {"cnpj_basico": "19131243", "competencia": "2026-10"},
        ],
        estabelecimentos=[
            {"cnpj_completo": "00000001000191", "cnpj_basico": "00000001", "competencia": "2026-10"},
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
            }
        ],
        simples=[{"cnpj_basico": "00000001", "competencia": "2026-10"}],
    )


class TestUnicidade:
    def test_nenhuma_duplicata_nas_chaves(self, estrutura_dados):
        report = run_quality_checks(estrutura_dados, total_esperado={"empresas": 2, "estabelecimentos": 1, "socios": 1, "simples": 1})
        assert report["unicidade"]["empresas"] == []
        assert report["unicidade"]["socios"] == []

    def test_duplicata_detectada(self, estrutura_dados):
        estrutura_dados.empresas.append(dict(estrutura_dados.empresas[0]))
        with pytest.raises(QualityError) as exc:
            run_quality_checks(
                estrutura_dados,
                total_esperado={"empresas": 3, "estabelecimentos": 1, "socios": 1, "simples": 1},
            )
        assert exc.value.code == "STAGING_UNIQUE_DUP"


class TestReferencial:
    def test_fk_orfao_detectada(self, estrutura_dados):
        estrutura_dados.estabelecimentos.append(
            {"cnpj_completo": "99999999000199", "cnpj_basico": "99999999", "competencia": "2026-10"}
        )
        with pytest.raises(QualityError) as exc:
            run_quality_checks(
                estrutura_dados,
                total_esperado={"empresas": 2, "estabelecimentos": 1, "socios": 1, "simples": 1},
            )
        assert exc.value.code == "STAGING_FK_ORPHAN"


class TestContagens:
    def test_contagem_divergente(self, estrutura_dados):
        with pytest.raises(QualityError) as exc:
            run_quality_checks(
                estrutura_dados,
                total_esperado={"empresas": 999, "estabelecimentos": 1, "socios": 1, "simples": 1},
            )
        assert exc.value.code == "STAGING_COUNT_MISMATCH"


class TestSchema:
    def test_schema_mismatch_detectado(self, estrutura_dados):
        estrutura_dados.empresas[0].pop("competencia")
        with pytest.raises(QualityError) as exc:
            run_quality_checks(
                estrutura_dados,
                total_esperado={"empresas": 2, "estabelecimentos": 1, "socios": 1, "simples": 1},
                schema_obrigatorio={"empresas": ["cnpj_basico", "competencia"]},
            )
        assert exc.value.code == "STAGING_SCHEMA_MISMATCH"


class TestTudoOk:
    def test_relatorio_completo_passa(self, estrutura_dados):
        report = run_quality_checks(
            estrutura_dados,
            total_esperado={"empresas": 2, "estabelecimentos": 1, "socios": 1, "simples": 1},
            schema_obrigatorio={
                "empresas": ["cnpj_basico", "competencia"],
                "estabelecimentos": ["cnpj_completo", "competencia"],
                "socios": ["cnpj_basico", "competencia"],
                "simples": ["cnpj_basico", "competencia"],
            },
        )
        assert all(tabela == [] or tabela == 0 for tabela in report["unicidade"].values())
        assert report["fk_orfas"] == []
        assert report["contagens_ok"] is True
