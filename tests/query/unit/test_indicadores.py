from __future__ import annotations

from datetime import date

from src.query.domain.indicadores import (
    deslocar_anos,
    idade_em_anos,
    limites_idade,
    tem_contato,
)


def test_idade_em_anos_completos_ate_a_competencia():
    referencia = date(2026, 10, 31)
    assert idade_em_anos(date(2020, 1, 1), referencia) == 6
    assert idade_em_anos(date(2026, 10, 31), referencia) == 0
    assert idade_em_anos(date(2026, 11, 1), referencia) == 0
    assert idade_em_anos(date(2021, 1, 1), referencia) == 5
    assert idade_em_anos(None, referencia) is None


def test_idade_trata_29_de_fevereiro():
    assert idade_em_anos(date(2024, 2, 29), date(2026, 2, 28)) == 1
    assert idade_em_anos(date(2024, 2, 29), date(2026, 3, 1)) == 2


def test_deslocar_anos_tolera_ano_nao_bissexto():
    assert deslocar_anos(date(2024, 2, 29), 1) == date(2023, 2, 28)
    assert deslocar_anos(date(2026, 10, 31), 6) == date(2020, 10, 31)


def test_limites_idade_vira_intervalo_inclusivo():
    referencia = date(2026, 10, 31)
    abertura_de, abertura_ate = limites_idade(referencia, 5, 10)
    assert abertura_ate == date(2021, 10, 31)
    assert abertura_de == date(2015, 11, 1)

    abertura_de, abertura_ate = limites_idade(referencia, None, 2)
    assert abertura_de == date(2023, 11, 1)
    assert abertura_ate is None

    abertura_de, abertura_ate = limites_idade(referencia, 0, 0)
    assert abertura_de == date(2025, 11, 1)
    assert abertura_ate == date(2026, 10, 31)


def test_limites_idade_sem_filtros():
    assert limites_idade(date(2026, 10, 31), None, None) == (None, None)


def test_presenca_de_contato():
    assert tem_contato("11", "99999999", "") is True
    assert tem_contato("", "", "contato@example.test") is True
    assert tem_contato("11", "", "contato@example.test") is True
    assert tem_contato("", "99999999", "") is False
    assert tem_contato(None, None, None) is False
    assert tem_contato("  ", "  ", "  ") is False
