from __future__ import annotations

import pytest

from src.query.domain.segmentos import (
    DIVISOES,
    NAO_CLASSIFICADO,
    SEGMENTOS,
    divisao_cnae,
    divisao_rows,
    is_segmento_valido,
    segmento_codigo,
    segmento_nome,
    segmento_para_cnae,
)


@pytest.mark.parametrize(
    ("cnae", "esperado"),
    [
        ("0111301", "AGRO"),
        ("0500100", "AGRO"),
        ("1011205", "INDUSTRIA"),
        ("3321000", "INDUSTRIA"),
        ("3511500", "UTILIDADES"),
        ("4120400", "CONSTRUCAO"),
        ("4511101", "VEICULOS"),
        ("4611300", "ATACADO"),
        ("4711301", "VAREJO"),
        ("4930202", "TRANSPORTE"),
        ("5510801", "ALOJAMENTO"),
        ("6201500", "TECNOLOGIA"),
        ("6421200", "FINANCEIRO"),
        ("6810201", "IMOBILIARIO"),
        ("6911701", "SERVICOS_PROFISSIONAIS"),
        ("7810800", "SERVICOS_APOIO"),
        ("8411300", "ADMIN_PUBLICA"),
        ("8513900", "EDUCACAO"),
        ("8610101", "SAUDE"),
        ("9001901", "CULTURA"),
        ("9511800", "OUTROS_SERVICOS"),
        ("9700500", NAO_CLASSIFICADO),
        ("9999999", NAO_CLASSIFICADO),
    ],
)
def test_classifica_por_divisao_cnae(cnae: str, esperado: str):
    assert segmento_codigo(cnae) == esperado


def test_cnae_ausente_ou_invalido_nao_classifica():
    assert segmento_codigo(None) == NAO_CLASSIFICADO
    assert segmento_codigo("") == NAO_CLASSIFICADO
    assert segmento_codigo("1") == NAO_CLASSIFICADO
    assert segmento_codigo("abc") == NAO_CLASSIFICADO
    assert divisao_cnae(None) is None
    assert divisao_cnae("4711301") == "47"


def test_catalogo_tem_codigos_unicos_e_nomes():
    codigos = [item.codigo for item in SEGMENTOS]
    assert len(codigos) == len(set(codigos))
    assert codigos.count(NAO_CLASSIFICADO) == 1
    assert all(item.nome.strip() for item in SEGMENTOS)


def test_toda_divisao_mapeada_aponta_para_segmento_existente():
    codigos = {item.codigo for item in SEGMENTOS}
    assert set(DIVISOES.values()) <= codigos
    assert len(DIVISOES) == len(divisao_rows())
    assert segmento_nome(NAO_CLASSIFICADO) == "Não classificado"
    assert segmento_nome("NAO_EXISTE") == "Não classificado"
    assert segmento_para_cnae("4711301").codigo == "VAREJO"
    assert is_segmento_valido("VAREJO") is True
    assert is_segmento_valido("varejo") is False
    assert is_segmento_valido(None) is False
