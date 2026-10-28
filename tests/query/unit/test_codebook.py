from __future__ import annotations

from src.query.domain.codebook import (
    descricao_matriz_filial,
    descricao_porte,
    descricao_situacao,
)


def test_situacao_codes_are_official():
    assert descricao_situacao("01") == "Nula"
    assert descricao_situacao("2") == "Ativa"
    assert descricao_situacao("03") == "Suspensa"
    assert descricao_situacao("04") == "Inapta"
    assert descricao_situacao("05") == "Ativa Não Regular"
    assert descricao_situacao("08") == "Baixada"


def test_porte_codes_are_official():
    assert descricao_porte("00") == "Não informado"
    assert descricao_porte("1") == "Microempresa"
    assert descricao_porte("03") == "Empresa de Pequeno Porte"
    assert descricao_porte("05") == "Demais empresas"


def test_matriz_filial_codes_are_official():
    assert descricao_matriz_filial("1") == "Matriz"
    assert descricao_matriz_filial("2") == "Filial"


def test_unknown_codes_fall_back_to_original_value():
    assert descricao_situacao("99") == "99"
    assert descricao_porte("99") == "99"
    assert descricao_matriz_filial("9") == "9"
