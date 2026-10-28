from __future__ import annotations

from datetime import date

import pytest

from src.query.web.filters import FilterError, parse_filters


def test_defaults():
    filtros = parse_filters({})
    assert filtros.q is None
    assert filtros.uf is None
    assert filtros.pagina == 1
    assert filtros.simples == "qualquer"
    assert filtros.mei == "qualquer"


def test_normalizes_cnpj_and_name():
    assert parse_filters({"q": "11.222.333/0001-81"}).q == "11222333000181"
    assert parse_filters({"q": "Comércio Açaí"}).q == "comercio acai"


def test_invalid_cnpj_is_rejected():
    with pytest.raises(FilterError, match="CNPJ"):
        parse_filters({"q": "11.222.333/0001-82"})
    with pytest.raises(FilterError, match="CNPJ"):
        parse_filters({"q": "123"})


def test_uf_municipio_cnae_porte_and_matriz_are_normalized():
    filtros = parse_filters(
        {
            "uf": "sp",
            "municipio": "7107",
            "cnae": "1111301",
            "porte": "1",
            "matriz_filial": "2",
            "situacao": "2",
        }
    )
    assert filtros.uf == "SP"
    assert filtros.municipio == "7107"
    assert filtros.cnae == "1111301"
    assert filtros.porte == "01"
    assert filtros.matriz_filial == "2"
    assert filtros.situacao == "02"


@pytest.mark.parametrize(
    "params",
    [
        {"uf": "ZZ"},
        {"municipio": "abc"},
        {"cnae": "111"},
        {"porte": "99"},
        {"matriz_filial": "9"},
        {"situacao": "99"},
        {"simples": "talvez"},
        {"mei": "talvez"},
    ],
)
def test_invalid_filter_values_are_rejected(params):
    with pytest.raises(FilterError):
        parse_filters(params)


def test_date_range_is_inclusive_and_ordered():
    filtros = parse_filters({"abertura_de": "2020-01-01", "abertura_ate": "2020-12-31"})
    assert filtros.abertura_de == date(2020, 1, 1)
    assert filtros.abertura_ate == date(2020, 12, 31)
    with pytest.raises(FilterError):
        parse_filters({"abertura_de": "2021-01-01", "abertura_ate": "2020-12-31"})
    with pytest.raises(FilterError):
        parse_filters({"abertura_de": "2020-13-01"})


def test_page_and_repeated_parameter():
    assert parse_filters({"pagina": "2"}).pagina == 2
    with pytest.raises(FilterError):
        parse_filters({"pagina": "0"})
    with pytest.raises(FilterError):
        parse_filters([("uf", "SP"), ("uf", "RJ")])


def test_short_name_is_rejected():
    with pytest.raises(FilterError):
        parse_filters({"q": "ab"})
