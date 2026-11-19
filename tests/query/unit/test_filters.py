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


def test_prospeccao_filters_are_parsed():
    filtros = parse_filters(
        {
            "segmento": "varejo",
            "idade_min": "2",
            "idade_max": "10",
            "filiais_min": "0",
            "filiais_max": "5",
            "com_contato": "SIM",
        },
        referencia=date(2026, 10, 31),
    )
    assert filtros.segmento == "VAREJO"
    assert filtros.idade_min == 2
    assert filtros.idade_max == 10
    assert filtros.filiais_min == 0
    assert filtros.filiais_max == 5
    assert filtros.com_contato == "sim"
    assert filtros.referencia == date(2026, 10, 31)


def test_prospeccao_defaults():
    filtros = parse_filters({}, referencia=date(2026, 10, 31))
    assert filtros.segmento is None
    assert filtros.idade_min is None
    assert filtros.idade_max is None
    assert filtros.filiais_min is None
    assert filtros.filiais_max is None
    assert filtros.com_contato == "qualquer"
    assert filtros.referencia == date(2026, 10, 31)


@pytest.mark.parametrize(
    "params",
    [
        {"segmento": "INEXISTENTE"},
        {"idade_min": "-1"},
        {"idade_max": "201"},
        {"idade_min": "abc"},
        {"filiais_min": "-1"},
        {"filiais_max": "10001"},
        {"com_contato": "talvez"},
        {"idade_min": "10", "idade_max": "5"},
        {"filiais_min": "5", "filiais_max": "2"},
    ],
)
def test_prospeccao_invalid_values_are_rejected(params):
    with pytest.raises(FilterError):
        parse_filters(params)


def test_prospeccao_query_params_roundtrip():
    filtros = parse_filters(
        {
            "segmento": "ATACADO",
            "idade_min": "1",
            "idade_max": "9",
            "filiais_min": "2",
            "filiais_max": "8",
            "com_contato": "nao",
        }
    )
    params = filtros.as_query_params()
    assert params == {
        "segmento": "ATACADO",
        "idade_min": "1",
        "idade_max": "9",
        "filiais_min": "2",
        "filiais_max": "8",
        "com_contato": "nao",
    }


def test_prospeccao_repeated_new_parameter_is_rejected():
    with pytest.raises(FilterError):
        parse_filters([("segmento", "VAREJO"), ("segmento", "ATACADO")])


def test_explicit_all_situations_roundtrips():
    filtros = parse_filters({"situacao": ""})
    assert filtros.situacao == ""
    assert filtros.as_query_params() == {"situacao": ""}
    # Ausente continua diferente de "todas".
    assert parse_filters({}).situacao is None
    assert parse_filters({}).as_query_params() == {}
