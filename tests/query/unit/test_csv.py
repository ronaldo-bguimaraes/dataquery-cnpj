from __future__ import annotations

from datetime import date
from decimal import Decimal

from src.query.web.presenters import CSV_HEADER, csv_row, csv_value


def test_csv_value_neutralizes_spreadsheet_formulas():
    assert csv_value("=1+1") == "'=1+1"
    assert csv_value("+55 11 99999-9999") == "'+55 11 99999-9999"
    assert csv_value("-10") == "'-10"
    assert csv_value("@usuario") == "'@usuario"
    assert csv_value("Empresa Alfa") == "Empresa Alfa"


def test_csv_value_formats_dates_booleans_and_nulls():
    assert csv_value(date(2020, 1, 1)) == "2020-01-01"
    assert csv_value(True) == "Sim"
    assert csv_value(False) == "Não"
    assert csv_value(None) == ""
    assert csv_value(0) == "0"


def test_csv_row_has_all_columns_and_derived_fields():
    row = {
        "cnpj_completo": "00000001000136",
        "cnpj_basico": "00000001",
        "razao_social": "=Empresa Perigosa",
        "nome_fantasia": "Alfa",
        "segmento_nome": "Indústria e transformação",
        "situacao_descricao": "Ativa",
        "uf": "SP",
        "municipio_descricao": "SAO PAULO",
        "cep": "01000000",
        "cnae_principal_codigo": "1111301",
        "cnae_secundaria_texto": "2222201,3333301",
        "qtd_cnaes": 3,
        "porte_descricao": "Microempresa",
        "natureza_juridica_descricao": "Sociedade Empresária Limitada",
        "capital_social": Decimal("1500.50"),
        "data_inicio_atividade": date(2020, 1, 1),
        "matriz_filial_descricao": "Matriz",
        "qtd_estabelecimentos": 2,
        "qtd_filiais": 1,
        "ddd_1": "11",
        "telefone_1": "999999999",
        "correio_eletronico": "contato@example.test",
        "optante_simples": True,
        "optante_mei": False,
    }
    values = csv_row(row, date(2026, 10, 31))

    assert len(values) == len(CSV_HEADER)
    assert values[0] == "00000001000136"
    assert values[1] == "00000001"
    assert values[2] == "'=Empresa Perigosa"
    assert values[4] == "Indústria e transformação"
    assert values[13] == "1500.50"
    assert values[14] == "2020-01-01"
    assert values[15] == "6"
    assert values[18] == "1"
    assert values[19] == "(11) 99999-9999"
    assert values[21] == "Sim"
    assert values[22] == "Não"
    assert values[23] == "2222201,3333301"


def test_csv_row_without_opening_date_or_contact():
    values = csv_row(
        {
            "cnpj_completo": "00000001000136",
            "cnpj_basico": "00000001",
            "razao_social": "Empresa",
            "data_inicio_atividade": None,
            "qtd_cnaes": None,
            "capital_social": None,
        },
        date(2026, 10, 31),
    )
    assert values[15] == ""
    assert values[10] == ""
    assert values[13] == ""
    assert values[19] == ""
