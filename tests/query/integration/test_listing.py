from __future__ import annotations

from src.query.db.repositories import (
    contar_estabelecimentos,
    listar_estabelecimentos,
    opcoes_filtro,
)
from src.query.web.filters import parse_filters


def _filters(**params):
    return parse_filters(params)


def test_no_filters_returns_all_ordered_by_cnpj(loaded_db):
    connection, _summary = loaded_db
    rows, total = listar_estabelecimentos(connection, _filters())

    assert total == 4
    assert [row["cnpj_completo"] for row in rows] == [
        "00000001000136",
        "00000001000217",
        "00000002000180",
        "00000003000125",
    ]
    assert contar_estabelecimentos(connection, _filters()) == 4


def test_combined_uf_and_cnae_filters(loaded_db):
    connection, _summary = loaded_db
    rows, total = listar_estabelecimentos(
        connection, _filters(uf="sp", cnae="1111301")
    )
    assert total == 1
    assert [row["cnpj_completo"] for row in rows] == ["00000001000136"]


def test_cnae_matches_principal_and_secondary(loaded_db):
    connection, _summary = loaded_db
    _, secondary = listar_estabelecimentos(connection, _filters(cnae="2222201"))
    _, principal = listar_estabelecimentos(connection, _filters(cnae="3333301"))
    assert secondary == 2
    assert principal == 2


def test_uf_municipio_porte_matriz_and_date_filters(loaded_db):
    connection, _summary = loaded_db
    assert listar_estabelecimentos(connection, _filters(uf="SP", municipio="7107"))[1] == 2
    assert listar_estabelecimentos(connection, _filters(porte="03"))[1] == 1
    assert listar_estabelecimentos(connection, _filters(matriz_filial="2"))[1] == 1
    rows, total = listar_estabelecimentos(
        connection,
        _filters(abertura_de="2021-01-01", abertura_ate="2022-12-31"),
    )
    assert total == 2
    assert [row["cnpj_completo"] for row in rows] == [
        "00000001000217",
        "00000002000180",
    ]


def test_situacao_and_simples_filters(loaded_db):
    connection, _summary = loaded_db
    assert listar_estabelecimentos(connection, _filters(situacao="08"))[1] == 1
    assert listar_estabelecimentos(connection, _filters(simples="sim"))[1] == 2
    assert listar_estabelecimentos(connection, _filters(mei="sim"))[1] == 1
    assert listar_estabelecimentos(connection, _filters(simples="nao"))[1] == 2


def test_literal_cnpj_and_name_search(loaded_db):
    connection, _summary = loaded_db
    assert listar_estabelecimentos(connection, _filters(q="00000001000217"))[1] == 1
    assert listar_estabelecimentos(connection, _filters(q="Alfa"))[1] == 2
    assert listar_estabelecimentos(connection, _filters(q="Açaí"))[1] == 1


def test_pagination_preserves_order_without_duplicates(loaded_db):
    connection, _summary = loaded_db
    first, total = listar_estabelecimentos(connection, _filters(pagina="1"), page_size=2)
    second, _ = listar_estabelecimentos(connection, _filters(pagina="2"), page_size=2)
    assert total == 4
    assert [row["cnpj_completo"] for row in first] == [
        "00000001000136",
        "00000001000217",
    ]
    assert [row["cnpj_completo"] for row in second] == [
        "00000002000180",
        "00000003000125",
    ]
    assert not set(row["cnpj_completo"] for row in first) & set(
        row["cnpj_completo"] for row in second
    )


def test_zero_results_is_not_an_error(loaded_db):
    connection, _summary = loaded_db
    rows, total = listar_estabelecimentos(
        connection, _filters(uf="MG", cnae="2222201")
    )
    assert rows == []
    assert total == 0


def test_filter_options_come_from_inventory(loaded_db):
    connection, _summary = loaded_db
    options = opcoes_filtro(connection, uf="SP")
    assert "SP" in options["ufs"]
    assert options["municipios"] == [{"codigo": "7107", "descricao": "SAO PAULO"}]
    assert any(item["codigo"] == "1111301" for item in options["cnaes"])
    assert options["situacoes"]
    assert options["portes"]
    assert options["matriz_filial"]
