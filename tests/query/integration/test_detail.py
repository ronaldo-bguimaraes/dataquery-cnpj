from __future__ import annotations

from src.query.db.repositories import (
    listar_cnaes,
    listar_socios,
    obter_estabelecimento,
    obter_simples,
)


def test_detail_returns_establishment_company_and_relations(loaded_db):
    connection, _summary = loaded_db
    row = obter_estabelecimento(connection, "00000001000136")

    assert row is not None
    assert row["razao_social"] == "Empresa Alfa Ltda"
    assert row["uf"] == "SP"
    assert row["municipio_descricao"] == "SAO PAULO"
    assert row["situacao_descricao"] == "Ativa"
    assert row["cnae_principal_descricao"] == "Cultivo de arroz"
    assert row["porte_descricao"] == "Microempresa"

    cnaes = listar_cnaes(connection, "00000001000136")
    assert [item["cnae_codigo"] for item in cnaes] == ["1111301", "2222201", "3333301"]

    socios = listar_socios(connection, "00000001")
    assert len(socios) == 1
    assert socios[0]["nome_socio"] == "Maria Exemplo"
    assert socios[0]["documento_socio_mascarado"] == "***.***.***-**"
    assert socios[0]["qualificacao_socio_descricao"] == "Sócio-Administrador"

    simples = obter_simples(connection, "00000001")
    assert simples is not None
    assert simples["optante_simples"] is True
    assert simples["optante_mei"] is False


def test_detail_without_optional_relations(loaded_db):
    connection, _summary = loaded_db
    row = obter_estabelecimento(connection, "00000003000125")
    assert row is not None
    assert listar_socios(connection, "00000003") == []
    assert obter_simples(connection, "00000003") is None


def test_detail_missing_establishment(loaded_db):
    connection, _summary = loaded_db
    assert obter_estabelecimento(connection, "11222333000181") is None
