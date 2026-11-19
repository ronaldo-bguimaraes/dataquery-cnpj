from __future__ import annotations

from datetime import date

from src.query.db.repositories import (
    agrupar_prospeccao,
    contar_estabelecimentos,
    iterar_prospeccao,
    listar_prospeccao,
)
from src.query.web.filters import parse_filters

REFERENCIA = date(2026, 10, 31)


def _filters(**params):
    return parse_filters(params, referencia=REFERENCIA)


def test_read_model_expoe_indicadores_e_contatos(loaded_db):
    connection, _summary = loaded_db
    row = connection.execute(
        """
        SELECT segmento_codigo, segmento_nome, qtd_cnaes,
               qtd_estabelecimentos, qtd_filiais, cep, ddd_1, telefone_1,
               correio_eletronico, natureza_juridica_descricao, capital_social,
               cnae_secundaria_texto
        FROM estabelecimentos_listagem
        WHERE cnpj_completo = '00000001000136'
        """
    ).fetchone()
    assert row[0] == "INDUSTRIA"
    assert row[1] == "Indústria e transformação"
    assert row[2] == 3
    assert row[3] == 2
    assert row[4] == 1
    assert row[5] == "01000000"
    assert row[6] == "11"
    assert row[7] == "99999999"
    assert row[8] == "contato@example.test"
    assert row[9] == "Sociedade Empresária Limitada"
    assert float(row[10]) == 5000.0
    assert row[11] == "2222201,3333301"


def test_read_model_classifica_varejo_e_nao_classificado(loaded_db):
    connection, _summary = loaded_db
    varejo = connection.execute(
        "SELECT segmento_codigo, qtd_cnaes, qtd_filiais "
        "FROM estabelecimentos_listagem WHERE cnpj_completo = '00000003000125'"
    ).fetchone()
    assert varejo == ("VAREJO", 1, 0)

    # CNAE ausente cai em "Não classificado" sem quebrar a carga.
    connection.execute(
        "UPDATE estabelecimentos_listagem SET cnae_principal_codigo = NULL, "
        "segmento_codigo = 'NAO_CLASSIFICADO', segmento_nome = 'Não classificado' "
        "WHERE cnpj_completo = '00000003000125'"
    )
    connection.commit()
    assert connection.execute(
        "SELECT segmento_nome FROM estabelecimentos_listagem "
        "WHERE cnpj_completo = '00000003000125'"
    ).fetchone()[0] == "Não classificado"


def test_filter_by_segmento(loaded_db):
    connection, _summary = loaded_db
    rows, total = listar_prospeccao(connection, _filters(segmento="INDUSTRIA"))
    assert total == 3
    assert [row["segmento_codigo"] for row in rows] == ["INDUSTRIA"] * 3

    rows, total = listar_prospeccao(connection, _filters(segmento="VAREJO"))
    assert total == 1
    assert rows[0]["cnpj_completo"] == "00000003000125"


def test_filter_by_contact_presence(loaded_db):
    connection, _summary = loaded_db
    com_contato, total = listar_prospeccao(connection, _filters(com_contato="sim"))
    assert total == 3
    assert all(row["correio_eletronico"] for row in com_contato)

    sem_contato, total = listar_prospeccao(connection, _filters(com_contato="nao"))
    assert total == 1
    assert sem_contato[0]["cnpj_completo"] == "00000001000217"


def test_filter_by_branch_count(loaded_db):
    connection, _summary = loaded_db
    com_filial, total = listar_prospeccao(connection, _filters(filiais_min="1"))
    # qtd_filiais é do CNPJ raiz: matriz e filial da mesma empresa entram juntas.
    assert total == 2
    assert {row["cnpj_basico"] for row in com_filial} == {"00000001"}
    assert all(row["qtd_filiais"] == 1 for row in com_filial)

    sem_filial, total = listar_prospeccao(connection, _filters(filiais_max="0"))
    assert total == 2
    assert all(row["qtd_filiais"] == 0 for row in sem_filial)


def test_filter_by_age_uses_competence_reference(loaded_db):
    connection, _summary = loaded_db
    mais_antigas, total = listar_prospeccao(connection, _filters(idade_min="5"))
    assert total == 2
    assert [row["cnpj_completo"] for row in mais_antigas] == [
        "00000001000136",
        "00000001000217",
    ]

    novas, total = listar_prospeccao(connection, _filters(idade_max="2"))
    assert total == 1
    assert novas[0]["cnpj_completo"] == "00000003000125"

    exatas, total = listar_prospeccao(
        connection, _filters(idade_min="4", idade_max="4")
    )
    assert total == 1
    assert exatas[0]["cnpj_completo"] == "00000002000180"


def test_filter_combines_situacao_and_segmento(loaded_db):
    connection, _summary = loaded_db
    ativos_varejo, total = listar_prospeccao(
        connection, _filters(situacao="02", segmento="VAREJO")
    )
    assert total == 1

    ativos_industria, total = listar_prospeccao(
        connection, _filters(situacao="02", segmento="INDUSTRIA")
    )
    assert total == 2


def test_group_by_segmento_and_uf(loaded_db):
    connection, _summary = loaded_db
    grupos = agrupar_prospeccao(connection, _filters())
    assert grupos["segmentos"] == [
        {"codigo": "INDUSTRIA", "rotulo": "Indústria e transformação", "total": 3},
        {"codigo": "VAREJO", "rotulo": "Comércio varejista", "total": 1},
    ]
    assert grupos["ufs"] == [
        {"codigo": "SP", "rotulo": "SP", "total": 2},
        {"codigo": "MG", "rotulo": "MG", "total": 1},
        {"codigo": "RJ", "rotulo": "RJ", "total": 1},
    ]


def test_group_respects_active_default(loaded_db):
    connection, _summary = loaded_db
    grupos = agrupar_prospeccao(connection, _filters(situacao="02"))
    assert sum(item["total"] for item in grupos["segmentos"]) == 3
    assert sum(item["total"] for item in grupos["ufs"]) == 3


def test_iterate_export_respects_limit_and_order(loaded_db):
    connection, _summary = loaded_db
    with connection.transaction():
        rows = list(iterar_prospeccao(connection, _filters(), limit=2))
    assert [row["cnpj_completo"] for row in rows] == [
        "00000001000136",
        "00000001000217",
    ]


def test_load_publishes_count_aggregates(loaded_db):
    connection, _summary = loaded_db
    total = connection.execute(
        "SELECT COALESCE(sum(total), 0) FROM estabelecimentos_filtros_stats"
    ).fetchone()[0]
    assert total == 4
    cnae_total = connection.execute(
        "SELECT COALESCE(sum(total), 0) FROM estabelecimentos_cnae_uf_stats"
    ).fetchone()[0]
    assert cnae_total == 6


def test_count_falls_back_when_aggregates_are_missing(loaded_db):
    connection, _summary = loaded_db
    connection.execute(
        "TRUNCATE estabelecimentos_filtros_stats, estabelecimentos_cnae_uf_stats"
    )
    connection.commit()
    assert contar_estabelecimentos(connection, _filters(situacao="08")) == 1
    assert contar_estabelecimentos(connection, _filters(cnae="2222201")) == 2
