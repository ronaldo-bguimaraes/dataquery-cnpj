"""Parameterized PostgreSQL queries used by the HTML and future API adapters."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row


def fetch_all(connection: psycopg.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def fetch_one(connection: psycopg.Connection, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()


def ping(connection: psycopg.Connection) -> bool:
    return connection.execute("SELECT 1").fetchone()[0] == 1


def active_metadata(connection: psycopg.Connection) -> dict[str, Any] | None:
    return fetch_one(
        connection,
        """
        SELECT competencia, fonte, carregado_em, total_empresas,
               total_estabelecimentos, total_socios
        FROM query_metadata
        WHERE ativo
        ORDER BY carregado_em DESC
        LIMIT 1
        """,
    )


from datetime import date  # noqa: E402
from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from src.query.web.filters import EstabelecimentoFiltros


COUNT_CAP = 50_000


def _has_filters(filtros: "EstabelecimentoFiltros") -> bool:
    return any(
        [
            filtros.q,
            filtros.situacao,
            filtros.uf,
            filtros.municipio,
            filtros.cnae,
            filtros.porte,
            filtros.matriz_filial,
            filtros.abertura_de,
            filtros.abertura_ate,
            filtros.simples != "qualquer",
            filtros.mei != "qualquer",
        ]
    )


def _where(filtros: "EstabelecimentoFiltros") -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if filtros.q:
        if filtros.q.isdigit() and len(filtros.q) == 14:
            clauses.append("e.cnpj_completo = %s")
            params.append(filtros.q)
        else:
            clauses.append("e.razao_social_normalizada LIKE %s")
            params.append(f"%{filtros.q}%")
    if filtros.situacao:
        clauses.append("e.situacao_codigo = %s")
        params.append(filtros.situacao)
    if filtros.uf:
        clauses.append("e.uf = %s")
        params.append(filtros.uf)
    if filtros.municipio:
        clauses.append("e.municipio_codigo = %s")
        params.append(filtros.municipio)
    if filtros.cnae:
        clauses.append(
            "e.cnpj_completo IN ("
            "SELECT ec.cnpj_completo FROM estabelecimento_cnaes ec "
            "WHERE ec.cnae_codigo = %s)"
        )
        params.append(filtros.cnae)
    if filtros.porte:
        clauses.append("e.porte_codigo = %s")
        params.append(filtros.porte)
    if filtros.matriz_filial:
        clauses.append("e.identificador_matriz_filial = %s")
        params.append(filtros.matriz_filial)
    if filtros.abertura_de:
        clauses.append("e.data_inicio_atividade >= %s")
        params.append(filtros.abertura_de)
    if filtros.abertura_ate:
        clauses.append("e.data_inicio_atividade <= %s")
        params.append(filtros.abertura_ate)
    if filtros.simples == "sim":
        clauses.append("e.optante_simples = true")
    elif filtros.simples == "nao":
        clauses.append("e.optante_simples = false")
    if filtros.mei == "sim":
        clauses.append("e.optante_mei = true")
    elif filtros.mei == "nao":
        clauses.append("e.optante_mei = false")

    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(clauses), params


def _stats_where(filtros: "EstabelecimentoFiltros") -> tuple[str, list[Any]]:
    """WHERE sobre a tabela pré-agregada (dimensões de baixa cardinalidade)."""

    clauses: list[str] = []
    params: list[Any] = []
    if filtros.uf:
        clauses.append("uf = %s")
        params.append(filtros.uf)
    if filtros.situacao:
        clauses.append("situacao_codigo = %s")
        params.append(filtros.situacao)
    if filtros.porte:
        clauses.append("porte_codigo = %s")
        params.append(filtros.porte)
    if filtros.matriz_filial:
        clauses.append("matriz_filial = %s")
        params.append(filtros.matriz_filial)
    if filtros.simples == "sim":
        clauses.append("optante_simples = true")
    elif filtros.simples == "nao":
        clauses.append("optante_simples = false")
    if filtros.mei == "sim":
        clauses.append("optante_mei = true")
    elif filtros.mei == "nao":
        clauses.append("optante_mei = false")
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def _stats_eligible(filtros: "EstabelecimentoFiltros") -> bool:
    """Filtros que a pré-agregação por dimensões cobre (sem texto/município/CNAE/data)."""

    return not any(
        (
            filtros.q,
            filtros.municipio,
            filtros.cnae,
            filtros.abertura_de,
            filtros.abertura_ate,
        )
    )


def contar_estabelecimentos(
    connection: psycopg.Connection, filtros: "EstabelecimentoFiltros"
) -> int:
    if not _has_filters(filtros):
        metadata = active_metadata(connection)
        return int(metadata["total_estabelecimentos"]) if metadata else 0
    if _stats_eligible(filtros):
        where, params = _stats_where(filtros)
        row = connection.execute(
            f"SELECT COALESCE(sum(total), 0) FROM estabelecimentos_filtros_stats {where}",
            tuple(params),
        ).fetchone()
        return int(row[0])
    where, params = _where(filtros)
    # Contagem limitada: para filtros amplos (texto comum, CNAE gigante) parar em
    # COUNT_CAP+1 evita varrer dezenas de milhões de linhas só para exibir o total.
    row = connection.execute(
        "SELECT count(*) FROM ("
        f"SELECT 1 FROM estabelecimentos_listagem e {where} LIMIT {COUNT_CAP + 1}"
        ") capped",
        tuple(params),
    ).fetchone()
    return int(row[0])


LISTING_COLUMNS = """    e.cnpj_completo,
    e.cnpj_basico,
    e.razao_social,
    e.nome_fantasia,
    e.situacao_codigo,
    e.situacao_descricao,
    e.uf,
    e.municipio_codigo,
    e.municipio_descricao,
    e.cnae_principal_codigo,
    e.cnae_principal_descricao,
    e.identificador_matriz_filial,
    e.matriz_filial_descricao,
    e.data_inicio_atividade,
    e.porte_codigo,
    e.porte_descricao,
    e.optante_simples,
    e.optante_mei
"""


def listar_estabelecimentos(
    connection: psycopg.Connection,
    filtros: "EstabelecimentoFiltros",
    *,
    page_size: int = 25,
) -> tuple[list[dict[str, Any]], int]:
    total = contar_estabelecimentos(connection, filtros)
    where, params = _where(filtros)
    offset = (filtros.pagina - 1) * page_size
    rows = fetch_all(
        connection,
        f"""
        SELECT {LISTING_COLUMNS}
        FROM estabelecimentos_listagem e
        {where}
        ORDER BY e.cnpj_completo
        LIMIT %s OFFSET %s
        """,
        tuple(params + [page_size, offset]),
    )
    return rows, total


def opcoes_filtro(
    connection: psycopg.Connection, *, uf: str | None = None
) -> dict[str, list[dict[str, Any]]]:
    ufs = fetch_all(
        connection,
        """
        SELECT DISTINCT uf
        FROM municipios_por_uf
        WHERE uf IS NOT NULL AND uf <> ''
        ORDER BY uf
        """,
    )
    municipios = (
        fetch_all(
            connection,
            """
            SELECT municipio_codigo AS codigo, municipio_descricao AS descricao
            FROM municipios_por_uf
            WHERE uf = %s
            ORDER BY municipio_descricao
            """,
            (uf,),
        )
        if uf
        else []
    )
    return {
        "ufs": [row["uf"] for row in ufs],
        "municipios": municipios,
        "cnaes": fetch_all(
            connection,
            "SELECT cnae_codigo AS codigo, descricao FROM cnaes_presentes ORDER BY cnae_codigo",
        ),
        "situacoes": fetch_all(
            connection,
            "SELECT codigo, descricao FROM situacoes_cadastrais ORDER BY codigo",
        ),
        "portes": fetch_all(
            connection, "SELECT codigo, descricao FROM portes ORDER BY codigo"
        ),
        "matriz_filial": fetch_all(
            connection,
            "SELECT codigo, descricao FROM matriz_filial ORDER BY codigo",
        ),
    }


def obter_estabelecimento(
    connection: psycopg.Connection, cnpj: str
) -> dict[str, Any] | None:
    return fetch_one(
        connection,
        """
        SELECT
            e.cnpj_completo,
            e.cnpj_basico,
            e.cnpj_ordem,
            e.cnpj_dv,
            e.identificador_matriz_filial,
            mf.descricao AS matriz_filial_descricao,
            e.nome_fantasia,
            e.situacao_cadastral,
            sc.descricao AS situacao_descricao,
            e.data_situacao_cadastral,
            e.motivo_situacao_cadastral,
            mo.descricao AS motivo_descricao,
            e.nome_cidade_exterior,
            e.pais_codigo,
            pa.descricao AS pais_descricao,
            e.data_inicio_atividade,
            e.cnae_principal,
            c.descricao AS cnae_principal_descricao,
            e.cnae_secundaria_texto,
            e.tipo_logradouro,
            e.logradouro,
            e.numero,
            e.complemento,
            e.bairro,
            e.cep,
            e.uf,
            e.municipio_codigo,
            mu.descricao AS municipio_descricao,
            e.ddd_1,
            e.telefone_1,
            e.ddd_2,
            e.telefone_2,
            e.ddd_fax,
            e.numero_fax,
            e.correio_eletronico,
            e.situacao_especial,
            e.data_situacao_especial,
            emp.razao_social,
            emp.cnpj_basico AS empresa_cnpj_basico,
            emp.natureza_juridica_codigo,
            nj.descricao AS natureza_juridica_descricao,
            emp.qualificacao_responsavel_codigo,
            qr.descricao AS qualificacao_responsavel_descricao,
            emp.capital_social,
            emp.porte_codigo,
            po.descricao AS porte_descricao,
            emp.ente_federativo
        FROM estabelecimentos e
        JOIN empresas emp ON emp.cnpj_basico = e.cnpj_basico
        LEFT JOIN matriz_filial mf ON mf.codigo = e.identificador_matriz_filial
        LEFT JOIN situacoes_cadastrais sc ON sc.codigo = lpad(e.situacao_cadastral, 2, '0')
        LEFT JOIN motivos mo ON mo.codigo = e.motivo_situacao_cadastral
        LEFT JOIN paises pa ON pa.codigo = e.pais_codigo
        LEFT JOIN cnaes c ON c.codigo = e.cnae_principal
        LEFT JOIN municipios mu ON mu.codigo = e.municipio_codigo
        LEFT JOIN naturezas_juridicas nj ON nj.codigo = emp.natureza_juridica_codigo
        LEFT JOIN qualificacoes_socios qr ON qr.codigo = emp.qualificacao_responsavel_codigo
        LEFT JOIN portes po ON po.codigo = lpad(emp.porte_codigo, 2, '0')
        WHERE e.cnpj_completo = %s
        """,
        (cnpj,),
    )


def listar_cnaes(connection: psycopg.Connection, cnpj: str) -> list[dict[str, Any]]:
    return fetch_all(
        connection,
        """
        SELECT ec.cnae_codigo, COALESCE(c.descricao, ec.cnae_codigo) AS descricao,
               ec.principal
        FROM estabelecimento_cnaes ec
        LEFT JOIN cnaes c ON c.codigo = ec.cnae_codigo
        WHERE ec.cnpj_completo = %s
        ORDER BY ec.principal DESC, ec.cnae_codigo
        """,
        (cnpj,),
    )


def listar_socios(
    connection: psycopg.Connection, cnpj_basico: str
) -> list[dict[str, Any]]:
    return fetch_all(
        connection,
        """
        SELECT
            s.nome_socio,
            s.documento_socio_mascarado,
            s.qualificacao_socio_codigo,
            qs.descricao AS qualificacao_socio_descricao,
            s.data_entrada_sociedade,
            s.pais_codigo,
            p.descricao AS pais_descricao,
            s.representante_legal,
            s.nome_representante,
            s.qualificacao_representante_legal,
            qr.descricao AS qualificacao_representante_descricao,
            s.faixa_etaria
        FROM socios s
        LEFT JOIN qualificacoes_socios qs ON qs.codigo = s.qualificacao_socio_codigo
        LEFT JOIN paises p ON p.codigo = s.pais_codigo
        LEFT JOIN qualificacoes_socios qr
            ON qr.codigo = s.qualificacao_representante_legal
        WHERE s.cnpj_basico = %s
        ORDER BY s.nome_socio, s.id
        """,
        (cnpj_basico,),
    )


def obter_simples(
    connection: psycopg.Connection, cnpj_basico: str
) -> dict[str, Any] | None:
    return fetch_one(
        connection,
        "SELECT * FROM simples WHERE cnpj_basico = %s",
        (cnpj_basico,),
    )
