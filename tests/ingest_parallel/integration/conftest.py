"""Fixtures de integração do serving (Postgres de teste)."""

from __future__ import annotations

import pytest


@pytest.fixture(name="clean_test_schema")
def clean_test_schema_fixture(ingest_test_dsn):
    """Reset mínimo: tabelas de carga e staging remanescentes de testes anteriores."""

    import psycopg

    with psycopg.connect(ingest_test_dsn) as conn:
        for table in ("ingestion_run", "load_partition", "serving_publication"):
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        nomes = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'"
            " AND (tablename LIKE 'staging_%' OR tablename LIKE 'archived_%')"
        ).fetchall()
        for (name,) in nomes:
            conn.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')
        # O banco de teste do portal tem o schema completo; derruba o core com dependências
        # para aplicar só os mínimos nesta bateria (tabelas derivadas recriadas aqui).
        for table in (
            "estabelecimentos_listagem",
            "estabelecimento_cnaes",
            "empresas",
            "estabelecimentos",
            "socios",
            "simples",
            "query_metadata",
            "cnaes",
            "municipios",
            "municipios_por_uf",
            "cnaes_presentes",
            "naturezas_juridicas",
            "qualificacoes_socios",
            "motivos",
            "paises",
            "situacoes_cadastrais",
            "matriz_filial",
            "portes",
        ):
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS empresas (cnpj_basico text PRIMARY KEY, razao_social text)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS estabelecimentos (cnpj_completo text PRIMARY KEY, cnpj_basico text)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS socios (id serial PRIMARY KEY, cnpj_basico text)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS simples (cnpj_basico text PRIMARY KEY)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS query_metadata ("
            "competencia text PRIMARY KEY, fonte text, carregado_em timestamptz,"
            " total_empresas bigint, total_estabelecimentos bigint, total_socios bigint, ativo boolean)"
        )
        conn.execute("TRUNCATE empresas, estabelecimentos, socios, simples, query_metadata")
        conn.commit()
    yield ingest_test_dsn
