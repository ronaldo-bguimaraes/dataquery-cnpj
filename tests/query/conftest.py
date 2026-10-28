from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.query.fixtures.parquets import escrever_inventario_completo

TEST_DSN = os.getenv(
    "CNPJ_QUERY_TEST_DATABASE_URL",
    "postgresql://cnpj_query_loader:change-me@127.0.0.1:55432/cnpj_query_test",
)
APP_DSN = os.getenv(
    "CNPJ_QUERY_DATABASE_URL",
    "postgresql://cnpj_query_app:change-me@127.0.0.1:55432/cnpj_query_test",
)

DATA_TABLES = [
    "estabelecimentos_listagem",
    "estabelecimento_cnaes",
    "socios",
    "simples",
    "estabelecimentos",
    "empresas",
    "municipios_por_uf",
    "cnaes_presentes",
    "query_metadata",
    "cnaes",
    "municipios",
    "naturezas_juridicas",
    "qualificacoes_socios",
    "motivos",
    "paises",
]


@pytest.fixture
def source_dir(tmp_path: Path) -> Path:
    return escrever_inventario_completo(tmp_path / "bronze" / "2026-10")


@pytest.fixture
def query_dsn() -> str:
    return TEST_DSN


@pytest.fixture
def app_dsn() -> str:
    return APP_DSN


@pytest.fixture
def query_db(query_dsn: str):
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency guard
        pytest.skip(f"psycopg indisponível: {exc}")

    from src.query.ingest.postgres import apply_schema

    try:
        connection = psycopg.connect(query_dsn)
    except Exception as exc:  # noqa: BLE001 - test infrastructure
        pytest.skip(f"PostgreSQL de teste indisponível: {exc}")

    apply_schema(connection)
    _truncate(connection)
    try:
        yield connection
    finally:
        try:
            _truncate(connection)
        finally:
            connection.close()


def _truncate(connection) -> None:
    connection.execute(
        "TRUNCATE TABLE " + ", ".join(DATA_TABLES) + " RESTART IDENTITY CASCADE"
    )
    connection.commit()


@pytest.fixture
def loaded_db(query_db, source_dir: Path, query_dsn: str):
    from src.query.ingest.load import load_inventory

    summary = load_inventory(source_dir, query_dsn, competencia="2026-10")
    return query_db, summary


@pytest.fixture
def app_client(loaded_db, monkeypatch):
    monkeypatch.setenv("CNPJ_QUERY_DATABASE_URL", APP_DSN)
    monkeypatch.setenv("CNPJ_QUERY_COMPETENCIA", "2026-10")
    monkeypatch.setenv("CNPJ_QUERY_PAGE_SIZE", "25")

    from fastapi.testclient import TestClient

    from src.query.web.app import create_app

    app = create_app()
    with TestClient(app) as client:
        yield client
