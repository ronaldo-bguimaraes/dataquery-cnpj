"""Shared fixtures for the ingest_parallel test suite (feature 003)."""

from __future__ import annotations

import os

import pytest

TEST_DSN = os.environ.get(
    "CNPJ_INGEST_TEST_DATABASE_URL",
    "postgresql://cnpj_query_loader:change-me@127.0.0.1:55432/cnpj_query_test",
)


@pytest.fixture(name="ingest_test_dsn")
def ingest_test_dsn_fixture():
    return TEST_DSN


def pytest_collection_modifyitems(config, items):  # noqa: ANN001
    """Skip Postgres integration tests when no test database is reachable."""

    import psycopg

    reachable = False
    try:
        with psycopg.connect(TEST_DSN, connect_timeout=3):
            reachable = True
    except Exception:
        reachable = False
    if reachable:
        return
    skip_marker = pytest.mark.skip(reason="Postgres test database not available")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)
