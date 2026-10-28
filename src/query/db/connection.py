"""PostgreSQL connection helpers for the read-only web application."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg_pool import ConnectionPool


def create_pool(
    dsn: str,
    *,
    min_size: int = 1,
    max_size: int = 5,
    readonly: bool = True,
) -> ConnectionPool:
    if not dsn:
        raise ValueError("DSN do PostgreSQL não configurado")
    kwargs = {}
    if readonly:
        kwargs["options"] = "-c default_transaction_read_only=on"
    pool = ConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        kwargs=kwargs,
        open=True,
    )
    return pool


@contextmanager
def connection(pool: ConnectionPool) -> Iterator[psycopg.Connection]:
    with pool.connection() as conn:
        yield conn
