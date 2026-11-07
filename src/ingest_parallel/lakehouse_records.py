"""Registro idempotente de partições Silver (FR-013/015/032, T030).

Substitui a deduplicação por snapshot Iceberg da branch enquanto o catálogo REST
não está provisionado: `registered`/`skipped`/`replaced` por conteúdo.
"""

from __future__ import annotations

import psycopg

REGISTRY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS silver_partition_registry (
    load_id text NOT NULL,
    partition_id text NOT NULL,
    content_hash text NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (load_id, partition_id)
);
"""


def register_partition(
    connection: psycopg.Connection,
    load_id: str,
    partition_id: str,
    content_hash: str,
) -> str:
    """Retorna `registered`, `skipped` (mesmo conteúdo) ou `replaced` (novo conteúdo)."""

    connection.execute(REGISTRY_TABLE_SQL)
    current = connection.execute(
        "SELECT content_hash FROM silver_partition_registry WHERE load_id = %s AND partition_id = %s",
        (load_id, partition_id),
    ).fetchone()
    if current is None:
        connection.execute(
            "INSERT INTO silver_partition_registry (load_id, partition_id, content_hash) VALUES (%s, %s, %s)",
            (load_id, partition_id, content_hash),
        )
        return "registered"
    if current[0] == content_hash:
        return "skipped"
    connection.execute(
        "UPDATE silver_partition_registry SET content_hash = %s, registered_at = now()"
        " WHERE load_id = %s AND partition_id = %s",
        (content_hash, load_id, partition_id),
    )
    return "replaced"
