"""Coordenação de execução por partição (fase mínima da US1 — FR-008/009/012, T023).

Somente o estado mínimo de lote: um lote só é marcado como `succeeded` após o efeito
durável (staging + swap); falhas nunca marcam lote concluído. Checkpoints completos
(lease/retomada) são o escopo da US2 (T027+).
"""

from __future__ import annotations

from typing import Any

import psycopg


class OrchestrationError(RuntimeError):
    """Estado inconsistente entre lote e dado durável."""


def ensure_partition_record(
    connection: psycopg.Connection,
    load_id: str,
    partition_id: str,
    table_name: str,
    *,
    status: str = "pending",
) -> None:
    connection.execute(
        """
        INSERT INTO load_partition (load_id, partition_id, table_name, status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (load_id, partition_id) DO NOTHING
        """,
        (load_id, partition_id, table_name, status),
    )


def mark_partition_succeeded(
    connection: psycopg.Connection,
    load_id: str,
    partition_id: str,
) -> None:
    """Só chamada APÓS o commit durável do lote (FR-008)."""

    connection.execute(
        """
        UPDATE load_partition
        SET status = 'succeeded', completed_at = now()
        WHERE load_id = %s AND partition_id = %s
        """,
        (load_id, partition_id),
    )


def mark_partition_failed(
    connection: psycopg.Connection,
    load_id: str,
    partition_id: str,
) -> None:
    connection.execute(
        """
        UPDATE load_partition
        SET status = 'failed', retry_count = retry_count + 1
        WHERE load_id = %s AND partition_id = %s
        """,
        (load_id, partition_id),
    )


def partitions_status(connection: psycopg.Connection, load_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT partition_id, table_name, status FROM load_partition WHERE load_id = %s ORDER BY partition_id",
        (load_id,),
    ).fetchall()
    return [
        {"partition_id": row[0], "table_name": row[1], "status": row[2]}
        for row in rows
    ]
