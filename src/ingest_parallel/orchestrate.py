"""Coordenação de execução por partição (fase mínima da US1 — FR-008/009/012, T023).

Somente o estado mínimo de lote: um lote só é marcado como `succeeded` após o efeito
durável (staging + swap); falhas nunca marcam lote concluído. Checkpoints completos
(lease/retomada) são o escopo da US2 (T027+).
"""

from __future__ import annotations

from typing import Any

import psycopg

from src.ingest_parallel.serving import ensure_run_tables


def main(argv: list[str] | None = None) -> int:
    """CLI de retomada: `python -m src.ingest_parallel.orchestrate --resume <load_id>`."""

    import json
    import os

    import argparse

    parser = argparse.ArgumentParser(prog="src.ingest_parallel.orchestrate")
    parser.add_argument("--resume", required=False, help="load_id para retomar")
    args = parser.parse_args(argv)
    if not args.resume:
        parser.error("--resume é obrigatório")
    dsn = os.environ.get(
        "CNPJ_INGEST_SERVING_DSN",
        "postgresql://cnpj_query_loader:change-me@127.0.0.1:55432/cnpj_query_test",
    )
    resumed: list[str] = []
    with psycopg.connect(dsn) as connection:
        ensure_run_tables(connection)
        resumed = resume_incomplete(
            connection, args.resume, task=lambda pid, table: resumed.append(pid)
        )
    print(
        json.dumps(
            {"load_id": args.resume, "retomadas": resumed},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


class OrchestrationError(RuntimeError):
    """Estado inconsistente entre lote e dado durável."""


class ConcurrencyGuardError(RuntimeError):
    """Corrida de competência detectada; `code` = RUN_ALREADY_ACTIVE (FR-016/017)."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


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


def claim_partition(
    connection: psycopg.Connection,
    load_id: str,
    partition_id: str,
    worker_id: str,
    *,
    lease_seconds: int = 900,
) -> bool:
    """Claim atômico de propriedade (FR-022): só quem fizer UPDATE ganha o lease.

    Só é elegível quando o lote não está concluído e o lease anterior expirou —
    nunca há duas escritas concorrentes para a mesma partição.
    """

    result = connection.execute(
        f"""
        UPDATE load_partition
        SET status = 'running', worker_id = %s,
            owner_lease = now() + interval '{lease_seconds} seconds'
        WHERE load_id = %s AND partition_id = %s
          AND status IN ('pending', 'failed', 'running')
          AND (worker_id IS NULL OR worker_id = %s
               OR owner_lease IS NULL OR owner_lease < now())
        RETURNING worker_id
        """,
        (worker_id, load_id, partition_id, worker_id),
    ).fetchone()
    return result is not None


def release_partition(
    connection: psycopg.Connection,
    load_id: str,
    partition_id: str,
    worker_id: str,
) -> None:
    """Libera o lease sem concluir (worker morre / aborta)."""

    connection.execute(
        """
        UPDATE load_partition
        SET status = 'pending', worker_id = NULL, owner_lease = NULL
        WHERE load_id = %s AND partition_id = %s AND worker_id = %s
        """,
        (load_id, partition_id, worker_id),
    )


def resume_incomplete(connection: psycopg.Connection, load_id: str, task) -> list[str]:  # noqa: ANN001
    """Retomada segura (FR-020/021): roda `task` apenas em partições não concluídas.

    `task(partition_id, table_name)` precisa ser idempotente; concluídas são puladas.
    """

    rows = connection.execute(
        """
        SELECT partition_id, table_name, status, worker_id, owner_lease
        FROM load_partition
        WHERE load_id = %s
          AND NOT (status = 'succeeded')
          AND (owner_lease IS NULL OR owner_lease < now())
        ORDER BY partition_id
        """,
        (load_id,),
    ).fetchall()
    executed: list[str] = []
    for partition_id, table_name, status, worker_id, _lease in rows:
        if status == "running" and worker_id:
            # lease de outro worker ativo: não retomar (FR-022)
            continue
        try:
            task(partition_id, table_name)
        except Exception:
            mark_partition_failed(connection, load_id, partition_id)
            raise
        mark_partition_succeeded(connection, load_id, partition_id)
        executed.append(partition_id)
    return executed


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


def acquire_competencia_guard(
    connection: psycopg.Connection,
    run_id: str,
    competencia: str,
) -> None:
    """Guard de corrida (FR-016/017): índice único parcial em status='running'.

    Levanta `ConcurrencyGuardError("RUN_ALREADY_ACTIVE")` quando outra execução
    já publica a competência — falha observável em vez de interleaving.
    """

    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ingestion_run_running_uniq"
        " ON ingestion_run (competencia) WHERE status = 'running'"
    )
    try:
        connection.execute(
            """
            INSERT INTO ingestion_run (run_id, load_id, competencia, status, error_summary)
            VALUES (%s, %s, %s, 'running', %s)
            """,
            (run_id, run_id, competencia, None),
        )
        connection.commit()
    except psycopg.errors.UniqueViolation:
        connection.rollback()
        raise ConcurrencyGuardError("RUN_ALREADY_ACTIVE", f"competência {competencia} já tem execução ativa") from None


def release_competencia_guard(connection: psycopg.Connection, run_id: str) -> None:
    """Fecha a execução ativa, liberando a competência (com o resultado em status)."""

    connection.execute(
        "UPDATE ingestion_run SET status = 'succeeded' WHERE run_id = %s AND status = 'running'",
        (run_id,),
    )
    connection.commit()


def run_parallel(partitions: list[dict], worker, *, max_workers: int = 8, on_error=None) -> dict:  # noqa: ANN001
    """Executa partições em paralelo respeitando o teto de concorrência (FR-023).

    Sem memória compartilhada entre tarefas (FR-025): cada task recebe seu payload.
    `on_error(partition_id)` coleta falhas observadas e não interrompe as outras tasks
    (US3 aceite 3). Retorna contagem de sucesso/erro.
    """

    import concurrent.futures

    executadas: list[str] = []
    falhas: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for partition in partitions:
            future = pool.submit(worker, partition)
            futures[future] = partition["partition_id"]
        for future, partition_id in futures.items():
            try:
                future.result()
                executadas.append(partition_id)
            except Exception:
                falhas.append(partition_id)
                if on_error is not None:
                    on_error(partition_id)
    return {"executadas": executadas, "falhas": falhas}


def partitions_status(connection: psycopg.Connection, load_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT partition_id, table_name, status FROM load_partition WHERE load_id = %s ORDER BY partition_id",
        (load_id,),
    ).fetchall()
    return [
        {"partition_id": row[0], "table_name": row[1], "status": row[2]}
        for row in rows
    ]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

