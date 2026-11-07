"""Serving Postgres: staging UNLOGGED por load_id + COPY + swap atômico (FR-039/040).

Contrato: contracts/serving_publication.md. Dados só chegam ao serving depois de
validados contra o dataset canônico; a troca é administrada sempre na MESMA transação.
"""

from __future__ import annotations

from typing import Any

import psycopg

from src.ingest_parallel.publish import staging_table_name
from src.ingest_parallel.quality import DatasetCounts, QualityError, run_quality_checks

SERVING_TABLES = ("empresas", "estabelecimentos", "socios", "simples")

#: Columns mínimos por tabela de staging nas integrações atuais (contrato §3.1).
CORE_COLUMNS = {
    "empresas": ["cnpj_basico", "razao_social", "competencia", "load_id"],
    "estabelecimentos": ["cnpj_completo", "cnpj_basico", "competencia", "load_id"],
    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "documento_socio_mascarado",
        "nome_socio",
        "qualificacao_socio_codigo",
        "data_entrada_sociedade",
        "competencia",
        "load_id",
    ],
    "simples": ["cnpj_basico", "competencia", "load_id"],
}


class ServingAbort(RuntimeError):
    """Qualquer impedimento de publicação (qualidade, contagem, swap)."""


RUN_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_run (
    run_id text PRIMARY KEY,
    load_id text NOT NULL,
    competencia text NOT NULL,
    status text NOT NULL,
    code_version text,
    error_summary text,
    started_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS load_partition (
    load_id text NOT NULL,
    partition_id text NOT NULL,
    table_name text NOT NULL,
    status text NOT NULL,
    worker_id text,
    owner_lease timestamptz,
    retry_count int NOT NULL DEFAULT 0,
    completed_at timestamptz,
    PRIMARY KEY (load_id, partition_id)
);
CREATE TABLE IF NOT EXISTS serving_publication (
    publication_id text PRIMARY KEY,
    competencia text NOT NULL,
    load_id text NOT NULL,
    previous_active text,
    status text NOT NULL,
    published_at timestamptz
);
"""


def _q(identifier: str) -> str:
    """Identificadores do Postgres (nomes de staging incluem hífens)."""

    return '"' + identifier.replace('"', '""') + '"'


def ensure_run_tables(connection: psycopg.Connection) -> None:
    """Cria as tabelas de estado da execução (idempotente)."""

    connection.execute(RUN_TABLES_SQL)
    # worker_id pode não existir em esquemas anteriores; adiciona idempotentemente.
    existing = {
        row[0]
        for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'load_partition'"
        ).fetchall()
    }
    if "worker_id" not in existing:
        connection.execute("ALTER TABLE load_partition ADD COLUMN worker_id text")


def create_staging(connection: psycopg.Connection, load_id: str) -> list[str]:
    """Cria staging UNLOGGED por tabela, a partir do esquema core (contrato §3.1)."""

    created: list[str] = []
    for table in SERVING_TABLES:
        columns = ", ".join(f"{_q(c)} text" for c in CORE_COLUMNS[table])
        name = _q(staging_table_name(table, load_id))
        connection.execute(
            f"CREATE UNLOGGED TABLE IF NOT EXISTS {name} ({columns})"
        )
        created.append(name)
    return created


def _drop_staging(connection: psycopg.Connection, load_id: str) -> None:
    for table in SERVING_TABLES:
        connection.execute(f"DROP TABLE IF EXISTS {_q(staging_table_name(table, load_id))} CASCADE")


def stage_rows(
    connection: psycopg.Connection,
    load_id: str,
    table: str,
    rows: list[dict[str, Any]],
) -> int:
    """COPY das linhas para a staging da tabela (só staging; nada publicado)."""

    columns = CORE_COLUMNS[table]
    target = _q(staging_table_name(table, load_id))
    count = 0
    with connection.cursor() as cursor:
        with cursor.copy(f"COPY {target} ({', '.join(_q(c) for c in columns)}) FROM STDIN") as copy:
            for row in rows:
                copy.write("\t".join("" if row.get(c) is None else str(row.get(c)) for c in columns) + "\n")
                count += 1
    return count


def _swap(connection: psycopg.Connection, counts: DatasetCounts, competencia: str, fonte: str) -> None:
    """A troca atômica: staging → base em 1 transação com lock timeout + metadados (FR-046)."""

    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout = '2s'")
        for table in SERVING_TABLES:
            staging = staging_table_name(table, counts.load_id)
            pk_col = "cnpj_completo" if table == "estabelecimentos" else "cnpj_basico"
            pk_name = staging + "_pk"
            # A base antiga leva o nome da constraint (RENAME não renomeia constraint:
            # droppar a base antes de criar o novo PK evita colisão de nomes globais).
            connection.execute(f"DROP TABLE IF EXISTS {_q(table)} CASCADE")
            connection.execute(
                f"ALTER TABLE {_q(staging)} ADD CONSTRAINT {_q(pk_name)} PRIMARY KEY ({_q(pk_col)})"
            )
            connection.execute(f"ALTER TABLE {_q(staging)} RENAME TO {_q(table)}")

        connection.execute("UPDATE query_metadata SET ativo = false WHERE ativo")
        connection.execute(
            """
            INSERT INTO query_metadata (
                competencia, fonte, carregado_em, total_empresas,
                total_estabelecimentos, total_socios, ativo
            )
            VALUES (%s, %s, now(), %s, %s, %s, true)
            ON CONFLICT (competencia) DO UPDATE SET
                fonte = EXCLUDED.fonte,
                carregado_em = EXCLUDED.carregado_em,
                total_empresas = EXCLUDED.total_empresas,
                total_estabelecimentos = EXCLUDED.total_estabelecimentos,
                total_socios = EXCLUDED.total_socios,
                ativo = true
            """,
            (competencia, fonte, len(counts.empresas), len(counts.estabelecimentos), len(counts.socios)),
        )
        connection.execute(
            """
            INSERT INTO serving_publication (
                publication_id, competencia, load_id, previous_active, status, published_at
            )
            VALUES (%s, %s, %s, %s, 'published', now())
            ON CONFLICT (publication_id) DO UPDATE SET
                status = 'published', published_at = EXCLUDED.published_at
            """,
            (counts.load_id, competencia, counts.load_id, _previous_active(connection, competencia)),
        )


def _previous_active(connection: psycopg.Connection, competencia: str) -> str | None:
    row = connection.execute(
        "SELECT competencia FROM query_metadata WHERE ativo ORDER BY competencia LIMIT 1"
    ).fetchone()
    if row and row[0] != competencia:
        return row[0]
    return None


def publish_serving(  # noqa: PLR0915
    dsn: str,
    counts: DatasetCounts,
    *,
    competencia: str,
    fonte: str = "Receita Federal",
) -> dict[str, Any]:
    """Valida e publica staging de forma atômica; falha mantém versão anterior (FR-012)."""

    try:
        with psycopg.connect(dsn) as connection:
            ensure_run_tables(connection)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS load_partition_tabela_idx ON load_partition (table_name)"
            )

            _drop_staging(connection, counts.load_id)
            create_staging(connection, counts.load_id)
            for table in SERVING_TABLES:
                stage_rows(connection, counts.load_id, table, getattr(counts, table))

            try:
                run_quality_checks(counts, total_esperado={t: len(getattr(counts, t)) for t in SERVING_TABLES})
            except QualityError as exc:
                raise ServingAbort(f"qualidade insuficiente: {exc.code}") from exc

            try:
                _swap(connection, counts, competencia, fonte)
            except psycopg.OperationalError as exc:
                raise ServingAbort("swap excedeu lock_timeout; versão anterior mantida") from exc

            connection.execute(
                "SELECT count(*) FROM query_metadata WHERE ativo"
            ).fetchone()
        return {
            "published": True,
            "competencia": competencia,
            "load_id": counts.load_id,
            "counts": {t: len(getattr(counts, t)) for t in SERVING_TABLES},
        }
    except psycopg.Error as exc:
        raise ServingAbort(f"falha de banco durante publicação: {type(exc).__name__}") from exc
