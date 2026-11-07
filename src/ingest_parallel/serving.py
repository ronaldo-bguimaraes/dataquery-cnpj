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


def _rename_constraint(connection: psycopg.Connection, table: str, *, from_name: str, to_name: str) -> None:
    """Renomeia constraint se existir (nomes globais sobrevivem a RENAME TABLE)."""

    connection.execute(
        f"""
        DO $$
        BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE c.conname = '{from_name}' AND t.relname = '{table}'
        ) THEN
            ALTER TABLE {_q(table)} RENAME CONSTRAINT {_q(from_name)} TO {_q(to_name)};
        END IF;
        END $$;
        """
    )


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
        previous_publication = _previous_publication_id(connection, counts.load_id)
        for table in SERVING_TABLES:
            staging = staging_table_name(table, counts.load_id)
            pk_col = "cnpj_completo" if table == "estabelecimentos" else "cnpj_basico"
            # Retenção da versão anterior para rollback (FR-047): a base atual é ARCHIVED
            # sob o nome do load que ela CONTÉM (a publicação anterior não sobrescrita).
            archive_name = f"archived_{table}_{previous_publication or counts.load_id}"
            archive = _q(archive_name)
            connection.execute(f"DROP TABLE IF EXISTS {archive} CASCADE")
            connection.execute(f"ALTER TABLE {_q(table)} RENAME TO {archive}")
            # Renomeia a constraint junto com o archive (nomes são globais no Postgres)
            _rename_constraint(
                connection, archive_name, from_name=f"pk_{table}", to_name=f"{archive_name}_pk"
            )
            connection.execute(
                f"ALTER TABLE {_q(staging)} ADD CONSTRAINT {_q('pk_' + table)} PRIMARY KEY ({_q(pk_col)})"
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
                status = 'published', published_at = EXCLUDED.published_at,
                previous_active = EXCLUDED.previous_active
            """,
            (counts.load_id, competencia, counts.load_id, _previous_publication_id(connection, counts.load_id)),
        )


def _previous_publication_id(connection: psycopg.Connection, current_load_id: str) -> str | None:
    """Versão anterior publicada (para rollback, FR-047), por data de publicação."""

    row = connection.execute(
        "SELECT load_id FROM serving_publication"
        " WHERE publication_id <> %s AND status = 'published'"
        " ORDER BY published_at DESC LIMIT 1",
        (current_load_id,),
    ).fetchone()
    return row[0] if row else None


def _prune_old_archives(connection: psycopg.Connection, table: str, keep_load_id: str) -> None:
    """Mantém apenas a geração anterior arquivada (política de retenção, FR-045).

    CUIDADO: não remover/archive do load em processo nem a geração anterior
    (o archive recém-reescrito carrega o nome da publicação anterior). Retenção
    completa (idade e expiração) fica na US4/T046.
    """

    prefix = f"archived_{table}_%"
    rows = connection.execute(
        "SELECT tablename FROM pg_tables"
        " WHERE schemaname = 'public' AND tablename LIKE %s AND tablename <> %s",
        (prefix, f"archived_{table}_{keep_load_id}"),
    ).fetchall()
    for (old_archive,) in rows:
        connection.execute(f"DROP TABLE IF EXISTS {_q(old_archive)} CASCADE")


def _previous_active(connection: psycopg.Connection, competencia: str) -> str | None:
    row = connection.execute(
        "SELECT competencia FROM query_metadata WHERE ativo ORDER BY competencia LIMIT 1"
    ).fetchone()
    if row and row[0] != competencia:
        return row[0]
    return None


def rollback_serving(dsn: str, publication_id: str) -> dict[str, Any]:
    """Restaura a versão arquivada da publicação anterior (FR-045, SC-011).

    Passos: previous_active → para cada tabela, base atual vira archive da versão
    cancelada; archive da versão anterior retorna para a base; status anterior vira
    `published` e o atual `rolled_back` — tudo em uma transação (contrato §5).
    """

    try:
        with psycopg.connect(dsn) as connection:
            ensure_run_tables(connection)
            pub = connection.execute(
                "SELECT previous_active, status FROM serving_publication WHERE publication_id = %s",
                (publication_id,),
            ).fetchone()
            if not pub:
                raise ServingAbort(f"publicação inexistente: {publication_id}")
            previous_active, atual_status = pub
            if previous_active is None:
                raise ServingAbort("não há versão anterior registrada para rollback")

            with connection.transaction():
                connection.execute("SET LOCAL lock_timeout = '2s'")
                for table in SERVING_TABLES:
                    base = _q(table)
                    archive_cancelada_name = f"archived_{table}_{publication_id}"
                    archive_cancelada = _q(archive_cancelada_name)
                    archive_anterior = _q(f"archived_{table}_{previous_active}")
                    archive_anterior_name = f"archived_{table}_{previous_active}"
                    connection.execute(f"DROP TABLE IF EXISTS {archive_cancelada} CASCADE")
                    # lugar para a base atual sem colidir
                    connection.execute(
                        f"ALTER TABLE IF EXISTS {base} RENAME TO {archive_cancelada}"
                    )
                    _rename_constraint(connection, archive_cancelada_name, from_name=f"pk_{table}", to_name=f"{archive_cancelada_name}_pk")
                    connection.execute(f"ALTER TABLE IF EXISTS {archive_anterior} RENAME TO {base}")
                    _rename_constraint(connection, table, from_name=f"{archive_anterior_name}_pk", to_name=f"pk_{table}")

                connection.execute(
                    "UPDATE serving_publication SET status = 'published', published_at = now()"
                    " WHERE publication_id = %s",
                    (previous_active,),
                )
                connection.execute(
                    "UPDATE serving_publication SET status = 'rolled_back' WHERE publication_id = %s",
                    (publication_id,),
                )
        return {"rolled_back": True, "publication_id": publication_id, "restored": previous_active}
    except ServingAbort:
        raise
    except psycopg.Error as exc:
        raise ServingAbort(f"rollback falhou: {type(exc).__name__}") from exc


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
