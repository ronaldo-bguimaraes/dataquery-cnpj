"""Carga completa do CNPJ (223M) em staging paralela + swap atômico.

Uso:
    python -m src.ingest_parallel.bulk --dsn postgresql://... --competencia 2026-10 --workers 8

Fluxo:
1. cria tabelas `stg_*` UNLOGGED (LIKE base, sem índices/constraints);
2. processa cada arquivo Bronze em um processo (multiprocessing), COPY para staging;
3. constrói PKs/índices e a listagem derivada em staging (base antiga segue legível);
4. valida contagens e troca todas as tabelas em UMA transação;
5. atualiza query_metadata e roda ANALYZE fora da transação de troca.

Não é o caminho final do Iceberg Silver (feature 003), mas usa as mesmas garantias
de staging + validação + swap atômico do serving (FR-039/040) para uma carga full.
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

import psycopg

from src.query.ingest.inventory import Inventory, discover_inventory
from src.query.ingest.postgres import (
    EMPRESA_COLUMNS,
    ESTABELECIMENTO_COLUMNS,
    SIMPLES_COLUMNS,
    SOCIO_COLUMNS,
    _copy_line,
)
from src.query.ingest.transform import (
    TransformStats,
    empresas_rows,
    estabelecimentos_rows,
    lookup_rows,
    simples_rows,
    socios_rows,
)

log = logging.getLogger("ingest_parallel.bulk")

STG = {
    "empresas": "stg_empresas",
    "estabelecimentos": "stg_estabelecimentos",
    "estabelecimento_cnaes": "stg_estabelecimento_cnaes",
    "socios": "stg_socios",
    "simples": "stg_simples",
    "estabelecimentos_listagem": "stg_estabelecimentos_listagem",
}

LOOKUP_TABLES = ("cnaes", "municipios", "naturezas_juridicas", "qualificacoes_socios", "motivos", "paises")
LOOKUP_SOURCE = {
    "Cnaes": ("cnaes", "cnae_codigo", "cnae_descricao"),
    "Municipios": ("municipios", "municipio_codigo", "municipio_descricao"),
    "Naturezas": ("naturezas_juridicas", "natureza_juridica_codigo", "natureza_juridica_descricao"),
    "Qualificacoes": ("qualificacoes_socios", "qualificacao_socio_codigo", "qualificacao_socio_descricao"),
    "Motivos": ("motivos", "motivo_codigo", "motivo_descricao"),
    "Paises": ("paises", "pais_codigo", "pais_descricao"),
}


# ---------------------------------------------------------------- worker (picklable)

def _copy_base_and_cnaes(conn, task: dict) -> tuple[int, int]:
    """Uma partição de estabelecimentos: COPY base + CNAEs (2 passes)."""

    path = Path(task["path"])
    limit = task.get("limit_rows") or 0
    row_groups = {str(path): task["row_groups"]} if task.get("row_groups") else None
    stats = TransformStats()
    count = 0
    with conn.cursor() as cursor:
        with cursor.copy(
            f"COPY {STG['estabelecimentos']} ({', '.join(ESTABELECIMENTO_COLUMNS)}) FROM STDIN"
        ) as copy:
            for row, _cnaes in estabelecimentos_rows([path], stats, row_groups):
                if limit and count >= limit:
                    break
                copy.write(_copy_line(row.get(c) for c in ESTABELECIMENTO_COLUMNS))
                count += 1
    cnaes_count = 0
    with conn.cursor() as cursor:
        with cursor.copy(
            f"COPY {STG['estabelecimento_cnaes']} (cnpj_completo, cnae_codigo, principal) FROM STDIN"
        ) as copy:
            for _row, cnaes in estabelecimentos_rows([path], stats, row_groups):
                if limit and cnaes_count >= limit:
                    break
                for cnpj, code, principal in cnaes:
                    copy.write(_copy_line([cnpj, code, principal]))
                    cnaes_count += 1

    return count, cnaes_count


def _load_task(task: dict) -> dict:
    """Executa uma partição (1 arquivo) em processo próprio."""

    table = task["table"]
    path = Path(task["path"])
    limit = task.get("limit_rows") or 0
    row_groups = {str(path): task["row_groups"]} if task.get("row_groups") else None
    stats = TransformStats()
    started = time.monotonic()
    rows_written = 0
    cnaes_written = 0
    try:
        with psycopg.connect(task["dsn"]) as conn:
            if table == "estabelecimentos":
                rows_written, cnaes_written = _copy_base_and_cnaes(conn, task)
            else:
                if table == "empresas":
                    columns, generator = EMPRESA_COLUMNS, empresas_rows([path], stats, row_groups)
                elif table == "socios":
                    columns, generator = SOCIO_COLUMNS, socios_rows([path], stats, row_groups)
                elif table == "simples":
                    columns, generator = SIMPLES_COLUMNS, simples_rows(path, task["competencia"], stats, row_groups)
                else:
                    raise ValueError(f"tabela sem loader: {table}")
                with conn.cursor() as cursor:
                    with cursor.copy(
                        f"COPY {STG[table]} ({', '.join(columns)}) FROM STDIN"
                    ) as copy:
                        for row in generator:
                            if limit and rows_written >= limit:
                                break
                            copy.write(_copy_line(row.get(c) for c in columns))
                            rows_written += 1
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        return {
            "table": table,
            "file": path.name,
            "error": f"{type(exc).__name__}: {exc}",
            "seconds": round(time.monotonic() - started, 1),
        }
    return {
        "table": table,
        "file": path.name,
        "rows": rows_written,
        "cnaes": cnaes_written,
        "rejects": asdict(stats),
        "seconds": round(time.monotonic() - started, 1),
    }


# ------------------------------------------------------------------ staging setup

def create_staging(connection: psycopg.Connection, limit_rows: int = 0) -> None:
    """Cria staging UNLOGGED sem constraints, clonando colunas/defaults da base."""

    for base, staging in [
        ("empresas", STG["empresas"]),
        ("estabelecimentos", STG["estabelecimentos"]),
        ("estabelecimento_cnaes", STG["estabelecimento_cnaes"]),
        ("socios", STG["socios"]),
        ("simples", STG["simples"]),
        ("estabelecimentos_listagem", STG["estabelecimentos_listagem"]),
        *[(t, f"stg_{t}") for t in LOOKUP_TABLES],
    ]:
        connection.execute(f"DROP TABLE IF EXISTS {staging} CASCADE")
        connection.execute(
            f"CREATE UNLOGGED TABLE {staging} (LIKE {base} INCLUDING DEFAULTS)"
        )
    connection.commit()


def load_lookups(connection: psycopg.Connection, inventory: Inventory) -> dict[str, int]:
    """Lookups são pequenos: carga sequencial no processo principal."""

    counts: dict[str, int] = {}
    for stem, (table, code_col, description_col) in LOOKUP_SOURCE.items():
        staging = f"stg_{table}"
        rows = list(lookup_rows(inventory.lookups[stem], code_col, description_col))
        with connection.cursor() as cursor:
            with cursor.copy(f"COPY {staging} (codigo, descricao) FROM STDIN") as copy:
                for code, description in rows:
                    copy.write(_copy_line([code, description]))
        counts[table] = len(rows)
    connection.commit()
    return counts


# ------------------------------------------------------------------ pre-swap build

DERIVED_LISTAGEM_SQL = """
INSERT INTO stg_estabelecimentos_listagem (
    cnpj_completo, cnpj_basico, razao_social, razao_social_normalizada,
    nome_fantasia, situacao_codigo, situacao_descricao, uf,
    municipio_codigo, municipio_descricao, cnae_principal_codigo,
    cnae_principal_descricao, identificador_matriz_filial,
    matriz_filial_descricao, data_inicio_atividade, porte_codigo,
    porte_descricao, optante_simples, optante_mei
)
SELECT
    e.cnpj_completo,
    e.cnpj_basico,
    emp.razao_social,
    emp.razao_social_normalizada,
    e.nome_fantasia,
    lpad(e.situacao_cadastral, 2, '0'),
    COALESCE(sc.descricao, e.situacao_cadastral),
    e.uf,
    e.municipio_codigo,
    COALESCE(m.descricao, e.municipio_codigo),
    e.cnae_principal,
    COALESCE(c.descricao, e.cnae_principal),
    e.identificador_matriz_filial,
    COALESCE(mf.descricao, e.identificador_matriz_filial),
    e.data_inicio_atividade,
    lpad(emp.porte_codigo, 2, '0'),
    COALESCE(p.descricao, emp.porte_codigo),
    COALESCE(s.optante_simples, false),
    COALESCE(s.optante_mei, false)
FROM stg_estabelecimentos e
JOIN stg_empresas emp ON emp.cnpj_basico = e.cnpj_basico
LEFT JOIN situacoes_cadastrais sc ON sc.codigo = lpad(e.situacao_cadastral, 2, '0')
LEFT JOIN municipios m ON m.codigo = e.municipio_codigo
LEFT JOIN cnaes c ON c.codigo = e.cnae_principal
LEFT JOIN matriz_filial mf ON mf.codigo = e.identificador_matriz_filial
LEFT JOIN portes p ON p.codigo = lpad(emp.porte_codigo, 2, '0')
LEFT JOIN stg_simples s ON s.cnpj_basico = e.cnpj_basico
"""


def build_indexes(connection: psycopg.Connection) -> None:
    """PKs/índices na staging (a base antiga continua servindo leituras)."""

    started = time.monotonic()
    statements = [
        f"ALTER TABLE {STG['empresas']} ADD PRIMARY KEY (cnpj_basico)",
        f"ALTER TABLE {STG['estabelecimentos']} ADD PRIMARY KEY (cnpj_completo)",
        f"ALTER TABLE {STG['estabelecimento_cnaes']} ADD PRIMARY KEY (id)",
        f"CREATE INDEX ON {STG['estabelecimento_cnaes']} (cnpj_completo, cnae_codigo)",
        f"ALTER TABLE {STG['socios']} ADD PRIMARY KEY (id)",
        f"CREATE INDEX ON {STG['socios']} (cnpj_basico)",
        f"ALTER TABLE {STG['simples']} ADD PRIMARY KEY (cnpj_basico)",
        "ALTER TABLE stg_cnaes ADD PRIMARY KEY (codigo)",
        "ALTER TABLE stg_municipios ADD PRIMARY KEY (codigo)",
        "ALTER TABLE stg_naturezas_juridicas ADD PRIMARY KEY (codigo)",
        "ALTER TABLE stg_qualificacoes_socios ADD PRIMARY KEY (codigo)",
        "ALTER TABLE stg_motivos ADD PRIMARY KEY (codigo)",
        "ALTER TABLE stg_paises ADD PRIMARY KEY (codigo)",
        f"CREATE INDEX ON {STG['estabelecimento_cnaes']} (cnae_codigo)",
    ]
    for statement in statements:
        connection.execute(statement)
        connection.commit()
    log.info("índices de staging prontos em %.1fs", time.monotonic() - started)


def build_derived(connection: psycopg.Connection) -> None:
    started = time.monotonic()
    log.info("construindo listagem derivada (73M)...")
    connection.execute(DERIVED_LISTAGEM_SQL)
    connection.commit()
    connection.execute(
        f"ALTER TABLE {STG['estabelecimentos_listagem']} ADD PRIMARY KEY (cnpj_completo)"
    )
    connection.commit()
    for column in ("uf, municipio_codigo", "situacao_codigo", "porte_codigo", "data_inicio_atividade", "cnpj_basico"):
        connection.execute(
            f"CREATE INDEX ON {STG['estabelecimentos_listagem']} ({column})"
        )
        connection.commit()
    log.info("listagem derivada pronta em %.1fs", time.monotonic() - started)


# --------------------------------------------------------------------- validation

def staging_ready(connection: psycopg.Connection, expected: dict[str, int]) -> bool:
    """True quando a staging já contém a carga completa (permite retomar sem recarregar)."""

    for table, total in expected.items():
        staging = STG[table] if table in STG else f"stg_{table}"
        exists = connection.execute(
            "SELECT to_regclass(%s) IS NOT NULL", (staging,)
        ).fetchone()[0]
        if not exists:
            return False
        count = connection.execute(f"SELECT count(*) FROM {staging}").fetchone()[0]
        if count < total * 0.99:
            log.warning("staging incompleta: %s=%s de %s", staging, count, total)
            return False
    return True


def dedupe_staging(connection: psycopg.Connection) -> dict[str, int]:
    """Aplica `keep_last` das business keys antes de criar as PKs (FR-003/004).

    A fonte RFB tem duplicatas pontuais (ex.: 1 `cnpj_basico` repetido em 2026-10);
    o contrato manda deduplicar, não falhar.
    """

    specs = [
        ("stg_empresas", ("cnpj_basico",)),
        ("stg_estabelecimentos", ("cnpj_completo",)),
        ("stg_simples", ("cnpj_basico",)),
        (
            "stg_socios",
            (
                "cnpj_basico",
                "identificador_socio",
                "documento_socio_mascarado",
                "nome_socio",
                "qualificacao_socio_codigo",
                "data_entrada_sociedade",
            ),
        ),
    ]
    removed: dict[str, int] = {}
    for table, keys in specs:
        started = time.monotonic()
        condition = " AND ".join(f"a.{key} = b.{key}" for key in keys)
        before = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        connection.execute(
            f"DELETE FROM {table} a USING {table} b WHERE a.ctid < b.ctid AND {condition}"
        )
        connection.commit()
        after = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        removed[table] = before - after
        log.info("dedupe %-22s removidas %8d em %.1fs", table, before - after, time.monotonic() - started)
    return removed


def validate_counts(connection: psycopg.Connection, expected: dict[str, int]) -> dict[str, int]:
    actual: dict[str, int] = {}
    for key in ("empresas", "estabelecimentos", "socios", "simples"):
        staging = STG[key]
        actual[key] = connection.execute(f"SELECT count(*) FROM {staging}").fetchone()[0]
        assert actual[key] > 0, f"staging vazio para {key}"
        assert actual[key] <= expected[key], f"{key}: {actual[key]} > esperado {expected[key]}"
        if actual[key] < expected[key] * 0.99:
            raise AssertionError(f"{key}: perda inesperada de linhas ({actual[key]} de {expected[key]})")
    return actual


# ------------------------------------------------------------------------- swap

SWAP_ORDER = [
    "estabelecimentos_listagem",
    "estabelecimento_cnaes",
    "socios",
    "simples",
    "estabelecimentos",
    "empresas",
]
LOOKUP_SWAP = [(t, f"stg_{t}") for t in LOOKUP_TABLES]
SWAP_TARGETS = {
    "empresas": STG["empresas"],
    "estabelecimentos": STG["estabelecimentos"],
    "estabelecimento_cnaes": STG["estabelecimento_cnaes"],
    "socios": STG["socios"],
    "simples": STG["simples"],
    "estabelecimentos_listagem": STG["estabelecimentos_listagem"],
}


def swap(connection: psycopg.Connection, competencia: str, fonte: str, counts: dict[str, int]) -> None:
    """Troca atômica de todas as tabelas em uma transação (FR-010/040)."""

    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout = '30s'")
        for table in SWAP_ORDER:
            connection.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        for table in SWAP_ORDER:
            connection.execute(f"ALTER TABLE {SWAP_TARGETS[table]} RENAME TO {table}")
        for base, staging in LOOKUP_SWAP:
            connection.execute(f"DROP TABLE IF EXISTS {base} CASCADE")
            connection.execute(f"ALTER TABLE {staging} RENAME TO {base}")

        # sequência própria para socios.id após o swap
        connection.execute("CREATE SEQUENCE IF NOT EXISTS socios_id_seq2")
        connection.execute(
            "ALTER TABLE socios ALTER COLUMN id SET DEFAULT nextval('socios_id_seq2')"
        )
        connection.execute(
            "SELECT setval('socios_id_seq2', COALESCE((SELECT max(id) FROM socios), 1), true)"
        )
        connection.execute("ALTER SEQUENCE socios_id_seq2 OWNED BY socios.id")

        # idem para estabelecimento_cnaes.id (bigserial vindo da staging)
        connection.execute("CREATE SEQUENCE IF NOT EXISTS estabelecimento_cnaes_id_seq2")
        connection.execute(
            "ALTER TABLE estabelecimento_cnaes ALTER COLUMN id"
            " SET DEFAULT nextval('estabelecimento_cnaes_id_seq2')"
        )
        connection.execute(
            "SELECT setval('estabelecimento_cnaes_id_seq2',"
            " COALESCE((SELECT max(id) FROM estabelecimento_cnaes), 1), true)"
        )
        connection.execute(
            "ALTER SEQUENCE estabelecimento_cnaes_id_seq2 OWNED BY estabelecimento_cnaes.id"
        )

        connection.execute(
            """
            INSERT INTO query_metadata (
                competencia, fonte, carregado_em, total_empresas,
                total_estabelecimentos, total_socios, ativo
            ) VALUES (%s, %s, now(), %s, %s, %s, true)
            ON CONFLICT (competencia) DO UPDATE SET
                fonte = EXCLUDED.fonte, carregado_em = EXCLUDED.carregado_em,
                total_empresas = EXCLUDED.total_empresas,
                total_estabelecimentos = EXCLUDED.total_estabelecimentos,
                total_socios = EXCLUDED.total_socios,
                ativo = true
            """,
            (competencia, fonte, counts["empresas"], counts["estabelecimentos"], counts["socios"]),
        )
        connection.execute("UPDATE query_metadata SET ativo = false WHERE competencia <> %s", (competencia,))


def derived_small(connection: psycopg.Connection) -> None:
    connection.execute(
        """
        INSERT INTO municipios_por_uf (uf, municipio_codigo, municipio_descricao)
        SELECT DISTINCT e.uf, e.municipio_codigo, COALESCE(m.descricao, e.municipio_codigo)
        FROM estabelecimentos e
        LEFT JOIN municipios m ON m.codigo = e.municipio_codigo
        WHERE e.uf IS NOT NULL AND e.uf <> '' AND e.municipio_codigo IS NOT NULL
        ON CONFLICT DO NOTHING
        """
    )
    connection.execute(
        """
        INSERT INTO cnaes_presentes (cnae_codigo, descricao)
        SELECT DISTINCT ec.cnae_codigo, COALESCE(c.descricao, ec.cnae_codigo)
        FROM estabelecimento_cnaes ec
        LEFT JOIN cnaes c ON c.codigo = ec.cnae_codigo
        ON CONFLICT DO NOTHING
        """
    )
    # Contagens por dimensões de baixa cardinalidade: a listagem usa SUM nesta tabela
    # em vez de count(*) sobre 73,8M linhas.
    connection.execute("TRUNCATE estabelecimentos_filtros_stats")
    connection.execute(
        """
        INSERT INTO estabelecimentos_filtros_stats (
            uf, situacao_codigo, porte_codigo, matriz_filial,
            optante_simples, optante_mei, total
        )
        SELECT uf, situacao_codigo, porte_codigo, identificador_matriz_filial,
               optante_simples, optante_mei, count(*)
        FROM estabelecimentos_listagem
        GROUP BY 1, 2, 3, 4, 5, 6
        """
    )
    # Contagens por CNAE × UF (CNAE sozinho tem ~1M de linhas por código)
    connection.execute("TRUNCATE estabelecimentos_cnae_uf_stats")
    connection.execute(
        """
        INSERT INTO estabelecimentos_cnae_uf_stats (cnae_codigo, uf, total)
        SELECT ec.cnae_codigo, e.uf, count(*)
        FROM estabelecimento_cnaes ec
        JOIN estabelecimentos_listagem e USING (cnpj_completo)
        WHERE e.uf IS NOT NULL AND e.uf <> ''
        GROUP BY 1, 2
        """
    )
    connection.commit()


# --------------------------------------------------------------------------- main

def run_bulk(
    dsn: str,
    bronze_dir: str | Path,
    competencia: str = "2026-10",
    *,
    workers: int = 8,
    limit_rows: int = 0,
    fonte: str = "Receita Federal",
    reuse_staging: bool = False,
) -> dict:
    started = time.monotonic()
    inventory = discover_inventory(bronze_dir, competencia)
    expected = {
        "empresas": sum(_count(path) for path in inventory.empresas),
        "estabelecimentos": sum(_count(path) for path in inventory.estabelecimentos),
        "socios": sum(_count(path) for path in inventory.socios),
        "simples": _count(inventory.simples),
    }
    log.info("inventário: %s", json.dumps(expected))

    # 1. staging + lookups (ou reuso de staging já carregada)
    reused = False
    with psycopg.connect(dsn) as connection:
        if reuse_staging and staging_ready(connection, expected):
            reused = True
            log.info("reutilizando staging existente (carga completa detectada)")
        else:
            create_staging(connection)
            load_lookups(connection, inventory)

    total_rows = 0
    if not reused:
        # 2. tasks por arquivo/row-group (arquivos grandes fatiados p/ paralelismo real)
        tasks: list[dict] = []
        for table, paths in (
            ("empresas", inventory.empresas),
            ("estabelecimentos", inventory.estabelecimentos),
            ("socios", inventory.socios),
            ("simples", [inventory.simples]),
        ):
            for path in paths:
                for row_groups, expected_slice in _slices(path):
                    tasks.append(
                        {
                            "table": table,
                            "path": str(path),
                            "dsn": dsn,
                            "competencia": competencia,
                            "limit_rows": limit_rows,
                            "row_groups": row_groups,
                            "expected": expected_slice,
                        }
                    )

        # maiores fatias primeiro: evita gargalo no fim
        tasks.sort(key=lambda task: task["expected"], reverse=True)

        # 3. carga paralela
        errors: list[dict] = []
        with mp.Pool(processes=workers) as pool:
            for result in pool.imap_unordered(_load_task, tasks):
                if "error" in result:
                    errors.append(result)
                    log.error("FALHA %s/%s: %s", result["table"], result["file"], result["error"])
                    continue
                total_rows += result["rows"]
                log.info(
                    "ok %-16s %-28s %10d linhas (%d cnaes) %6.1fs",
                    result["table"],
                    result["file"],
                    result["rows"],
                    result.get("cnaes", 0),
                    result["seconds"],
                )
        if errors:
            raise RuntimeError(f"{len(errors)} partição(ões) falharam: {errors[:3]}")

    # 4. dedupe + índices + derivados + validação + swap
    with psycopg.connect(dsn) as connection:
        duplicates = dedupe_staging(connection)
        build_indexes(connection)
        build_derived(connection)
        counts = validate_counts(connection, expected)
        log.info("contagens: %s", counts)
        swap(connection, competencia, fonte, counts)
        derived_small(connection)
        connection.commit()

    # 5. ANALYZE fora da transação de swap
    with psycopg.connect(dsn, autocommit=True) as connection:
        for table in (
            "empresas",
            "estabelecimentos",
            "estabelecimento_cnaes",
            "socios",
            "simples",
            "estabelecimentos_listagem",
        ):
            log.info("ANALYZE %s", table)
            connection.execute(f"ANALYZE {table}")

    summary = {
        "competencia": competencia,
        "workers": workers,
        "reused_staging": reused,
        "linhas_carregadas": total_rows,
        "duplicatas_removidas": duplicates,
        "esperado": expected,
        "contagens": counts,
        "duracao_segundos": round(time.monotonic() - started, 1),
    }
    log.info("concluído: %s", json.dumps(summary))
    return summary


def _count(path: Path) -> int:
    import pyarrow.parquet as pq

    return pq.read_metadata(path).num_rows


def _slices(path: Path, chunk_rows: int = 3_000_000) -> list[tuple[list[int], int]]:
    """Divide um Parquet em fatias de row-groups (paralelismo sem duplicar/omitir)."""

    import pyarrow.parquet as pq

    metadata = pq.ParquetFile(path).metadata
    slices: list[tuple[list[int], int]] = []
    current: list[int] = []
    accumulated = 0
    for index in range(metadata.num_row_groups):
        current.append(index)
        accumulated += metadata.row_group(index).num_rows
        if accumulated >= chunk_rows:
            slices.append((current, accumulated))
            current, accumulated = [], 0
    if current:
        slices.append((current, accumulated))
    return slices or [([], 0)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="src.ingest_parallel.bulk")
    parser.add_argument("--dsn", default=os.environ.get("CNPJ_INGEST_SERVING_DSN", ""))
    parser.add_argument("--bronze-dir", default="data/bronze/2026-10")
    parser.add_argument("--competencia", default="2026-10")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument(
        "--reuse-staging",
        action="store_true",
        help="reaproveita staging completa (retomada sem recarregar 223M)",
    )
    args = parser.parse_args(argv)
    if not args.dsn:
        parser.error("--dsn ou CNPJ_INGEST_SERVING_DSN é obrigatório")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    summary = run_bulk(
        args.dsn,
        args.bronze_dir,
        args.competencia,
        workers=args.workers,
        limit_rows=args.limit_rows,
        reuse_staging=args.reuse_staging,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
