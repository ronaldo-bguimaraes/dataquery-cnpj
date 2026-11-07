"""PostgreSQL schema and bulk loading for the query portal."""

from __future__ import annotations

import logging
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator

import psycopg

from src.query.config import validate_competencia
from src.query.ingest.inventory import Inventory, discover_inventory
from src.query.ingest.transform import (
    TransformStats,
    empresas_rows,
    estabelecimentos_rows,
    lookup_rows,
    simples_rows,
    socios_rows,
)

log = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db" / "schema.sql"

EMPRESA_COLUMNS = [
    "cnpj_basico",
    "razao_social",
    "razao_social_normalizada",
    "natureza_juridica_codigo",
    "qualificacao_responsavel_codigo",
    "capital_social",
    "porte_codigo",
    "ente_federativo",
]

ESTABELECIMENTO_COLUMNS = [
    "cnpj_completo",
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais_codigo",
    "data_inicio_atividade",
    "cnae_principal",
    "cnae_secundaria_texto",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio_codigo",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "numero_fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

SOCIO_COLUMNS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio",
    "documento_socio_mascarado",
    "qualificacao_socio_codigo",
    "data_entrada_sociedade",
    "pais_codigo",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante_legal",
    "faixa_etaria",
]

SIMPLES_COLUMNS = [
    "cnpj_basico",
    "opcao_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
    "optante_simples",
    "optante_mei",
]

LOOKUPS = {
    "Cnaes": ("cnaes", "cnae_codigo", "cnae_descricao"),
    "Municipios": ("municipios", "municipio_codigo", "municipio_descricao"),
    "Naturezas": (
        "naturezas_juridicas",
        "natureza_juridica_codigo",
        "natureza_juridica_descricao",
    ),
    "Qualificacoes": (
        "qualificacoes_socios",
        "qualificacao_socio_codigo",
        "qualificacao_socio_descricao",
    ),
    "Motivos": ("motivos", "motivo_codigo", "motivo_descricao"),
    "Paises": ("paises", "pais_codigo", "pais_descricao"),
}


class LoadError(RuntimeError):
    """Raised when a load cannot be published."""


def apply_schema(connection: psycopg.Connection) -> None:
    connection.execute(SCHEMA_PATH.read_text())
    connection.commit()


def _copy_text(value: Any) -> str:
    """Encode one value for PostgreSQL COPY text format."""

    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    text = str(value)
    # PostgreSQL text não aceita NUL (0x00); dados RFB contêm NUL em campos livres.
    text = text.replace("\x00", "")
    return (
        text.replace("\\", "\\\\")
        .replace("\t", r"\t")
        .replace("\n", r"\n")
        .replace("\r", r"\r")
        .replace("\b", r"\b")
        .replace("\f", r"\f")
        .replace("\v", r"\v")
    )


def _copy_line(values: Iterable[Any]) -> str:
    return "\t".join(_copy_text(value) for value in values) + "\n"


def _copy_rows(
    connection: psycopg.Connection,
    table: str,
    columns: list[str],
    rows: Iterable[dict[str, Any]],
) -> int:
    count = 0
    sql = f"COPY {table} ({', '.join(columns)}) FROM STDIN"
    with connection.cursor() as cursor:
        with cursor.copy(sql) as copy:
            for row in rows:
                copy.write(_copy_line(row.get(column) for column in columns))
                count += 1
    return count


def _copy_estabelecimentos(
    connection: psycopg.Connection,
    inventory: Inventory,
    stats: TransformStats,
) -> int:
    count = 0
    base_sql = (
        "COPY estabelecimentos ("
        + ", ".join(ESTABELECIMENTO_COLUMNS)
        + ") FROM STDIN"
    )
    cnae_sql = "COPY estabelecimento_cnaes (cnpj_completo, cnae_codigo, principal) FROM STDIN"
    with connection.cursor() as base_cursor:
        with base_cursor.copy(base_sql) as base_copy:
            for row, _cnaes in estabelecimentos_rows(inventory.estabelecimentos, stats):
                base_copy.write(
                    _copy_line(row.get(column) for column in ESTABELECIMENTO_COLUMNS)
                )
                count += 1
    with connection.cursor() as cnae_cursor:
        with cnae_cursor.copy(cnae_sql) as cnae_copy:
            for _row, cnaes in estabelecimentos_rows(inventory.estabelecimentos, stats):
                for cnpj, code, principal in cnaes:
                    cnae_copy.write(_copy_line([cnpj, code, principal]))
    return count


def _truncate(connection: psycopg.Connection) -> None:
    connection.execute(
        """
        TRUNCATE TABLE
            estabelecimentos_listagem,
            estabelecimento_cnaes,
            socios,
            simples,
            estabelecimentos,
            empresas,
            municipios_por_uf,
            cnaes_presentes,
            query_metadata,
            cnaes,
            municipios,
            naturezas_juridicas,
            qualificacoes_socios,
            motivos,
            paises
        RESTART IDENTITY CASCADE
        """
    )


def _build_derived(connection: psycopg.Connection, competencia: str, fonte: str, counts: dict[str, int]) -> None:
    connection.execute(
        """
        INSERT INTO municipios_por_uf (uf, municipio_codigo, municipio_descricao)
        SELECT DISTINCT
            e.uf,
            e.municipio_codigo,
            COALESCE(m.descricao, e.municipio_codigo)
        FROM estabelecimentos e
        LEFT JOIN municipios m ON m.codigo = e.municipio_codigo
        WHERE e.uf IS NOT NULL AND e.uf <> ''
          AND e.municipio_codigo IS NOT NULL AND e.municipio_codigo <> ''
        """
    )
    connection.execute(
        """
        INSERT INTO cnaes_presentes (cnae_codigo, descricao)
        SELECT DISTINCT
            ec.cnae_codigo,
            COALESCE(c.descricao, ec.cnae_codigo)
        FROM estabelecimento_cnaes ec
        LEFT JOIN cnaes c ON c.codigo = ec.cnae_codigo
        """
    )
    connection.execute(
        """
        INSERT INTO estabelecimentos_listagem (
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
        FROM estabelecimentos e
        JOIN empresas emp ON emp.cnpj_basico = e.cnpj_basico
        LEFT JOIN situacoes_cadastrais sc ON sc.codigo = lpad(e.situacao_cadastral, 2, '0')
        LEFT JOIN municipios m ON m.codigo = e.municipio_codigo
        LEFT JOIN cnaes c ON c.codigo = e.cnae_principal
        LEFT JOIN matriz_filial mf ON mf.codigo = e.identificador_matriz_filial
        LEFT JOIN portes p ON p.codigo = lpad(emp.porte_codigo, 2, '0')
        LEFT JOIN simples s ON s.cnpj_basico = e.cnpj_basico
        """
    )
    connection.execute(
        """
        INSERT INTO query_metadata (
            competencia, fonte, carregado_em, total_empresas,
            total_estabelecimentos, total_socios, ativo
        ) VALUES (%s, %s, now(), %s, %s, %s, true)
        ON CONFLICT (competencia) DO UPDATE SET
            fonte = EXCLUDED.fonte,
            carregado_em = EXCLUDED.carregado_em,
            total_empresas = EXCLUDED.total_empresas,
            total_estabelecimentos = EXCLUDED.total_estabelecimentos,
            total_socios = EXCLUDED.total_socios,
            ativo = true
        """,
        (
            competencia,
            fonte,
            counts["empresas"],
            counts["estabelecimentos"],
            counts["socios"],
        ),
    )
    connection.execute(
        "UPDATE query_metadata SET ativo = false WHERE competencia <> %s",
        (competencia,),
    )


def _validate(connection: psycopg.Connection, counts: dict[str, int]) -> None:
    actual = {
        "empresas": connection.execute("SELECT count(*) FROM empresas").fetchone()[0],
        "estabelecimentos": connection.execute(
            "SELECT count(*) FROM estabelecimentos"
        ).fetchone()[0],
        "socios": connection.execute("SELECT count(*) FROM socios").fetchone()[0],
        "listagem": connection.execute(
            "SELECT count(*) FROM estabelecimentos_listagem"
        ).fetchone()[0],
    }
    if actual["empresas"] != counts["empresas"]:
        raise LoadError(
            f"contagem de empresas divergente: {actual['empresas']} != {counts['empresas']}"
        )
    if actual["estabelecimentos"] != counts["estabelecimentos"]:
        raise LoadError(
            "contagem de estabelecimentos divergente: "
            f"{actual['estabelecimentos']} != {counts['estabelecimentos']}"
        )
    if actual["socios"] != counts["socios"]:
        raise LoadError(
            f"contagem de sócios divergente: {actual['socios']} != {counts['socios']}"
        )
    if actual["listagem"] != counts["estabelecimentos"]:
        raise LoadError(
            "listagem divergente: "
            f"{actual['listagem']} != {counts['estabelecimentos']}"
        )


def load_inventory(
    source_dir: str | Path, dsn: str, *, competencia: str = "2026-10", fonte: str = "Receita Federal"
) -> dict[str, Any]:
    """Load one competence atomically and return a small summary."""

    competencia = validate_competencia(competencia)
    inventory = discover_inventory(source_dir, competencia)
    started = time.monotonic()
    stats = TransformStats()
    counts = {"empresas": 0, "estabelecimentos": 0, "socios": 0, "simples": 0}

    try:
        with psycopg.connect(dsn) as connection:
            apply_schema(connection)
            try:
                _truncate(connection)
                for stem, (table, code_col, description_col) in LOOKUPS.items():
                    _copy_rows(
                        connection,
                        table,
                        ["codigo", "descricao"],
                        (
                            {"codigo": code, "descricao": description}
                            for code, description in lookup_rows(
                                inventory.lookups[stem], code_col, description_col
                            )
                        ),
                    )

                log.info("Carregando empresas")
                counts["empresas"] = _copy_rows(
                    connection,
                    "empresas",
                    EMPRESA_COLUMNS,
                    empresas_rows(inventory.empresas, stats),
                )
                log.info("Empresas carregadas: %s", counts["empresas"])
                log.info("Carregando estabelecimentos e CNAEs")
                counts["estabelecimentos"] = _copy_estabelecimentos(
                    connection, inventory, stats
                )
                log.info("Estabelecimentos carregados: %s", counts["estabelecimentos"])
                log.info("Carregando sócios")
                counts["socios"] = _copy_rows(
                    connection,
                    "socios",
                    SOCIO_COLUMNS,
                    socios_rows(inventory.socios, stats),
                )
                log.info("Sócios carregados: %s", counts["socios"])
                counts["simples"] = _copy_rows(
                    connection,
                    "simples",
                    SIMPLES_COLUMNS,
                    simples_rows(inventory.simples, competencia, stats),
                )
                _build_derived(connection, competencia, fonte, counts)
                _validate(connection, counts)
                connection.execute(
                    "ANALYZE empresas; ANALYZE estabelecimentos; "
                    "ANALYZE estabelecimento_cnaes; ANALYZE socios; "
                    "ANALYZE simples; ANALYZE estabelecimentos_listagem"
                )
                connection.commit()
            except Exception as exc:
                connection.rollback()
                if isinstance(exc, LoadError):
                    raise
                raise LoadError(f"falha ao carregar competência {competencia}: {type(exc).__name__}") from exc
    except LoadError:
        raise
    except Exception as exc:
        raise LoadError(f"falha ao publicar competência {competencia}: {type(exc).__name__}") from exc

    duration = time.monotonic() - started
    summary = {
        "competencia": competencia,
        "fonte": fonte,
        **counts,
        "descartes": stats.as_dict(),
        "duracao_segundos": round(duration, 3),
    }
    log.info("Carga concluída: %s", summary)
    return summary
