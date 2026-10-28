"""CLI entrypoint for loading one competence into the query database."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from src.query.config import ConfigError, QueryConfig, validate_competencia
from src.query.ingest.inventory import InventoryError
from src.query.ingest.postgres import LoadError, load_inventory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Carrega os Parquets Bronze de uma competência no PostgreSQL de consulta."
    )
    parser.add_argument("--source-dir", type=Path, help="diretório com os Parquets da competência")
    parser.add_argument("--competencia", help="competência AAAA-MM (default: ambiente)")
    parser.add_argument("--dsn", help="DSN PostgreSQL de carga (default: CNPJ_QUERY_DATABASE_URL)")
    parser.add_argument("--fonte", default=os.getenv("CNPJ_QUERY_SOURCE", "Receita Federal"))
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("CNPJ_QUERY_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = build_parser().parse_args(argv)
    try:
        config = QueryConfig.from_env(require_database=False)
        competencia = validate_competencia(args.competencia or config.competencia)
        dsn = args.dsn or config.database_url
        if not dsn:
            raise ConfigError("DSN não informado; defina CNPJ_QUERY_DATABASE_URL ou use --dsn")
        source_dir = args.source_dir or config.source_dir
        summary = load_inventory(
            source_dir,
            dsn,
            competencia=competencia,
            fonte=args.fonte or config.fonte,
        )
    except (ConfigError, InventoryError, LoadError) as exc:
        logging.getLogger(__name__).error("Carga falhou: %s", exc)
        return 1
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
