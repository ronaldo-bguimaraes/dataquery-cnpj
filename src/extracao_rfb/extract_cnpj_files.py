"""CLI fina para disparar o download de uma competência CNPJ.

Uso::

    python -m src.extracao_rfb.extract_cnpj_files --competencia 2026-02 --output-dir /data/cnpj

Nenhum trabalho acontece no import: toda a I/O começa em :func:`main`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from src.download_utils.download_files import DEFAULT_CONCURRENCY
from src.extracao_rfb.download_competencia import (
    DownloadError,
    EmptyCompetenciaError,
    OutputDirectoryError,
    download_competencia,
)
from src.extracao_rfb.competencia import InvalidCompetenciaError
from src.extracao_rfb.rfb_client import ListingError

log = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_DOWNLOAD_FAILURES = 1
EXIT_CONFIG_OR_LISTING = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Baixa os arquivos CNPJ de uma competência AAAA-MM da Receita Federal."
    )
    parser.add_argument(
        "--competencia",
        required=True,
        help="Competência no formato AAAA-MM (mês de 01 a 12).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Diretório base de saída (default: variável CNPJ_DOWNLOAD_DIR).",
    )
    parser.add_argument(
        "--limite",
        type=int,
        default=None,
        help="Máximo de arquivos a baixar, em ordem lexicográfica (default: todos).",
    )
    parser.add_argument(
        "--concorrencia",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Número de downloads simultâneos (default: {DEFAULT_CONCURRENCY}).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)

    output_dir = args.output_dir or os.environ.get("CNPJ_DOWNLOAD_DIR")
    if not output_dir:
        print(
            "erro: informe --output-dir ou defina CNPJ_DOWNLOAD_DIR",
            file=sys.stderr,
        )
        return EXIT_CONFIG_OR_LISTING

    try:
        summary = download_competencia(
            competencia=args.competencia,
            output_dir=output_dir,
            limite=args.limite,
            concorrencia=args.concorrencia,
        )
    except DownloadError as exc:
        print(json.dumps(exc.summary.to_dict()))
        print(f"erro: {exc}", file=sys.stderr)
        return EXIT_DOWNLOAD_FAILURES
    except (
        InvalidCompetenciaError,
        OutputDirectoryError,
        EmptyCompetenciaError,
        ListingError,
        ValueError,
    ) as exc:
        print(f"erro: {exc}", file=sys.stderr)
        return EXIT_CONFIG_OR_LISTING

    print(json.dumps(summary.to_dict()))
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
