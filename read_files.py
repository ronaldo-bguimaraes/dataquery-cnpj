"""Legacy directory converter exposed as an explicit, side-effect-free API.

Importing this module only defines functions and classes. Conversion starts only
when :meth:`Process.convert_all`, :func:`convert_zip` or the CLI is called.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import os
from pathlib import Path
import time

from conversor import convert_compressed_csv_to_parquet
from read_schema import test_regex
from utils import get_available_memory

DEFAULT_CHUNK_SIZE = 16 * 1024 * 1024
MAX_CHUNK_SIZE = 64 * 1024 * 1024


def _default_chunk_size() -> int:
    try:
        available = int(get_available_memory() * 0.05)
    except Exception:  # noqa: BLE001 - safe fallback
        return DEFAULT_CHUNK_SIZE
    return max(DEFAULT_CHUNK_SIZE, min(available, MAX_CHUNK_SIZE))


def convert_zip(
    filepath: str | os.PathLike[str],
    output_file: str | os.PathLike[str],
    columns_names: list[str] | None = None,
    chunk_size: int | None = None,
) -> dict:
    """Convert the single CSV inside ``filepath`` into ``output_file``."""

    source = Path(filepath)
    if columns_names is None:
        columns_names = test_regex(source.name).get("columns") or []
    if not columns_names:
        raise LookupError(f"schema sem colunas para {source.name}")
    size = _default_chunk_size() if chunk_size is None else chunk_size
    return convert_compressed_csv_to_parquet(
        str(source), columns_names, size, output_file
    )


class Process:
    def __init__(self, input_dir: str | os.PathLike[str], target_dir: str | os.PathLike[str]):
        self._input_dir = Path(input_dir)
        self._target_dir = Path(target_dir)

    def columns_empresas(self):
        return [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte",
            "ente_federativo",
        ]

    def convert_file(self, file: Path) -> dict:
        dados = test_regex(file.name)
        etname = dados.get("name", "")
        columns = dados.get("columns", [])
        count = self._counter[etname]

        output_path = Path(self._target_dir, f"{etname}_{count}.parquet").resolve()
        print(f"convertendo arquivo {file.name} para parquet")
        started = time.perf_counter()
        result = convert_compressed_csv_to_parquet(
            str(file),
            columns,
            _default_chunk_size(),
            output_path,
        )
        duration = time.perf_counter() - started
        self._counter[etname] += 1
        print(f"arquivo {output_path} gerado com sucesso em {duration:.2f}s")
        return {
            "nome": file.name,
            "output": str(output_path),
            "duracao_segundos": duration,
            **(result or {}),
        }

    def convert_all(self) -> list[dict]:
        os.makedirs(self._target_dir, exist_ok=True)
        self._counter = defaultdict(int)
        results = []
        for file in sorted(self._input_dir.iterdir()):
            if file.is_file():
                results.append(self.convert_file(file))
        return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Converte ZIPs CNPJ em Parquets usando o schema central."
    )
    parser.add_argument("input_dir", help="diretório com os ZIPs de entrada")
    parser.add_argument("target_dir", help="diretório de saída dos Parquets")
    args = parser.parse_args(argv)

    results = Process(args.input_dir, args.target_dir).convert_all()
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())
