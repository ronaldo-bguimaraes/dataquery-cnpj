"""Resolve o diretório Bronze usado pelo loader de consulta.

No caminho padrão os Parquets vivem no RustFS (S3-compatível). O loader baixa a
competência para um diretório temporário e reaproveita as mesmas validações
locais do inventário. O backend filesystem continua disponível para testes e
execução offline, sem baixar nada.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from src.query.ingest.inventory import BRONZE_FILE_NAMES

_LOCAL_BACKENDS = {"", "filesystem", "local", "fs"}


def is_local_backend(backend: str | None) -> bool:
    """Return ``True`` when ``backend`` selects the local filesystem."""

    return (backend or "filesystem").strip().lower() in _LOCAL_BACKENDS


def download_bronze(
    competencia: str,
    destination: str | os.PathLike[str],
) -> Path:
    """Copy the 37 Bronze Parquets of ``competencia`` to ``destination``.

    Used by host-side tools (for example the bulk loader) that still need a
    local directory. The download is atomic per file: an interrupted transfer
    never leaves a partial Parquet visible.
    """

    from src.storage.factory import build_store_from_env

    store = build_store_from_env()
    target = Path(destination)
    target.mkdir(parents=True, exist_ok=True)
    prefix = f"bronze/{competencia}"
    for name in BRONZE_FILE_NAMES:
        local = target / name
        remote = store.head(f"{prefix}/{name}")
        if (
            remote is not None
            and remote.size > 0
            and local.is_file()
            and local.stat().st_size == remote.size
        ):
            continue
        store.get_to_path(f"{prefix}/{name}", local)
    return target


@contextmanager
def bronze_source_dir(
    competencia: str,
    *,
    local_root: str | os.PathLike[str],
    storage_backend: str | None = None,
    scratch_dir: str | os.PathLike[str] | None = None,
    source_dir: str | os.PathLike[str] | None = None,
) -> Iterator[Path]:
    """Yield a local directory containing the 37 Bronze Parquets.

    ``source_dir`` (CLI) always wins. With a filesystem backend the configured
    root is used directly. With ``s3``/``r2`` the objects under
    ``bronze/<competencia>/`` are downloaded into a temporary scratch directory
    that is removed afterwards.
    """

    if source_dir is not None:
        yield Path(source_dir)
        return

    if is_local_backend(storage_backend):
        yield Path(local_root) / "bronze" / competencia
        return

    configured_scratch = scratch_dir or os.getenv(
        "CNPJ_QUERY_SCRATCH_DIR", "/tmp/cnpj-query"
    )
    scratch_root = Path(configured_scratch)
    scratch_root.mkdir(parents=True, exist_ok=True)

    workdir = Path(
        tempfile.mkdtemp(prefix=f"bronze-{competencia}-", dir=scratch_root)
    )
    try:
        download_bronze(competencia, workdir)
        yield workdir
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    """Baixa uma competência Bronze do storage configurado para um diretório local."""

    parser = argparse.ArgumentParser(
        description="Copia os Parquets Bronze do storage S3-compatível para um diretório local."
    )
    parser.add_argument("--competencia", required=True, help="competência AAAA-MM")
    parser.add_argument(
        "--dest",
        required=True,
        type=Path,
        help="diretório local de destino (ex.: data/bronze/2026-10)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=os.getenv("CNPJ_QUERY_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        target = download_bronze(args.competencia, args.dest)
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        logging.getLogger(__name__).error("Download falhou: %s", exc)
        return 1
    print(target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
