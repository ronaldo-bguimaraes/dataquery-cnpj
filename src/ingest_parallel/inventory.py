"""Manifesto da fonte: inventário, hashes e identidade da carga (FR-001, F R-014).

Contrato: contracts/source_manifest.md.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from src.ingest_parallel.config import derive_load_id
from src.ingest_parallel.config import code_version as _code_version
from src.query.config import validate_competencia

EXPECTED_SOURCE_COUNT_RFB = 37


class ManifestError(ValueError):
    """Manifesto fora do contrato; `code` segue contracts/source_manifest.md §4."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


@dataclass(frozen=True)
class SourceFileEntry:
    """Um arquivo de origem do inventário (contrato §2)."""

    file_name: str
    storage_path: str
    size_bytes: int
    sha256: str
    row_count: int
    source_url: str = ""


@dataclass(frozen=True)
class SourceManifest:
    """Identidade completa da fonte de uma competência (contrato §1)."""

    competencia: str
    data_version_id: str
    files: list[SourceFileEntry]
    code_version: str
    source_count: int = 0
    total_rows_expected: int = 0

    @property
    def load_id(self) -> str:
        return derive_load_id(self.competencia, self.data_version_id)


def _hash_file(path: Path, chunk: int = 1024 * 1024) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while block := handle.read(chunk):
            digest.update(block)
            size += len(block)
    return size, digest.hexdigest()


def build_source_manifest(
    source_dir: str | Path,
    competencia: str,
    *,
    code_version_id: str | None = None,
) -> SourceManifest:
    """Varre o Bronze, calcula hashes e monta o manifesto versionado."""

    try:
        validate_competencia(competencia)
    except ValueError as exc:
        raise ManifestError("COMPETENCIA_INVALID", str(exc)) from exc

    directory = Path(source_dir)
    if not directory.is_dir():
        raise ManifestError("SOURCE_FILE_MISSING", f"diretório ausente: {directory}")

    files: list[SourceFileEntry] = []
    for path in sorted(directory.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        size, digest = _hash_file(path)
        row_count = 0
        if path.suffix == ".parquet":
            metadata = pq.read_metadata(path)
            row_count = metadata.num_rows
        files.append(
            SourceFileEntry(
                file_name=path.name,
                storage_path=str(path),
                size_bytes=size,
                sha256=digest,
                row_count=row_count,
            )
        )
    if not files:
        raise ManifestError("SOURCE_COUNT_EMPTY", f"manifesto sem arquivos: {directory}")

    # data_version_id: SHA-256 dos hashes concatenados, em ordem estável pelo nome.
    concatenated = "".join(entry.sha256 for entry in files).encode()
    data_version_id = hashlib.sha256(concatenated).hexdigest()

    return SourceManifest(
        competencia=competencia,
        data_version_id=data_version_id,
        files=files,
        code_version=code_version_id or _code_version(),
        source_count=len(files),
        total_rows_expected=sum(entry.row_count for entry in files),
    )


def validate_bronze_inventory(
    manifest: SourceManifest,
    storage_dir: str | Path,
    *,
    valido: int | None = EXPECTED_SOURCE_COUNT_RFB,
) -> list[Path]:
    """Valida o manifesto contra o storage e devolve os caminhos na ordem do manifest.

    Regras (contrato §3):
    - `valido=37` é o contrato RFB; divergência impede a carga (SOURCE_COUNT_DIVERGENT);
      passe `valido=None` para inventários de teste que não seguem o contrato RFB.
    - Cada arquivo deva existir (SOURCE_FILE_MISSING) e o hash bater (SOURCE_HASH_MISMATCH).
    """

    directory = Path(storage_dir)
    if not directory.is_dir():
        raise ManifestError("SOURCE_FILE_MISSING", f"diretório ausente: {directory}")

    paths: list[Path] = []
    for entry in manifest.files:
        path = directory / entry.file_name
        if not path.is_file():
            raise ManifestError("SOURCE_FILE_MISSING", f"arquivo ausente: {entry.file_name}")
        _size, digest = _hash_file(path)
        if digest != entry.sha256:
            raise ManifestError(
                "SOURCE_HASH_MISMATCH",
                f"conflito de hash SHA-256: {entry.file_name}",
            )
        paths.append(path)

    if valido is not None and manifest.source_count != valido:
        raise ManifestError(
            "SOURCE_COUNT_DIVERGENT",
            f"source_count deve ser {valido}; encontrado {manifest.source_count}",
        )
    return paths
