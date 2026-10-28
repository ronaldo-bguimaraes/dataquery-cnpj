"""Discovery and validation of the Bronze Parquet inventory."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from schema import schema


class InventoryError(ValueError):
    """Raised when the source inventory is incomplete or incompatible."""


@dataclass(frozen=True)
class Inventory:
    competencia: str
    empresas: list[Path]
    estabelecimentos: list[Path]
    socios: list[Path]
    simples: Path
    lookups: dict[str, Path]

    @property
    def all_files(self) -> list[Path]:
        return [
            *self.empresas,
            *self.estabelecimentos,
            *self.socios,
            self.simples,
            *self.lookups.values(),
        ]

    @property
    def total_files(self) -> int:
        return len(self.all_files)


LOOKUP_FILES = {
    "Cnaes": "cnaes",
    "Motivos": "motivos",
    "Municipios": "municipios",
    "Naturezas": "naturezas_juridicas",
    "Paises": "paises",
    "Qualificacoes": "qualificacoes_socios",
}


def _require_columns(path: Path, schema_name: str, count: int | None = None) -> None:
    expected = list(schema[schema_name]["columns"])
    names = list(pq.read_schema(path).names)
    if names != expected:
        raise InventoryError(
            f"schema divergente em {path.name}: esperado {expected}, encontrado {names}"
        )
    if count is not None and len(names) != count:
        raise InventoryError(f"schema divergente em {path.name}: {len(names)} colunas")


def _family(source_dir: Path, pattern: str, label: str, expected: int) -> list[Path]:
    files = sorted(source_dir.glob(pattern))
    if len(files) != expected:
        raise InventoryError(
            f"família {label} deve ter {expected} arquivo(s); encontrados {len(files)}"
        )
    return files


def discover_inventory(source_dir: str | Path, competencia: str = "2026-10") -> Inventory:
    """Validate the 37 expected files and return their paths."""

    source = Path(source_dir)
    if not source.is_dir():
        raise InventoryError(f"diretório de origem não existe: {source}")

    empresas = _family(source, "Empresas*.parquet", "Empresas", 10)
    estabelecimentos = _family(
        source, "Estabelecimentos*.parquet", "Estabelecimentos", 10
    )
    socios = _family(source, "Socios*.parquet", "Socios", 10)

    simples_files = sorted(source.glob("Simples*.parquet"))
    if len(simples_files) != 1 or simples_files[0].name != "Simples.parquet":
        raise InventoryError(
            "família Simples deve ter exatamente Simples.parquet; "
            f"encontrados {[path.name for path in simples_files]}"
        )

    lookups: dict[str, Path] = {}
    for stem, schema_name in LOOKUP_FILES.items():
        path = source / f"{stem}.parquet"
        if not path.is_file():
            raise InventoryError(f"arquivo de apoio ausente: {path.name}")
        lookups[stem] = path
        _require_columns(path, schema_name)

    for path in empresas:
        _require_columns(path, "empresas")
    for path in estabelecimentos:
        _require_columns(path, "estabelecimentos")
    for path in socios:
        _require_columns(path, "socios")
    _require_columns(simples_files[0], "simples")

    return Inventory(
        competencia=competencia,
        empresas=empresas,
        estabelecimentos=estabelecimentos,
        socios=socios,
        simples=simples_files[0],
        lookups=lookups,
    )
