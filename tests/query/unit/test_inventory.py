from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from src.query.ingest.inventory import InventoryError, discover_inventory
from tests.query.fixtures.parquets import rewrite_wrong_schema


def test_discovers_all_37_files(source_dir: Path):
    inventory = discover_inventory(source_dir)

    assert len(inventory.empresas) == 10
    assert len(inventory.estabelecimentos) == 10
    assert len(inventory.socios) == 10
    assert inventory.simples.name == "Simples.parquet"
    assert len(inventory.lookups) == 6


def test_missing_family_fails(source_dir: Path):
    for path in source_dir.glob("Empresas*.parquet"):
        path.unlink()

    with pytest.raises(InventoryError, match="Empresas"):
        discover_inventory(source_dir)


def test_duplicate_simples_fails(source_dir: Path):
    shutil.copy(source_dir / "Simples.parquet", source_dir / "Simples2.parquet")

    with pytest.raises(InventoryError, match="Simples"):
        discover_inventory(source_dir)


def test_schema_mismatch_fails(source_dir: Path):
    rewrite_wrong_schema(source_dir / "Empresas0.parquet")

    with pytest.raises(InventoryError, match="coluna|schema|Empresas"):
        discover_inventory(source_dir)


def test_empty_directory_fails(tmp_path: Path):
    with pytest.raises(InventoryError):
        discover_inventory(tmp_path)
