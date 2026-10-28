from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from src.query.ingest.inventory import InventoryError
from src.query.ingest.load import load_inventory
from src.query.ingest.postgres import LoadError
from tests.query.fixtures.parquets import rewrite_wrong_schema


def test_missing_family_does_not_publish(query_db, source_dir: Path, query_dsn: str):
    for path in source_dir.glob("Empresas*.parquet"):
        path.unlink()

    with pytest.raises(InventoryError, match="Empresas"):
        load_inventory(source_dir, query_dsn, competencia="2026-10")

    assert query_db.execute("SELECT count(*) FROM query_metadata").fetchone()[0] == 0


def test_schema_mismatch_does_not_publish(query_db, source_dir: Path, query_dsn: str):
    rewrite_wrong_schema(source_dir / "Empresas0.parquet")

    with pytest.raises(InventoryError, match="coluna|schema|Empresas"):
        load_inventory(source_dir, query_dsn, competencia="2026-10")

    assert query_db.execute("SELECT count(*) FROM query_metadata").fetchone()[0] == 0


def test_failed_reload_preserves_previous_publication(
    query_db, source_dir: Path, query_dsn: str
):
    load_inventory(source_dir, query_dsn, competencia="2026-10")
    shutil.copy(source_dir / "Empresas0.parquet", source_dir / "Empresas1.parquet")

    with pytest.raises(LoadError):
        load_inventory(source_dir, query_dsn, competencia="2026-10")

    assert query_db.execute("SELECT count(*) FROM empresas").fetchone()[0] == 3
    metadata = query_db.execute(
        "SELECT total_empresas, total_estabelecimentos FROM query_metadata WHERE ativo"
    ).fetchone()
    assert metadata == (3, 4)
