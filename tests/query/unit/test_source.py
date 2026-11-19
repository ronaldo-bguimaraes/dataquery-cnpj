from __future__ import annotations

import pytest

from src.query.ingest.inventory import BRONZE_FILE_NAMES
from src.query.ingest.source import bronze_source_dir, download_bronze, is_local_backend
from src.storage.protocol import StorageOperationError
from tests.fakes import FakeObjectStore


def _bronze_objects(competencia: str = "2026-10") -> dict[str, bytes]:
    return {
        f"bronze/{competencia}/{name}": b"parquet-" + name.encode()
        for name in BRONZE_FILE_NAMES
    }


def test_local_backend_points_at_configured_root(tmp_path):
    with bronze_source_dir(
        "2026-10", local_root=tmp_path, storage_backend="filesystem"
    ) as source:
        assert source == tmp_path / "bronze" / "2026-10"


def test_explicit_source_dir_wins_over_backend(tmp_path):
    explicit = tmp_path / "explicit"

    with bronze_source_dir(
        "2026-10",
        local_root=tmp_path,
        storage_backend="s3",
        source_dir=explicit,
    ) as source:
        assert source == explicit


@pytest.mark.parametrize("backend", ["", "filesystem", "local", "fs", " FILESYSTEM "])
def test_local_backend_detection(backend):
    assert is_local_backend(backend) is True


@pytest.mark.parametrize("backend", ["s3", "r2", "R2"])
def test_object_storage_backend_detection(backend):
    assert is_local_backend(backend) is False


def test_s3_backend_downloads_all_files_and_cleans_scratch(tmp_path, monkeypatch):
    store = FakeObjectStore(_bronze_objects())
    monkeypatch.setattr(
        "src.storage.factory.build_store_from_env", lambda: store
    )

    with bronze_source_dir(
        "2026-10",
        local_root=tmp_path / "local",
        storage_backend="s3",
        scratch_dir=tmp_path / "scratch",
    ) as source:
        assert source.parent == tmp_path / "scratch"
        assert sorted(path.name for path in source.iterdir()) == sorted(
            BRONZE_FILE_NAMES
        )
        assert (source / "Cnaes.parquet").read_bytes() == b"parquet-Cnaes.parquet"

    assert not source.exists()
    assert store.get_calls == [
        f"bronze/2026-10/{name}" for name in BRONZE_FILE_NAMES
    ]


def test_missing_object_fails_and_removes_scratch(tmp_path, monkeypatch):
    store = FakeObjectStore(_bronze_objects())
    store.objects.pop("bronze/2026-10/Simples.parquet")
    monkeypatch.setattr(
        "src.storage.factory.build_store_from_env", lambda: store
    )

    with pytest.raises(StorageOperationError):
        with bronze_source_dir(
            "2026-10",
            local_root=tmp_path / "local",
            storage_backend="s3",
            scratch_dir=tmp_path / "scratch",
        ):
            pass

    assert list((tmp_path / "scratch").iterdir()) == []


def test_download_bronze_materializes_a_persistent_directory(tmp_path, monkeypatch):
    store = FakeObjectStore(_bronze_objects())
    monkeypatch.setattr(
        "src.storage.factory.build_store_from_env", lambda: store
    )

    target = download_bronze("2026-10", tmp_path / "data" / "bronze" / "2026-10")

    assert (target / "Empresas0.parquet").exists()
    assert len(list(target.iterdir())) == len(BRONZE_FILE_NAMES)


def test_download_bronze_skips_already_materialized_files(tmp_path, monkeypatch):
    store = FakeObjectStore(_bronze_objects())
    monkeypatch.setattr(
        "src.storage.factory.build_store_from_env", lambda: store
    )
    target = tmp_path / "bronze"
    source = download_bronze("2026-10", target)
    store.get_calls.clear()

    download_bronze("2026-10", source)

    assert store.get_calls == []
