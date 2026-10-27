from __future__ import annotations

from pathlib import Path

import pytest

import src.storage.filesystem as filesystem_module
from src.storage.filesystem import FilesystemObjectStore
from src.storage.protocol import ObjectMetadata, StorageOperationError

RAW_KEY = "raw/2026-02/Cnaes.zip"
BRONZE_KEY = "bronze/2026-02/Cnaes.parquet"


def make_store(tmp_path: Path) -> tuple[FilesystemObjectStore, Path]:
    root = tmp_path / "data"
    root.mkdir()
    return FilesystemObjectStore(root=root), root


def part_files(root: Path) -> list[Path]:
    return [path for path in root.rglob("*") if path.is_file() and ".part" in path.name]


def test_ping_accepts_existing_writable_directory(tmp_path):
    store, _ = make_store(tmp_path)

    store.ping()


def test_ping_rejects_missing_root(tmp_path):
    store = FilesystemObjectStore(root=tmp_path / "missing")

    with pytest.raises(StorageOperationError) as excinfo:
        store.ping()

    assert "não existe" in str(excinfo.value)


def test_ping_rejects_regular_file_as_root(tmp_path):
    root = tmp_path / "data"
    root.write_bytes(b"not-a-directory")
    store = FilesystemObjectStore(root=root)

    with pytest.raises(StorageOperationError) as excinfo:
        store.ping()

    assert "não é diretório" in str(excinfo.value)


def test_ping_rejects_non_writable_root(tmp_path, monkeypatch):
    store, _ = make_store(tmp_path)
    monkeypatch.setattr(
        filesystem_module.os, "access", lambda path, mode: False
    )

    with pytest.raises(StorageOperationError) as excinfo:
        store.ping()

    assert "permissão de escrita" in str(excinfo.value)


def test_head_returns_none_for_missing_object(tmp_path):
    store, _ = make_store(tmp_path)

    assert store.head(RAW_KEY) is None


def test_head_returns_size_for_existing_object(tmp_path):
    store, root = make_store(tmp_path)
    path = root / RAW_KEY
    path.parent.mkdir(parents=True)
    path.write_bytes(b"12345")

    metadata = store.head(RAW_KEY)

    assert isinstance(metadata, ObjectMetadata)
    assert metadata.key == RAW_KEY
    assert metadata.size == 5


def test_zero_byte_object_is_present_but_not_reusable(tmp_path):
    store, root = make_store(tmp_path)
    path = root / RAW_KEY
    path.parent.mkdir(parents=True)
    path.write_bytes(b"")

    metadata = store.head(RAW_KEY)

    assert metadata is not None
    assert metadata.size == 0
    assert (metadata.size > 0) is False


@pytest.mark.parametrize(
    "key",
    ["../escape.zip", "/etc/passwd", "raw/../escape.zip", "s3://bucket/key", "raw\\x.zip"],
)
def test_unsafe_keys_are_rejected(tmp_path, key):
    store, _ = make_store(tmp_path)

    with pytest.raises(ValueError):
        store.head(key)
    with pytest.raises(ValueError):
        store.put_atomic(key, tmp_path / "source")


def test_get_to_path_copies_bytes_and_returns_metadata(tmp_path):
    store, root = make_store(tmp_path)
    path = root / RAW_KEY
    path.parent.mkdir(parents=True)
    path.write_bytes(b"zip-bytes")
    destination = tmp_path / "scratch" / "Cnaes.zip"

    metadata = store.get_to_path(RAW_KEY, destination)

    assert destination.read_bytes() == b"zip-bytes"
    assert metadata.key == RAW_KEY
    assert metadata.size == len(b"zip-bytes")
    assert part_files(root) == []
    assert part_files(destination.parent) == []


def test_get_to_path_missing_object_is_sanitized(tmp_path):
    store, root = make_store(tmp_path)

    with pytest.raises(StorageOperationError) as excinfo:
        store.get_to_path(RAW_KEY, tmp_path / "Cnaes.zip")

    assert "objeto ausente" in str(excinfo.value)
    assert part_files(root) == []
    assert list(tmp_path.glob("*.part")) == []


def test_get_to_path_failure_leaves_no_temporary_or_partial_destination(
    tmp_path, monkeypatch
):
    store, root = make_store(tmp_path)
    path = root / RAW_KEY
    path.parent.mkdir(parents=True)
    path.write_bytes(b"zip-bytes")
    destination = tmp_path / "scratch" / "Cnaes.zip"

    def broken_copy(source, target, *, chunk_size=1024):
        Path(target).write_bytes(b"partial")
        raise OSError("disco cheio")

    monkeypatch.setattr(filesystem_module, "_copy_file", broken_copy)

    with pytest.raises(StorageOperationError) as excinfo:
        store.get_to_path(RAW_KEY, destination)

    assert "download do objeto" in str(excinfo.value)
    assert not destination.exists()
    assert part_files(root) == []
    assert list(destination.parent.glob("*.part")) == []


def test_put_atomic_writes_object_and_removes_temporary(tmp_path):
    store, root = make_store(tmp_path)
    source = tmp_path / "Cnaes.zip"
    source.write_bytes(b"zip-bytes")

    metadata = store.put_atomic(RAW_KEY, source)

    assert (root / RAW_KEY).read_bytes() == b"zip-bytes"
    assert metadata.key == RAW_KEY
    assert metadata.size == len(b"zip-bytes")
    assert part_files(root) == []


def test_put_atomic_replaces_previous_object(tmp_path):
    store, root = make_store(tmp_path)
    destination = root / BRONZE_KEY
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"previous")
    source = tmp_path / "Cnaes.parquet"
    source.write_bytes(b"PAR1-new")

    store.put_atomic(BRONZE_KEY, source)

    assert destination.read_bytes() == b"PAR1-new"
    assert part_files(root) == []


def test_put_atomic_failure_preserves_previous_object_and_removes_temporary(
    tmp_path, monkeypatch
):
    store, root = make_store(tmp_path)
    destination = root / BRONZE_KEY
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"previous-valid-parquet")
    source = tmp_path / "Cnaes.parquet"
    source.write_bytes(b"PAR1-new")

    def broken_copy(source_path, target, *, chunk_size=1024):
        Path(target).write_bytes(b"partial")
        raise OSError("disco cheio")

    monkeypatch.setattr(filesystem_module, "_copy_file", broken_copy)

    with pytest.raises(StorageOperationError) as excinfo:
        store.put_atomic(BRONZE_KEY, source)

    assert "upload do objeto" in str(excinfo.value)
    assert destination.read_bytes() == b"previous-valid-parquet"
    assert part_files(root) == []


def test_put_atomic_rejects_empty_source_without_publishing(tmp_path):
    store, root = make_store(tmp_path)
    source = tmp_path / "empty.zip"
    source.write_bytes(b"")

    with pytest.raises(ValueError):
        store.put_atomic(RAW_KEY, source)

    assert not (root / RAW_KEY).exists()
    assert part_files(root) == []


def test_put_atomic_missing_source_is_sanitized(tmp_path):
    store, root = make_store(tmp_path)

    with pytest.raises(StorageOperationError) as excinfo:
        store.put_atomic(RAW_KEY, tmp_path / "missing.zip")

    assert "leitura do arquivo local" in str(excinfo.value)
    assert not (root / RAW_KEY).exists()


def test_delete_is_idempotent(tmp_path):
    store, root = make_store(tmp_path)
    path = root / RAW_KEY
    path.parent.mkdir(parents=True)
    path.write_bytes(b"zip-bytes")

    store.delete(RAW_KEY)
    store.delete(RAW_KEY)

    assert not path.exists()


def test_repr_does_not_expose_secrets(tmp_path):
    store, root = make_store(tmp_path)

    assert repr(store) == f"FilesystemObjectStore(root={str(root.resolve())!r})"
    assert "secret" not in repr(store).lower()
    assert "token" not in repr(store).lower()
