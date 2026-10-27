from __future__ import annotations

import pytest

from src.storage.factory import build_store_from_env
from src.storage.filesystem import FilesystemObjectStore
from src.storage.minio import S3ObjectStore
from src.storage.protocol import StorageConfigError

ENDPOINT = "http://s3.test:9000"
BUCKET = "cnpj-data"
ACCESS_KEY = "access-key-for-tests"
SECRET_KEY = "super-secret-key-for-tests"


def set_s3_env(monkeypatch, **overrides):
    values = {
        "CNPJ_S3_ENDPOINT": ENDPOINT,
        "CNPJ_S3_BUCKET": BUCKET,
        "CNPJ_S3_ACCESS_KEY": ACCESS_KEY,
        "CNPJ_S3_SECRET_KEY": SECRET_KEY,
    }
    values.update(overrides)
    for name, value in values.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)


def test_omitted_backend_defaults_to_filesystem(monkeypatch, tmp_path):
    monkeypatch.delenv("CNPJ_STORAGE_BACKEND", raising=False)
    monkeypatch.setenv("CNPJ_DATA_ROOT", str(tmp_path / "data"))

    store = build_store_from_env()

    assert isinstance(store, FilesystemObjectStore)
    assert store.root == (tmp_path / "data").resolve()


@pytest.mark.parametrize("backend", ["filesystem", "local", "fs", " FILESYSTEM "])
def test_filesystem_aliases(monkeypatch, tmp_path, backend):
    monkeypatch.setenv("CNPJ_STORAGE_BACKEND", backend)
    monkeypatch.setenv("CNPJ_DATA_ROOT", str(tmp_path / "data"))

    store = build_store_from_env()

    assert isinstance(store, FilesystemObjectStore)
    assert store.root == (tmp_path / "data").resolve()


@pytest.mark.parametrize("backend", ["s3", "r2", "R2"])
def test_s3_backend_uses_s3_configuration(monkeypatch, backend):
    monkeypatch.setenv("CNPJ_STORAGE_BACKEND", backend)
    set_s3_env(monkeypatch)

    store = build_store_from_env()

    assert isinstance(store, S3ObjectStore)
    assert store.endpoint == ENDPOINT
    assert store.bucket == BUCKET
    assert SECRET_KEY not in repr(store)
    assert ACCESS_KEY not in repr(store)


@pytest.mark.parametrize(
    "missing",
    [
        "CNPJ_S3_ENDPOINT",
        "CNPJ_S3_BUCKET",
        "CNPJ_S3_ACCESS_KEY",
        "CNPJ_S3_SECRET_KEY",
    ],
)
def test_s3_backend_requires_full_configuration(monkeypatch, missing):
    monkeypatch.setenv("CNPJ_STORAGE_BACKEND", "s3")
    set_s3_env(monkeypatch, **{missing: None})

    with pytest.raises(StorageConfigError) as excinfo:
        build_store_from_env()

    assert SECRET_KEY not in str(excinfo.value)
    assert ACCESS_KEY not in str(excinfo.value)


def test_invalid_backend_fails_before_any_backend_io(monkeypatch):
    monkeypatch.setenv("CNPJ_STORAGE_BACKEND", "ftp")
    monkeypatch.delenv("CNPJ_DATA_ROOT", raising=False)

    with pytest.raises(StorageConfigError) as excinfo:
        build_store_from_env()

    message = str(excinfo.value)
    assert "CNPJ_STORAGE_BACKEND" in message
    assert "filesystem" in message
    assert "s3" in message
    assert SECRET_KEY not in message
    assert ACCESS_KEY not in message
