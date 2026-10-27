"""Round-trip on the local filesystem backend, without RFB or boto3.

These tests use the same ``FilesystemObjectStore`` and ``convert_raw_file`` used by
the DAG when ``CNPJ_STORAGE_BACKEND=filesystem``. They are skipped only when the
temporary root is not writable.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import src.storage.filesystem as filesystem_module
from src.conversao.bronze import convert_raw_file
from src.storage.filesystem import FilesystemObjectStore
from src.storage.protocol import StorageOperationError
from tests.fakes import make_zip

CSV = "1111-1/00;Comércio\n2222-2/00;Serviços\n".encode("latin1")
RAW_KEY = "raw/2026-02/Cnaes.zip"
BRONZE_KEY = "bronze/2026-02/Cnaes.parquet"


@pytest.fixture
def root(tmp_path):
    path = tmp_path / "data"
    path.mkdir()
    if not os.access(path, os.W_OK):
        pytest.skip("raiz temporária sem permissão de escrita")
    return path


def test_filesystem_round_trip_from_raw_to_bronze(root, tmp_path):
    store = FilesystemObjectStore(root=root)
    source = tmp_path / "Cnaes.zip"
    source.write_bytes(make_zip({"Cnaes.csv": CSV}))
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    raw_metadata = store.put_atomic(RAW_KEY, source)
    result = convert_raw_file(
        "2026-02",
        "Cnaes.zip",
        RAW_KEY,
        store=store,
        scratch_dir=scratch,
    )

    assert raw_metadata.size > 0
    assert (root / RAW_KEY).read_bytes() == source.read_bytes()
    assert result.status == "publicado"
    assert result.bronze_key == BRONZE_KEY
    assert result.linhas == 2
    bronze_path = root / BRONZE_KEY
    assert bronze_path.is_file()
    assert bronze_path.read_bytes().startswith(b"PAR1")
    table: pa.Table = pq.read_table(bronze_path)
    assert table.column_names == ["cnae_codigo", "cnae_descricao"]
    assert table.num_rows == 2
    assert table.to_pydict()["cnae_descricao"] == ["Comércio", "Serviços"]
    assert list(scratch.iterdir()) == []
    assert not list(root.rglob("*.part"))


def test_zero_byte_raw_is_present_but_not_reusable(root):
    store = FilesystemObjectStore(root=root)
    path = root / RAW_KEY
    path.parent.mkdir(parents=True)
    path.write_bytes(b"")

    metadata = store.head(RAW_KEY)

    assert metadata is not None
    assert metadata.size == 0
    assert (metadata.size > 0) is False


def test_failed_put_atomic_keeps_previous_bronze_visible(root, tmp_path, monkeypatch):
    store = FilesystemObjectStore(root=root)
    bronze_path = root / BRONZE_KEY
    bronze_path.parent.mkdir(parents=True)
    bronze_path.write_bytes(b"previous-valid-parquet")
    candidate = tmp_path / "Cnaes.parquet"
    candidate.write_bytes(b"PAR1-new")
    original = filesystem_module._copy_file

    def broken_copy(source, destination, *, chunk_size=1024):
        from pathlib import Path

        Path(destination).write_bytes(b"partial")
        raise OSError("disco cheio")

    monkeypatch.setattr(filesystem_module, "_copy_file", broken_copy)

    with pytest.raises(StorageOperationError):
        store.put_atomic(BRONZE_KEY, candidate)

    assert bronze_path.read_bytes() == b"previous-valid-parquet"
    assert not list(root.rglob("*.part"))
    assert callable(original)


def test_filesystem_backend_does_not_import_boto3():
    assert "boto3" not in filesystem_module.__dict__
    assert getattr(filesystem_module, "boto3", None) is None
