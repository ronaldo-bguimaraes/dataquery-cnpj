"""Opt-in round-trip against a real S3-compatible endpoint (R2 or MinIO).

The default local suite uses the filesystem backend and never needs MinIO. These
tests run only when ``CNPJ_STORAGE_BACKEND=s3`` (or ``r2``) and the configured
bucket is reachable; otherwise they skip explicitly. They exercise the same
production path (``S3ObjectStore``, multipart-capable uploads and
:func:`convert_raw_file`) without touching the RFB.

Run against a real bucket with::

    CNPJ_STORAGE_BACKEND=s3 \\
    CNPJ_S3_ENDPOINT=... CNPJ_S3_BUCKET=... \\
    CNPJ_S3_ACCESS_KEY=... CNPJ_S3_SECRET_KEY=... \\
    docker compose --profile test run --rm airflow-tests \\
        tests/integration/test_minio_pipeline.py -q
"""

from __future__ import annotations

import os

import pytest

from src.conversao.bronze import convert_raw_file
from src.storage.minio import S3ObjectStore, S3OperationError, StorageConfigError
from tests.fakes import make_zip, read_parquet

# Dedicated sentinel competence; the fixtures remove both keys before and after
# every test so this file never depends on (or pollutes) real competences.
COMPETENCIA = "2099-12"
NOME = "Cnaes.zip"
RAW_KEY = f"raw/{COMPETENCIA}/{NOME}"
BRONZE_KEY = f"bronze/{COMPETENCIA}/Cnaes.parquet"
CSV = "1111-1/00;Comércio\n2222-2/00;Serviços\n".encode("latin1")


@pytest.fixture(scope="module")
def s3_store() -> S3ObjectStore:
    backend = (os.getenv("CNPJ_STORAGE_BACKEND") or "").strip().lower()
    if backend not in {"s3", "r2"}:
        pytest.skip(
            "backend S3 não selecionado (CNPJ_STORAGE_BACKEND != s3/r2)"
        )

    try:
        store = S3ObjectStore()
    except StorageConfigError as exc:
        pytest.skip(f"S3 não configurado: {exc}")

    try:
        # The bucket is expected to be provisioned; never create it here.
        store.ping()
    except S3OperationError as exc:
        pytest.skip(f"S3 indisponível: {exc}")
    return store


@pytest.fixture
def isolated_keys(s3_store: S3ObjectStore):
    def cleanup() -> None:
        for key in (RAW_KEY, BRONZE_KEY):
            s3_store.delete(key)

    cleanup()
    try:
        yield
    finally:
        cleanup()


def _put_zip(store: S3ObjectStore, tmp_path) -> bytes:
    payload = make_zip({"Cnaes.csv": CSV})
    source = tmp_path / "upload" / NOME
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(payload)
    store.put_atomic(RAW_KEY, source)
    return payload


def test_s3_raw_zip_to_bronze_parquet_round_trip(
    s3_store: S3ObjectStore, isolated_keys, tmp_path
) -> None:
    payload = _put_zip(s3_store, tmp_path)

    metadata = s3_store.head(RAW_KEY)
    assert metadata is not None
    assert metadata.size == len(payload)

    downloaded_raw = tmp_path / "downloaded" / NOME
    downloaded = s3_store.get_to_path(RAW_KEY, downloaded_raw)
    assert downloaded.size == len(payload)
    assert downloaded_raw.read_bytes() == payload

    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()

    result = convert_raw_file(
        COMPETENCIA,
        NOME,
        RAW_KEY,
        store=s3_store,
        scratch_dir=scratch_dir,
    )

    assert result.status == "publicado"
    assert result.raw_key == RAW_KEY
    assert result.bronze_key == BRONZE_KEY
    assert result.linhas == 2
    assert result.bytes_parquet > 0
    assert result.erro is None

    bronze_metadata = s3_store.head(BRONZE_KEY)
    assert bronze_metadata is not None
    assert bronze_metadata.size == result.bytes_parquet

    downloaded_bronze = tmp_path / "downloaded" / "Cnaes.parquet"
    s3_store.get_to_path(BRONZE_KEY, downloaded_bronze)
    table = read_parquet(downloaded_bronze.read_bytes())

    assert table.num_rows == 2
    assert table.column_names == ["cnae_codigo", "cnae_descricao"]
    assert table.to_pydict() == {
        "cnae_codigo": ["1111-1/00", "2222-2/00"],
        "cnae_descricao": ["Comércio", "Serviços"],
    }

    assert list(scratch_dir.rglob("*")) == []
    assert list(tmp_path.rglob("*.part")) == []


def test_s3_reconversion_reuses_raw_and_replaces_bronze(
    s3_store: S3ObjectStore, isolated_keys, tmp_path
) -> None:
    payload = _put_zip(s3_store, tmp_path)

    first = convert_raw_file(
        COMPETENCIA, NOME, RAW_KEY, store=s3_store, scratch_dir=tmp_path
    )
    second = convert_raw_file(
        COMPETENCIA, NOME, RAW_KEY, store=s3_store, scratch_dir=tmp_path
    )

    assert first.status == second.status == "publicado"
    assert first.linhas == second.linhas == 2

    downloaded_raw = tmp_path / "raw" / NOME
    s3_store.get_to_path(RAW_KEY, downloaded_raw)
    assert downloaded_raw.read_bytes() == payload

    downloaded_bronze = tmp_path / "bronze" / "Cnaes.parquet"
    s3_store.get_to_path(BRONZE_KEY, downloaded_bronze)
    assert read_parquet(downloaded_bronze.read_bytes()).num_rows == 2
    assert list(tmp_path.rglob("*.part")) == []
