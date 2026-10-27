from __future__ import annotations

import logging
from pathlib import Path

import pytest

from src.storage.minio import (
    ObjectMetadata,
    S3ObjectStore,
    S3OperationError,
    StorageConfigError,
)
from botocore.exceptions import EndpointConnectionError

from tests.fakes import FakeS3Client, client_error

ENDPOINT = "http://minio.test:9000"
BUCKET = "cnpj-data"
ACCESS_KEY = "access-key-for-tests"
SECRET_KEY = "super-secret-key-for-tests"


def make_store(fake: FakeS3Client | None = None, **kwargs) -> S3ObjectStore:
    return S3ObjectStore(
        endpoint=ENDPOINT,
        bucket=BUCKET,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        client=fake or FakeS3Client(),
        **kwargs,
    )


def test_configuration_can_come_from_environment(monkeypatch):
    monkeypatch.setenv("CNPJ_S3_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("CNPJ_S3_BUCKET", BUCKET)
    monkeypatch.setenv("CNPJ_S3_ACCESS_KEY", ACCESS_KEY)
    monkeypatch.setenv("CNPJ_S3_SECRET_KEY", SECRET_KEY)

    store = S3ObjectStore(client=FakeS3Client())

    assert store.endpoint == ENDPOINT
    assert store.bucket == BUCKET


@pytest.mark.parametrize(
    "missing",
    ["CNPJ_S3_ENDPOINT", "CNPJ_S3_BUCKET"],
)
def test_missing_required_configuration_fails(monkeypatch, missing):
    values = {
        "CNPJ_S3_ENDPOINT": ENDPOINT,
        "CNPJ_S3_BUCKET": BUCKET,
        "CNPJ_S3_ACCESS_KEY": ACCESS_KEY,
        "CNPJ_S3_SECRET_KEY": SECRET_KEY,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv(missing)

    with pytest.raises(StorageConfigError):
        S3ObjectStore(client=FakeS3Client())


def test_missing_credentials_fail_when_client_is_not_injected(monkeypatch):
    monkeypatch.setenv("CNPJ_S3_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("CNPJ_S3_BUCKET", BUCKET)
    monkeypatch.delenv("CNPJ_S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("CNPJ_S3_SECRET_KEY", raising=False)

    with pytest.raises(StorageConfigError):
        S3ObjectStore()


def test_repr_and_logs_do_not_expose_credentials(caplog):
    store = make_store()

    with caplog.at_level(logging.DEBUG):
        store.ensure_bucket()

    assert SECRET_KEY not in repr(store)
    assert ACCESS_KEY not in repr(store)
    assert SECRET_KEY not in caplog.text
    assert ACCESS_KEY not in caplog.text


def test_ensure_bucket_creates_missing_bucket_idempotently():
    fake = FakeS3Client()
    store = make_store(fake)

    store.ensure_bucket()
    store.ensure_bucket()

    assert fake.buckets == {BUCKET}
    assert fake.create_bucket_calls == [BUCKET]
    assert fake.head_bucket_calls == [BUCKET, BUCKET]


def test_ensure_bucket_propagates_sanitized_error():
    fake = FakeS3Client()
    fake.fail_head_bucket = client_error("AccessDenied", "Access Denied")
    store = make_store(fake)

    with pytest.raises(S3OperationError) as excinfo:
        store.ensure_bucket()

    message = str(excinfo.value)
    assert "AccessDenied" in message or "Access Denied" in message
    assert SECRET_KEY not in message
    assert ACCESS_KEY not in message


def test_head_object_returns_none_when_missing():
    store = make_store()

    assert store.head_object("raw/2026-02/Cnaes.zip") is None
    assert store.has_valid_object("raw/2026-02/Cnaes.zip") is False


def test_head_object_returns_size_for_existing_object():
    fake = FakeS3Client()
    fake.objects[(BUCKET, "raw/2026-02/Cnaes.zip")] = b"12345"
    store = make_store(fake)

    metadata = store.head_object("raw/2026-02/Cnaes.zip")

    assert isinstance(metadata, ObjectMetadata)
    assert metadata.key == "raw/2026-02/Cnaes.zip"
    assert metadata.size == 5
    assert store.has_valid_object("raw/2026-02/Cnaes.zip") is True


def test_zero_byte_object_is_not_valid():
    fake = FakeS3Client()
    fake.objects[(BUCKET, "raw/2026-02/Cnaes.zip")] = b""
    store = make_store(fake)

    assert store.head_object("raw/2026-02/Cnaes.zip").size == 0
    assert store.has_valid_object("raw/2026-02/Cnaes.zip") is False


def test_head_object_translates_errors_without_credentials():
    fake = FakeS3Client()
    fake.fail_head_object = client_error("InternalError", "boom")
    store = make_store(fake)

    with pytest.raises(S3OperationError) as excinfo:
        store.head_object("raw/2026-02/Cnaes.zip")

    assert "raw/2026-02/Cnaes.zip" in str(excinfo.value)
    assert SECRET_KEY not in str(excinfo.value)


def test_get_object_writes_exact_bytes(tmp_path):
    fake = FakeS3Client()
    fake.objects[(BUCKET, "raw/2026-02/Cnaes.zip")] = b"zip-bytes"
    store = make_store(fake)
    destination = tmp_path / "scratch" / "Cnaes.zip"

    written = store.get_object("raw/2026-02/Cnaes.zip", destination)

    assert written == 9
    assert destination.read_bytes() == b"zip-bytes"
    assert list(tmp_path.rglob("*.part")) == []


def test_get_object_missing_key_raises_sanitized_error(tmp_path):
    store = make_store()

    with pytest.raises(S3OperationError) as excinfo:
        store.get_object("raw/2026-02/Cnaes.zip", tmp_path / "Cnaes.zip")

    assert "raw/2026-02/Cnaes.zip" in str(excinfo.value)
    assert SECRET_KEY not in str(excinfo.value)
    assert list(tmp_path.rglob("*.part")) == []


def test_get_object_failure_removes_temporary_file(tmp_path):
    fake = FakeS3Client()
    fake.fail_get = client_error("InternalError", "boom")
    store = make_store(fake)

    with pytest.raises(S3OperationError):
        store.get_object("raw/2026-02/Cnaes.zip", tmp_path / "Cnaes.zip")

    assert list(tmp_path.iterdir()) == []


def test_put_object_uses_single_upload_for_small_files(tmp_path):
    fake = FakeS3Client()
    store = make_store(fake)
    source = tmp_path / "Cnaes.zip"
    source.write_bytes(b"small")

    written = store.put_object("raw/2026-02/Cnaes.zip", source)

    assert written == 5
    assert fake.objects[(BUCKET, "raw/2026-02/Cnaes.zip")] == b"small"
    assert fake.put_object_calls == [(BUCKET, "raw/2026-02/Cnaes.zip", 5)]
    assert fake.completed_multipart == []


def test_put_object_uses_multipart_and_only_publishes_after_complete(tmp_path):
    fake = FakeS3Client()
    store = make_store(fake, multipart_threshold=4, multipart_chunk_size=4)
    source = tmp_path / "Empresas0.zip"
    source.write_bytes(b"0123456789")

    written = store.put_object("raw/2026-02/Empresas0.zip", source)

    assert written == 10
    assert fake.objects[(BUCKET, "raw/2026-02/Empresas0.zip")] == b"0123456789"
    assert fake.put_object_calls == []
    assert fake.completed_multipart == [(BUCKET, "raw/2026-02/Empresas0.zip", 10)]
    assert len(fake.multipart) == 0


def test_multipart_completion_failure_does_not_expose_partial_object(tmp_path):
    fake = FakeS3Client()
    fake.fail_complete = client_error("InternalError", "complete failed")
    store = make_store(fake, multipart_threshold=4, multipart_chunk_size=4)
    source = tmp_path / "Empresas0.zip"
    source.write_bytes(b"0123456789")

    with pytest.raises(S3OperationError):
        store.put_object("raw/2026-02/Empresas0.zip", source)

    assert (BUCKET, "raw/2026-02/Empresas0.zip") not in fake.objects
    assert fake.aborted_multipart


def test_put_object_rejects_empty_file(tmp_path):
    fake = FakeS3Client()
    store = make_store(fake)
    source = tmp_path / "empty.zip"
    source.write_bytes(b"")

    with pytest.raises(ValueError):
        store.put_object("raw/2026-02/empty.zip", source)

    assert (BUCKET, "raw/2026-02/empty.zip") not in fake.objects


def test_bronze_key_round_trip(tmp_path):
    fake = FakeS3Client()
    store = make_store(fake)
    source = tmp_path / "Cnaes.parquet"
    source.write_bytes(b"PAR1-bronze")

    store.put_object("bronze/2026-02/Cnaes.parquet", source)
    destination = tmp_path / "read" / "Cnaes.parquet"
    store.get_object("bronze/2026-02/Cnaes.parquet", destination)

    assert destination.read_bytes() == b"PAR1-bronze"


def test_put_object_missing_source_is_sanitized(tmp_path):
    store = make_store()

    with pytest.raises(S3OperationError) as excinfo:
        store.put_object("raw/2026-02/Cnaes.zip", tmp_path / "missing.zip")

    assert SECRET_KEY not in str(excinfo.value)


def test_delete_object_removes_existing_key():
    fake = FakeS3Client()
    fake.objects[(BUCKET, "raw/2026-02/Cnaes.zip")] = b"zip-bytes"
    store = make_store(fake)

    store.delete_object("raw/2026-02/Cnaes.zip")

    assert (BUCKET, "raw/2026-02/Cnaes.zip") not in fake.objects


def test_delete_object_is_idempotent_for_missing_key():
    store = make_store()

    store.delete_object("raw/2026-02/ausente.zip")


def test_delete_object_translates_errors_without_credentials():
    fake = FakeS3Client()
    fake.fail_delete = client_error("AccessDenied", "Access Denied")
    store = make_store(fake)

    with pytest.raises(S3OperationError) as excinfo:
        store.delete_object("raw/2026-02/Cnaes.zip")

    assert "raw/2026-02/Cnaes.zip" in str(excinfo.value)
    assert SECRET_KEY not in str(excinfo.value)
    assert ACCESS_KEY not in str(excinfo.value)


def test_ensure_bucket_translates_unreachable_endpoint_without_leaking_secrets():
    fake = FakeS3Client()
    fake.fail_head_bucket = EndpointConnectionError(endpoint_url=ENDPOINT)
    store = make_store(fake)

    with pytest.raises(S3OperationError) as excinfo:
        store.ensure_bucket()

    message = str(excinfo.value)
    assert "verificação do bucket" in message
    assert SECRET_KEY not in message
    assert ACCESS_KEY not in message


def test_ensure_bucket_translates_unreachable_endpoint_on_create():
    fake = FakeS3Client()
    fake.fail_create_bucket = EndpointConnectionError(endpoint_url=ENDPOINT)
    store = make_store(fake)

    with pytest.raises(S3OperationError) as excinfo:
        store.ensure_bucket()

    assert "criação do bucket" in str(excinfo.value)


def test_ping_checks_bucket_without_creating_it():
    fake = FakeS3Client()
    fake.buckets.add(BUCKET)
    store = make_store(fake)

    store.ping()

    assert fake.head_bucket_calls == [BUCKET]
    assert fake.create_bucket_calls == []


def test_ping_missing_bucket_is_sanitized_and_does_not_create_it():
    fake = FakeS3Client()
    store = make_store(fake)

    with pytest.raises(S3OperationError) as excinfo:
        store.ping()

    message = str(excinfo.value)
    assert "bucket não encontrado" in message
    assert "provisione" in message
    assert fake.create_bucket_calls == []
    assert SECRET_KEY not in message
    assert ACCESS_KEY not in message


def test_protocol_head_matches_head_object():
    fake = FakeS3Client()
    fake.objects[(BUCKET, "raw/2026-02/Cnaes.zip")] = b"12345"
    store = make_store(fake)

    metadata = store.head("raw/2026-02/Cnaes.zip")

    assert isinstance(metadata, ObjectMetadata)
    assert metadata.size == 5
    assert store.has_valid_object("raw/2026-02/Cnaes.zip") is True


def test_protocol_put_atomic_and_get_to_path_round_trip(tmp_path):
    fake = FakeS3Client()
    store = make_store(fake)
    source = tmp_path / "Cnaes.zip"
    source.write_bytes(b"zip-bytes")
    destination = tmp_path / "scratch" / "Cnaes.zip"

    published = store.put_atomic("raw/2026-02/Cnaes.zip", source)
    downloaded = store.get_to_path("raw/2026-02/Cnaes.zip", destination)

    assert published.size == 9
    assert downloaded.size == 9
    assert downloaded.key == "raw/2026-02/Cnaes.zip"
    assert destination.read_bytes() == b"zip-bytes"
    assert list(tmp_path.rglob("*.part")) == []


def test_protocol_put_atomic_rejects_empty_object(tmp_path):
    fake = FakeS3Client()
    store = make_store(fake)
    source = tmp_path / "empty.zip"
    source.write_bytes(b"")

    with pytest.raises(ValueError):
        store.put_atomic("raw/2026-02/empty.zip", source)

    assert (BUCKET, "raw/2026-02/empty.zip") not in fake.objects


def test_protocol_delete_is_idempotent():
    fake = FakeS3Client()
    fake.objects[(BUCKET, "raw/2026-02/Cnaes.zip")] = b"zip-bytes"
    store = make_store(fake)

    store.delete("raw/2026-02/Cnaes.zip")
    store.delete("raw/2026-02/ausente.zip")

    assert (BUCKET, "raw/2026-02/Cnaes.zip") not in fake.objects


@pytest.mark.parametrize(
    "key", ["../escape.zip", "/etc/passwd", "s3://bucket/key", "raw\\x.zip"]
)
def test_protocol_rejects_unsafe_keys(tmp_path, key):
    store = make_store()
    source = tmp_path / "Cnaes.zip"
    source.write_bytes(b"zip-bytes")

    with pytest.raises(ValueError):
        store.head(key)
    with pytest.raises(ValueError):
        store.put_atomic(key, source)
