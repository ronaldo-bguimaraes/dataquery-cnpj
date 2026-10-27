"""S3-compatible backend (Cloudflare R2 in production, MinIO for local tests).

The store exposes only the operations the pipeline needs. Credentials are read from
the environment (or injected explicitly) and never appear in ``repr``, logs or
exception messages. The final object key is only written after a complete PUT or a
complete multipart upload, so interrupted uploads never become visible.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from src.download_utils.redaction import describe_exception
from src.storage.protocol import (
    ObjectMetadata,
    StorageConfigError,
    StorageOperationError,
    validate_key,
)

try:  # Optional: Compose passes the values as real environment variables.
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover - dotenv is present in the local runtime
    pass


DEFAULT_MULTIPART_THRESHOLD = 8 * 1024 * 1024
DEFAULT_MULTIPART_CHUNK_SIZE = 8 * 1024 * 1024
DEFAULT_READ_CHUNK_SIZE = 1024 * 1024

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}
_BUCKET_ALREADY_EXISTS_CODES = {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}


# Backwards-compatible name kept for existing callers/tests.
S3OperationError = StorageOperationError


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _is_not_found(exc: ClientError) -> bool:
    return _error_code(exc) in _NOT_FOUND_CODES


class S3ObjectStore:
    """S3-compatible object store with atomic publication semantics."""

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        bucket: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region_name: str | None = None,
        client: Any | None = None,
        multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD,
        multipart_chunk_size: int = DEFAULT_MULTIPART_CHUNK_SIZE,
        read_chunk_size: int = DEFAULT_READ_CHUNK_SIZE,
    ) -> None:
        self._endpoint = endpoint or os.getenv("CNPJ_S3_ENDPOINT")
        self._bucket = bucket or os.getenv("CNPJ_S3_BUCKET")
        access_key = access_key or os.getenv("CNPJ_S3_ACCESS_KEY")
        secret_key = secret_key or os.getenv("CNPJ_S3_SECRET_KEY")

        if not self._endpoint:
            raise StorageConfigError("CNPJ_S3_ENDPOINT não configurado")
        if not self._bucket:
            raise StorageConfigError("CNPJ_S3_BUCKET não configurado")

        if multipart_threshold < 1:
            raise ValueError("multipart_threshold deve ser >= 1")
        if multipart_chunk_size < 1:
            raise ValueError("multipart_chunk_size deve ser >= 1")
        if read_chunk_size < 1:
            raise ValueError("read_chunk_size deve ser >= 1")

        self._multipart_threshold = multipart_threshold
        self._multipart_chunk_size = multipart_chunk_size
        self._read_chunk_size = read_chunk_size

        if client is None:
            if not access_key or not secret_key:
                raise StorageConfigError("credenciais S3 não configuradas")
            client = boto3.session.Session().client(
                "s3",
                endpoint_url=self._endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name
                or os.getenv("CNPJ_S3_REGION")
                or "us-east-1",
                config=Config(
                    signature_version="s3v4",
                    retries={"max_attempts": 3, "mode": "standard"},
                ),
            )
        self._client = client

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def bucket(self) -> str:
        return self._bucket

    def __repr__(self) -> str:
        return f"S3ObjectStore(endpoint={self._endpoint!r}, bucket={self._bucket!r})"

    def _raise(self, action: str, key: str | None, exc: BaseException) -> None:
        target = f" {key}" if key else ""
        raise S3OperationError(
            f"{action} falhou{target}: {describe_exception(exc)}"
        ) from None

    def ensure_bucket(self) -> None:
        """Create the configured bucket if it does not exist (idempotent)."""

        try:
            self._client.head_bucket(Bucket=self._bucket)
            return
        except ClientError as exc:
            if not _is_not_found(exc):
                self._raise("verificação do bucket", self._bucket, exc)
        except BotoCoreError as exc:
            self._raise("verificação do bucket", self._bucket, exc)

        try:
            self._client.create_bucket(Bucket=self._bucket)
        except ClientError as exc:
            if _error_code(exc) in _BUCKET_ALREADY_EXISTS_CODES:
                return
            self._raise("criação do bucket", self._bucket, exc)
        except BotoCoreError as exc:
            self._raise("criação do bucket", self._bucket, exc)

    def ping(self) -> None:
        """Check that the configured bucket is reachable.

        The bucket is not created here: production (R2) assumes it is already
        provisioned, while local tests may call :meth:`ensure_bucket` explicitly.
        """

        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError as exc:
            if _is_not_found(exc):
                raise StorageOperationError(
                    f"bucket não encontrado: {self._bucket}; "
                    "provisione o bucket antes de executar"
                ) from None
            self._raise("verificação do bucket", self._bucket, exc)
        except BotoCoreError as exc:
            self._raise("verificação do bucket", self._bucket, exc)

    def head(self, key: str) -> ObjectMetadata | None:
        """Return metadata for a logical key or ``None`` when absent."""

        return self.head_object(validate_key(key))

    def get_to_path(self, key: str, dest: Path) -> ObjectMetadata:
        """Copy ``key`` to a local scratch path and return its metadata."""

        key = validate_key(key)
        size = self.get_object(key, dest)
        return ObjectMetadata(key=key, size=size)

    def put_atomic(self, key: str, source: Path) -> ObjectMetadata:
        """Publish ``source`` under ``key`` only after the upload completes."""

        key = validate_key(key)
        size = self.put_object(key, source)
        return ObjectMetadata(key=key, size=size)

    def delete(self, key: str) -> None:
        """Idempotently remove a logical key."""

        self.delete_object(validate_key(key))

    def head_object(self, key: str) -> ObjectMetadata | None:
        """Return metadata for ``key`` or ``None`` when the object is absent."""

        try:
            response = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _is_not_found(exc):
                return None
            self._raise("leitura de metadados", key, exc)
        except BotoCoreError as exc:
            self._raise("leitura de metadados", key, exc)

        return ObjectMetadata(
            key=key,
            size=int(response.get("ContentLength", 0)),
            etag=response.get("ETag"),
        )

    def has_valid_object(self, key: str) -> bool:
        """An object is reusable only when it exists and has a positive size."""

        metadata = self.head_object(key)
        return metadata is not None and metadata.size > 0

    def get_object(self, key: str, destination: str | os.PathLike[str]) -> int:
        """Download ``key`` to ``destination`` atomically and return its size."""

        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(
            f".{target.name}.{uuid.uuid4().hex}.part"
        )
        body = None
        written = 0
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            body = response["Body"]
            with temporary.open("wb") as handle:
                while True:
                    chunk = body.read(self._read_chunk_size)
                    if not chunk:
                        break
                    handle.write(chunk)
                    written += len(chunk)
            os.replace(temporary, target)
            return written
        except ClientError as exc:
            if _is_not_found(exc):
                raise S3OperationError(f"objeto ausente: {key}") from None
            self._raise("download do objeto", key, exc)
        except (BotoCoreError, OSError) as exc:
            self._raise("download do objeto", key, exc)
        finally:
            if body is not None:
                try:
                    body.close()
                except Exception:  # noqa: BLE001 - cleanup must never mask the error
                    pass
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def delete_object(self, key: str) -> None:
        """Remove ``key``; deleting a missing key is a no-op for S3/MinIO."""

        try:
            self._client.delete_object(Bucket=self._bucket, Key=key)
        except (ClientError, BotoCoreError) as exc:
            self._raise("remoção do objeto", key, exc)

    def put_object(self, key: str, source: str | os.PathLike[str]) -> int:
        """Publish ``source`` under ``key`` and return the written size."""

        path = Path(source)
        try:
            size = path.stat().st_size
        except OSError as exc:
            self._raise("leitura do arquivo local", key, exc)

        if size <= 0:
            raise ValueError("objeto vazio não é publicado")

        if size >= self._multipart_threshold:
            return self._put_multipart(key, path, size)
        return self._put_single(key, path, size)

    def _put_single(self, key: str, path: Path, size: int) -> int:
        try:
            with path.open("rb") as handle:
                self._client.put_object(
                    Bucket=self._bucket, Key=key, Body=handle
                )
        except (ClientError, BotoCoreError, OSError) as exc:
            self._raise("upload do objeto", key, exc)
        return size

    def _put_multipart(self, key: str, path: Path, size: int) -> int:
        upload_id: str | None = None
        try:
            response = self._client.create_multipart_upload(
                Bucket=self._bucket, Key=key
            )
            upload_id = response["UploadId"]
            parts: list[dict[str, Any]] = []
            with path.open("rb") as handle:
                part_number = 1
                while True:
                    chunk = handle.read(self._multipart_chunk_size)
                    if not chunk:
                        break
                    part = self._client.upload_part(
                        Bucket=self._bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    parts.append(
                        {"PartNumber": part_number, "ETag": part["ETag"]}
                    )
                    part_number += 1

            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return size
        except (ClientError, BotoCoreError, KeyError, OSError) as exc:
            if upload_id is not None:
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._bucket, Key=key, UploadId=upload_id
                    )
                except Exception:  # noqa: BLE001 - preserve the original failure
                    pass
            self._raise("upload multipart do objeto", key, exc)


def get_minio_session():
    """Legacy helper kept for callers that build their own boto3 session."""

    return boto3.Session(
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )


def get_minio_resource():
    """Legacy helper kept for callers that build their own boto3 resource."""

    return get_minio_session().resource(
        service_name="s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )
