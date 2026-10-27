"""Shared test doubles for the Raw -> Bronze pipeline.

The fakes implement only the small storage protocol used by the pipeline, so
tests never need a running object store.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.storage.protocol import ObjectMetadata, StorageOperationError as S3OperationError


def client_error(code: str, message: str = "falha simulada") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "FakeS3Operation",
    )


class FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunk = self._data[self._offset :]
            self._offset = len(self._data)
            return chunk
        chunk = self._data[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def iter_chunks(self, chunk_size: int = 1024):
        for start in range(0, len(self._data), chunk_size):
            yield self._data[start : start + chunk_size]

    def close(self) -> None:
        self.closed = True


class FakeS3Client:
    """In-memory boto3 S3 client with multipart and failure hooks."""

    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], bytes] = {}
        self.multipart: dict[str, dict] = {}
        self.head_bucket_calls: list[str] = []
        self.create_bucket_calls: list[str] = []
        self.put_object_calls: list[tuple[str, str, int]] = []
        self.completed_multipart: list[tuple[str, str, int]] = []
        self.aborted_multipart: list[str] = []
        self.fail_head_bucket: Exception | None = None
        self.fail_create_bucket: Exception | None = None
        self.fail_head_object: Exception | None = None
        self.fail_get: Exception | None = None
        self.fail_put: Exception | None = None
        self.fail_delete: Exception | None = None
        self.fail_complete: Exception | None = None
        self.delete_object_calls: list[tuple[str, str]] = []
        self._next_upload_id = 1

    def head_bucket(self, *, Bucket: str):
        self.head_bucket_calls.append(Bucket)
        if self.fail_head_bucket is not None:
            raise self.fail_head_bucket
        if Bucket not in self.buckets:
            raise client_error("404", "Not Found")
        return {}

    def create_bucket(self, *, Bucket: str):
        self.create_bucket_calls.append(Bucket)
        if self.fail_create_bucket is not None:
            raise self.fail_create_bucket
        self.buckets.add(Bucket)
        return {}

    def head_object(self, *, Bucket: str, Key: str):
        if self.fail_head_object is not None:
            raise self.fail_head_object
        data = self.objects.get((Bucket, Key))
        if data is None:
            raise client_error("404", "Not Found")
        return {"ContentLength": len(data), "ETag": '"fake-etag"'}

    def get_object(self, *, Bucket: str, Key: str):
        if self.fail_get is not None:
            raise self.fail_get
        data = self.objects.get((Bucket, Key))
        if data is None:
            raise client_error("NoSuchKey", "Not Found")
        return {"Body": FakeBody(data), "ContentLength": len(data)}

    def put_object(self, *, Bucket: str, Key: str, Body, **_kwargs):
        if self.fail_put is not None:
            raise self.fail_put
        data = Body.read() if hasattr(Body, "read") else bytes(Body)
        self.objects[(Bucket, Key)] = data
        self.put_object_calls.append((Bucket, Key, len(data)))
        return {"ETag": '"fake-etag"'}

    def delete_object(self, *, Bucket: str, Key: str):
        if self.fail_delete is not None:
            raise self.fail_delete
        self.delete_object_calls.append((Bucket, Key))
        self.objects.pop((Bucket, Key), None)
        return {}

    def create_multipart_upload(self, *, Bucket: str, Key: str, **_kwargs):
        upload_id = f"upload-{self._next_upload_id}"
        self._next_upload_id += 1
        self.multipart[upload_id] = {"Bucket": Bucket, "Key": Key, "Parts": {}}
        return {"UploadId": upload_id}

    def upload_part(self, *, Bucket: str, Key: str, PartNumber: int, UploadId: str, Body, **_kwargs):
        upload = self.multipart.get(UploadId)
        if upload is None:
            raise client_error("NoSuchUpload", "upload desconhecido")
        upload["Parts"][PartNumber] = bytes(Body)
        return {"ETag": f'"part-{PartNumber}"'}

    def complete_multipart_upload(
        self, *, Bucket: str, Key: str, UploadId: str, MultipartUpload, **_kwargs
    ):
        if self.fail_complete is not None:
            raise self.fail_complete
        upload = self.multipart.pop(UploadId)
        data = b"".join(
            upload["Parts"][part["PartNumber"]] for part in MultipartUpload["Parts"]
        )
        self.objects[(Bucket, Key)] = data
        self.completed_multipart.append((Bucket, Key, len(data)))
        return {"ETag": '"fake-etag"'}

    def abort_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str, **_kwargs):
        self.aborted_multipart.append(UploadId)
        self.multipart.pop(UploadId, None)
        return {}


class FakeObjectStore:
    """In-memory implementation of the :class:`ObjectStore` protocol.

    The counter names are historical (``ensure_calls`` now counts ``ping``) so
    existing domain tests keep asserting the same behaviour without an object
    store running.
    """

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects: dict[str, bytes] = dict(objects or {})
        self.ensure_calls = 0
        self.head_calls: list[str] = []
        self.get_calls: list[str] = []
        self.put_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.fail_ensure: Exception | None = None
        self.fail_head: Exception | None = None
        self.fail_get: Exception | None = None
        self.fail_put: Exception | None = None
        self.fail_delete: Exception | None = None

    def ping(self) -> None:
        self.ensure_calls += 1
        if self.fail_ensure is not None:
            raise self.fail_ensure

    def head(self, key: str) -> ObjectMetadata | None:
        self.head_calls.append(key)
        if self.fail_head is not None:
            raise self.fail_head
        if key not in self.objects:
            return None
        return ObjectMetadata(key=key, size=len(self.objects[key]))

    def get_to_path(self, key: str, dest: Path) -> ObjectMetadata:
        self.get_calls.append(key)
        if self.fail_get is not None:
            raise self.fail_get
        if key not in self.objects:
            raise S3OperationError(f"objeto ausente: {key}")
        path = Path(dest)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = self.objects[key]
        path.write_bytes(data)
        return ObjectMetadata(key=key, size=len(data))

    def put_atomic(self, key: str, source: Path) -> ObjectMetadata:
        self.put_calls.append(key)
        if self.fail_put is not None:
            raise self.fail_put
        data = Path(source).read_bytes()
        if not data:
            raise ValueError("objeto vazio não é publicado")
        self.objects[key] = data
        return ObjectMetadata(key=key, size=len(data))

    def delete(self, key: str) -> None:
        self.delete_calls.append(key)
        if self.fail_delete is not None:
            raise self.fail_delete
        self.objects.pop(key, None)


def make_zip(members: dict[str, bytes]) -> bytes:
    """Build an in-memory ZIP with the given member names and payloads."""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def read_parquet(data: bytes) -> pa.Table:
    """Read a Parquet payload from memory, as Bronze consumers would."""

    return pq.read_table(pa.BufferReader(data))
