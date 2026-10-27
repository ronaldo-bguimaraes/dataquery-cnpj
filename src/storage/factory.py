"""Build the configured storage backend.

The pipeline never instantiates a concrete backend directly; it asks this factory,
which reads the environment once per process/task. ``filesystem`` is the local
default and ``s3`` (alias ``r2``) selects any S3-compatible endpoint, including
Cloudflare R2.
"""

from __future__ import annotations

import os

from src.storage.filesystem import FilesystemObjectStore
from src.storage.protocol import ObjectStore, StorageConfigError
from src.storage.minio import S3ObjectStore

_FILESYSTEM_BACKENDS = {"", "filesystem", "local", "fs"}
_S3_BACKENDS = {"s3", "r2"}


def build_store_from_env() -> ObjectStore:
    """Return the backend selected by ``CNPJ_STORAGE_BACKEND``."""

    backend = (os.getenv("CNPJ_STORAGE_BACKEND") or "filesystem").strip().lower()

    if backend in _FILESYSTEM_BACKENDS:
        return FilesystemObjectStore(root=os.getenv("CNPJ_DATA_ROOT") or "/data")

    if backend in _S3_BACKENDS:
        return S3ObjectStore()

    raise StorageConfigError(
        f"CNPJ_STORAGE_BACKEND inválido: {backend!r}; use 'filesystem' ou 's3'"
    )
