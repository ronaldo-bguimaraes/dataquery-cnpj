"""Storage package: generic protocol, backend factory and concrete backends.

Only the lightweight protocol is imported eagerly. Concrete backends (and the
factory that pulls in boto3 for S3/R2) are imported lazily so domain modules and
DAG parsing never require an S3 client.
"""

from __future__ import annotations

from typing import Any

from src.storage.protocol import (
    ObjectMetadata,
    ObjectStore,
    StorageConfigError,
    StorageOperationError,
    validate_key,
)

__all__ = [
    "FilesystemObjectStore",
    "ObjectMetadata",
    "ObjectStore",
    "S3ObjectStore",
    "StorageConfigError",
    "StorageOperationError",
    "build_store_from_env",
    "validate_key",
]

_LAZY = {
    "FilesystemObjectStore": ("src.storage.filesystem", "FilesystemObjectStore"),
    "S3ObjectStore": ("src.storage.minio", "S3ObjectStore"),
    "build_store_from_env": ("src.storage.factory", "build_store_from_env"),
}


def __getattr__(name: str) -> Any:
    target = _LAZY.get(name)
    if target is None:
        raise AttributeError(f"module 'src.storage' has no attribute {name!r}")
    module_name, attribute = target
    from importlib import import_module

    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value
