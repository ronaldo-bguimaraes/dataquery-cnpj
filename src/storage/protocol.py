"""Storage protocol shared by the Raw and Bronze layers.

The domain only knows this small protocol. Concrete backends (local filesystem,
S3-compatible object storage such as Cloudflare R2) are selected by configuration
and never leak provider details (bucket, path, credentials) into the pipeline.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

_KEY_RE = re.compile(r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$")
_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


class StorageConfigError(RuntimeError):
    """Raised when the configured backend is missing required configuration."""


class StorageOperationError(RuntimeError):
    """Raised when a storage operation fails; messages never contain secrets."""


@dataclass(frozen=True)
class ObjectMetadata:
    """Small metadata returned by ``head``/``put_atomic``/``get_to_path``."""

    key: str
    size: int
    etag: str | None = None


def validate_key(key: str) -> str:
    """Return ``key`` unchanged when it is a safe logical key.

    Logical keys never carry a URI scheme, absolute path, backslash or ``..``
    segment, so a backend can resolve them without escaping its own root.
    """

    if not isinstance(key, str) or not key:
        raise ValueError("chave de armazenamento deve ser uma string não vazia")
    if "\\" in key or _URI_RE.match(key) or key.startswith("/"):
        raise ValueError(f"chave de armazenamento inválida: {key!r}")
    if not _KEY_RE.match(key):
        raise ValueError(f"chave de armazenamento inválida: {key!r}")
    if any(part in {".", ".."} for part in key.split("/")):
        raise ValueError(f"chave de armazenamento inválida: {key!r}")
    return key


@runtime_checkable
class ObjectStore(Protocol):
    """Minimal storage interface used by download, conversion and tests."""

    def ping(self) -> None:
        """Preflight: root writable or bucket reachable. No secrets in errors."""

    def head(self, key: str) -> ObjectMetadata | None:
        """Return ``None`` when absent; ``size == 0`` is present but not reusable."""

    def get_to_path(self, key: str, dest: Path) -> ObjectMetadata:
        """Copy the object to a local scratch path."""

    def put_atomic(self, key: str, source: Path) -> ObjectMetadata:
        """Publish ``source`` under ``key`` only after the write completes."""

    def delete(self, key: str) -> None:
        """Remove ``key``; deleting a missing key is a no-op."""
