"""Local filesystem backend for the storage protocol.

Keys are logical (``raw/<AAAA-MM>/<nome>``, ``bronze/<AAAA-MM>/<nome>.parquet``)
and resolve under ``CNPJ_DATA_ROOT`` (``/data`` inside the task containers). Writes
go to a sibling temporary file and become visible only after ``os.replace``.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

from src.storage.protocol import (
    ObjectMetadata,
    StorageConfigError,
    StorageOperationError,
    validate_key,
)

_DEFAULT_CHUNK_SIZE = 1024 * 1024


def _copy_file(source: Path, destination: Path, *, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> int:
    """Stream ``source`` into ``destination`` and return the written size."""

    written = 0
    with source.open("rb") as reader, destination.open("wb") as writer:
        while True:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            writer.write(chunk)
            written += len(chunk)
        writer.flush()
        os.fsync(writer.fileno())
    return written


class FilesystemObjectStore:
    """Object store backed by a local directory (default ``/data``)."""

    def __init__(self, root: str | os.PathLike[str] | None = None) -> None:
        configured = root or os.getenv("CNPJ_DATA_ROOT")
        if not configured:
            raise StorageConfigError("CNPJ_DATA_ROOT não configurado")
        self._root = Path(configured).resolve()

    @property
    def root(self) -> Path:
        return self._root

    def __repr__(self) -> str:
        return f"FilesystemObjectStore(root={str(self._root)!r})"

    def _path_for(self, key: str) -> Path:
        validate_key(key)
        candidate = (self._root / key).resolve()
        try:
            candidate.relative_to(self._root)
        except ValueError:
            raise ValueError(f"chave fora da raiz de armazenamento: {key!r}") from None
        return candidate

    def ping(self) -> None:
        if not self._root.exists():
            raise StorageOperationError(
                f"raiz de armazenamento não existe: {self._root}"
            )
        if not self._root.is_dir():
            raise StorageOperationError(
                f"raiz de armazenamento não é diretório: {self._root}"
            )
        if not os.access(self._root, os.W_OK):
            raise StorageOperationError(
                f"raiz de armazenamento sem permissão de escrita: {self._root}"
            )

    def head(self, key: str) -> ObjectMetadata | None:
        path = self._path_for(key)
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise StorageOperationError(
                f"leitura de metadados falhou {key}: {exc.strerror or exc}"
            ) from None
        if not path.is_file():
            return None
        return ObjectMetadata(key=key, size=stat.st_size)

    def get_to_path(self, key: str, dest: Path) -> ObjectMetadata:
        source = self._path_for(key)
        target = Path(dest)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.part")
        try:
            size = _copy_file(source, temporary)
            os.replace(temporary, target)
        except FileNotFoundError:
            raise StorageOperationError(f"objeto ausente: {key}") from None
        except OSError as exc:
            raise StorageOperationError(
                f"download do objeto falhou {key}: {exc.strerror or exc}"
            ) from None
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return ObjectMetadata(key=key, size=size)

    def put_atomic(self, key: str, source: Path) -> ObjectMetadata:
        path = self._path_for(key)
        source_path = Path(source)
        try:
            size = source_path.stat().st_size
        except OSError as exc:
            raise StorageOperationError(
                f"leitura do arquivo local falhou {key}: {exc.strerror or exc}"
            ) from None
        if size <= 0:
            raise ValueError("objeto vazio não é publicado")

        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
        try:
            written = _copy_file(source_path, temporary)
            os.replace(temporary, path)
        except OSError as exc:
            raise StorageOperationError(
                f"upload do objeto falhou {key}: {exc.strerror or exc}"
            ) from None
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return ObjectMetadata(key=key, size=written)

    def delete(self, key: str) -> None:
        path = self._path_for(key)
        try:
            path.unlink()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise StorageOperationError(
                f"remoção do objeto falhou {key}: {exc.strerror or exc}"
            ) from None
