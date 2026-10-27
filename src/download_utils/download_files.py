from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import aiohttp

from src.download_utils.redaction import describe_exception

FileStatus = Literal["baixado", "ignorado", "falhou"]

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_CONCURRENCY = 3


@dataclass(frozen=True)
class DownloadTarget:
    """One file to download. ``url`` is hidden from ``repr`` on purpose."""

    nome: str
    url: str = field(repr=False)


@dataclass(frozen=True)
class FileResult:
    nome: str
    status: FileStatus
    bytes: int = 0
    erro: str | None = None


@dataclass(frozen=True)
class DownloadReport:
    resultados: tuple[FileResult, ...]
    baixados: int
    ignorados: int
    falhas: int
    bytes_baixados: int
    duracao_segundos: float


def _require_positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} deve ser um inteiro >= 1")


def _valid_name(name: str) -> bool:
    return bool(name) and name not in {".", ".."} and "/" not in name and "\\" not in name


def _temporary_path(final_path: Path) -> Path:
    return final_path.with_name(f".{final_path.name}.{os.getpid()}.{uuid.uuid4().hex}.part")


async def _download_one(
    session: aiohttp.ClientSession,
    target: DownloadTarget,
    output_dir: Path,
    chunk_size: int,
) -> FileResult:
    final_path = output_dir / target.nome

    if final_path.is_file() and final_path.stat().st_size > 0:
        return FileResult(nome=target.nome, status="ignorado")

    temporary_path = _temporary_path(final_path)
    written = 0
    expected: int | None = None

    try:
        async with session.get(target.url) as response:
            response.raise_for_status()

            if not response.headers.get("Content-Encoding"):
                expected = response.content_length

            with open(temporary_path, "wb") as handle:
                async for chunk in response.content.iter_chunked(chunk_size):
                    handle.write(chunk)
                    written += len(chunk)

        if expected is not None and written != expected:
            raise OSError(f"tamanho incompleto: {written} de {expected} bytes")

        os.replace(temporary_path, final_path)
        return FileResult(nome=target.nome, status="baixado", bytes=written)
    except Exception as exc:  # noqa: BLE001 - each file fails independently
        return FileResult(
            nome=target.nome,
            status="falhou",
            erro=describe_exception(exc),
        )
    finally:
        if temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError:
                pass


async def download_files(
    targets: list[DownloadTarget] | tuple[DownloadTarget, ...],
    output_dir: str | os.PathLike[str],
    *,
    concorrencia: int = DEFAULT_CONCURRENCY,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> DownloadReport:
    """Download ``targets`` into ``output_dir``, isolating per-file failures.

    The function never raises for an individual file failure: the returned report carries
    ``falhou`` results so the caller can decide how to surface the failure.
    """

    _require_positive("concorrencia", concorrencia)
    _require_positive("chunk_size", chunk_size)

    for target in targets:
        if not _valid_name(target.nome):
            raise ValueError(f"nome de arquivo inválido: {target.nome!r}")

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    semaphore = asyncio.Semaphore(concorrencia)

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:

        async def run(target: DownloadTarget) -> FileResult:
            async with semaphore:
                return await _download_one(session, target, directory, chunk_size)

        if targets:
            resultados = await asyncio.gather(*(run(target) for target in targets))
        else:
            resultados = ()

    ordered = tuple(sorted(resultados, key=lambda result: result.nome))
    baixados = sum(1 for result in ordered if result.status == "baixado")
    ignorados = sum(1 for result in ordered if result.status == "ignorado")
    falhas = sum(1 for result in ordered if result.status == "falhou")

    return DownloadReport(
        resultados=ordered,
        baixados=baixados,
        ignorados=ignorados,
        falhas=falhas,
        bytes_baixados=sum(result.bytes for result in ordered if result.status == "baixado"),
        duracao_segundos=time.monotonic() - started,
    )
