"""Orquestra o download de todos os arquivos de uma competência da RFB.

O módulo não executa trabalho no import: toda a I/O acontece quando
``download_competencia`` (ou sua versão assíncrona) é chamada. URLs e tokens nunca
aparecem em log, resumo ou mensagem de erro.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.download_utils.download_files import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CONCURRENCY,
    FileResult,
    DownloadTarget,
    download_files,
)
from src.download_utils.redaction import describe_exception
from src.extracao_rfb.competencia import select_files, validate_competencia, validate_limit
from src.extracao_rfb.rfb_client import ClientRFB

log = logging.getLogger(__name__)


class OutputDirectoryError(RuntimeError):
    """Raised when the output directory cannot be created or written to."""


class EmptyCompetenciaError(RuntimeError):
    """Raised when the competência has no files to download."""


class ExecutionInProgressError(RuntimeError):
    """Raised when another run already holds the competência lock."""


class DownloadError(RuntimeError):
    """Raised when at least one selected file failed to download.

    The complete summary is preserved in ``summary`` so callers can inspect the
    successful files and publish the failure details.
    """

    def __init__(self, summary: "DownloadSummary") -> None:
        self.summary = summary
        failed = [result.nome for result in summary.arquivos if result.status == "falhou"]
        nomes = ", ".join(failed)
        super().__init__(
            f"{summary.falhas} arquivo(s) falharam na competência "
            f"{summary.competencia}: {nomes}"
        )


@dataclass(frozen=True)
class DownloadSummary:
    """Observable result of a competência download."""

    competencia: str
    output_dir: str
    encontrados: int
    selecionados: int
    baixados: int
    ignorados: int
    falhas: int
    bytes_baixados: int
    duracao_segundos: float
    parcial: bool
    arquivos: tuple[FileResult, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation (no Paths, no URLs)."""

        return {
            "competencia": self.competencia,
            "output_dir": self.output_dir,
            "encontrados": self.encontrados,
            "selecionados": self.selecionados,
            "baixados": self.baixados,
            "ignorados": self.ignorados,
            "falhas": self.falhas,
            "bytes_baixados": self.bytes_baixados,
            "duracao_segundos": self.duracao_segundos,
            "parcial": self.parcial,
            "arquivos": [
                {
                    "nome": result.nome,
                    "status": result.status,
                    "bytes": result.bytes,
                    "erro": result.erro,
                }
                for result in self.arquivos
            ],
        }


def _prepare_output_dir(output_dir: str | os.PathLike[str], competencia: str) -> Path:
    """Create and smoke-test ``<output_dir>/<competencia>`` before any network I/O."""

    directory = Path(output_dir) / competencia
    probe = directory / f".write-test-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe.write_bytes(b"")
        probe.unlink()
    except OSError as exc:
        raise OutputDirectoryError(
            f"diretório de saída não gravável: {directory}: {describe_exception(exc)}"
        ) from None
    finally:
        if probe.exists():
            try:
                probe.unlink()
            except OSError:
                pass

    return directory.resolve()


def _file_path(client: ClientRFB, competencia: str, nome: str) -> str:
    return f"{client.base_path.rstrip('/')}/{competencia}/{nome}"


class _CompetenciaLock:
    """Exclusive, non-blocking ``flock`` with a bounded wait."""

    def __init__(self, path: Path, timeout: float) -> None:
        self._path = path
        self._timeout = timeout
        self._handle = None

    def _acquire(self):
        handle = self._path.open("a+")
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    handle.close()
                    raise ExecutionInProgressError(
                        "já existe uma execução em andamento para a competência "
                        f"{self._path.parent.name}"
                    ) from None
                time.sleep(0.05)
            else:
                return handle

    async def __aenter__(self) -> "_CompetenciaLock":
        self._handle = await asyncio.to_thread(self._acquire)
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        handle, self._handle = self._handle, None
        if handle is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()
        return False


def _remove_stale_parts(directory: Path) -> None:
    """Remove temporary files left behind by an interrupted execution."""

    seen: set[Path] = set()
    for pattern in ("*.part", ".*.part"):
        for path in directory.glob(pattern):
            if path in seen or not path.is_file():
                continue
            seen.add(path)
            try:
                path.unlink()
            except OSError as exc:
                log.warning(
                    "não foi possível remover temporário %s: %s",
                    path.name,
                    describe_exception(exc),
                )


def _build_summary(
    competencia: str,
    output_dir: Path,
    encontrados: int,
    selecionados: int,
    parcial: bool,
    report,
    duracao_segundos: float,
) -> DownloadSummary:
    return DownloadSummary(
        competencia=competencia,
        output_dir=str(output_dir),
        encontrados=encontrados,
        selecionados=selecionados,
        baixados=report.baixados,
        ignorados=report.ignorados,
        falhas=report.falhas,
        bytes_baixados=report.bytes_baixados,
        duracao_segundos=duracao_segundos,
        parcial=parcial,
        arquivos=report.resultados,
    )


async def _download_locked(
    competencia: str,
    output: Path,
    limite: int | None,
    client: ClientRFB,
    concorrencia: int,
    chunk_size: int,
) -> DownloadSummary:
    started = time.monotonic()
    log.info(
        "Iniciando download da competência %s (limite=%s) em %s",
        competencia,
        limite,
        output,
    )

    files = client.list_files(competencia)
    if not files:
        raise EmptyCompetenciaError(
            f"competência {competencia} não possui arquivos publicados"
        )

    selected = select_files(files, limite)
    targets = [
        DownloadTarget(
            nome=nome,
            url=client.get_file_url(_file_path(client, competencia, nome)),
        )
        for nome in selected
    ]

    report = await download_files(
        targets,
        output,
        concorrencia=concorrencia,
        chunk_size=chunk_size,
    )

    for result in report.resultados:
        if result.status == "falhou":
            log.warning(
                "Arquivo %s falhou na competência %s: %s",
                result.nome,
                competencia,
                result.erro,
            )
        else:
            log.info(
                "Arquivo %s %s (%d bytes) na competência %s",
                result.nome,
                result.status,
                result.bytes,
                competencia,
            )

    summary = _build_summary(
        competencia=competencia,
        output_dir=output,
        encontrados=len(files),
        selecionados=len(targets),
        parcial=limite is not None and limite < len(files),
        report=report,
        duracao_segundos=max(0.0, time.monotonic() - started),
    )

    log.info("Resumo da competência %s: %s", competencia, json.dumps(summary.to_dict()))

    if summary.falhas:
        raise DownloadError(summary)

    return summary


async def download_competencia_async(
    competencia: str,
    output_dir: str | os.PathLike[str],
    *,
    limite: int | None = None,
    concorrencia: int = DEFAULT_CONCURRENCY,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    client: ClientRFB | None = None,
    lock_timeout: float = 5.0,
) -> DownloadSummary:
    """Download every file of ``competencia`` into ``<output_dir>/<AAAA-MM>``.

    Raises ``InvalidCompetenciaError``, ``OutputDirectoryError``,
    ``ListingError``, ``EmptyCompetenciaError``, ``ExecutionInProgressError`` or
    ``DownloadError``. Individual download failures do not prevent the other files
    from being written.
    """

    competencia = validate_competencia(competencia)
    limite = validate_limit(limite)
    if lock_timeout < 0:
        raise ValueError("lock_timeout deve ser >= 0")

    client = client or ClientRFB()
    output = _prepare_output_dir(output_dir, competencia)

    async with _CompetenciaLock(output / ".download.lock", lock_timeout):
        _remove_stale_parts(output)
        return await _download_locked(
            competencia,
            output,
            limite,
            client,
            concorrencia,
            chunk_size,
        )


def download_competencia(
    competencia: str,
    output_dir: str | os.PathLike[str],
    *,
    limite: int | None = None,
    concorrencia: int = DEFAULT_CONCURRENCY,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    client: ClientRFB | None = None,
    lock_timeout: float = 5.0,
) -> DownloadSummary:
    """Synchronous wrapper around :func:`download_competencia_async`."""

    return asyncio.run(
        download_competencia_async(
            competencia,
            output_dir,
            limite=limite,
            concorrencia=concorrencia,
            chunk_size=chunk_size,
            client=client,
            lock_timeout=lock_timeout,
        )
    )
