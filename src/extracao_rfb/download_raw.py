"""List, select and download a single Raw ZIP from the RFB.

Each call handles exactly one file. Airflow owns the fan-out and retries, so this
module never downloads several files in parallel. Raw objects are preserved by
competence and reused when they already exist with a positive size.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from src.download_utils.download_files import (
    DEFAULT_CHUNK_SIZE,
    DownloadTarget,
    download_files,
)
from src.download_utils.redaction import describe_exception
from src.extracao_rfb.competencia import (
    select_files,
    validate_competencia,
    validate_limit,
)
from src.extracao_rfb.download_competencia import EmptyCompetenciaError
from src.extracao_rfb.rfb_client import ClientRFB
from src.storage.protocol import ObjectStore

DownloadStatus = Literal["baixado", "reutilizado", "falhou"]


@dataclass(frozen=True)
class RawListing:
    competencia: str
    encontrados: int
    selecionados: int
    limite: int | None
    parcial: bool
    nomes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "competencia": self.competencia,
            "encontrados": self.encontrados,
            "selecionados": self.selecionados,
            "limite": self.limite,
            "parcial": self.parcial,
            "arquivos": [{"nome": nome} for nome in self.nomes],
        }


@dataclass(frozen=True)
class RawFileResult:
    nome: str
    raw_key: str | None
    status: DownloadStatus
    bytes_baixados: int = 0
    duracao_segundos: float = 0.0
    erro: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "nome": self.nome,
            "raw_key": self.raw_key,
            "status_download": self.status,
            "bytes": self.bytes_baixados,
            "duracao_segundos": self.duracao_segundos,
            "erro": self.erro,
        }


def _valid_name(name: str) -> bool:
    return (
        bool(name)
        and name not in {".", ".."}
        and "/" not in name
        and "\\" not in name
    )


def raw_key_for(competencia: str, nome: str) -> str:
    """Return the deterministic Raw key for an original ZIP name."""

    return f"raw/{competencia}/{nome}"


def listar_arquivos(
    competencia: str,
    *,
    limite: int | None = None,
    store: ObjectStore | None = None,
    client: ClientRFB | None = None,
) -> RawListing:
    """List, filter, sort and select the ZIPs of ``competencia``.

    Validation happens before network or object-store access. The limit is applied
    to the sorted list before any Raw existence check, so an existing object counts
    inside the limit and never frees a slot.
    """

    competencia = validate_competencia(competencia)
    limite = validate_limit(limite)

    if store is not None:
        store.ping()

    client = client or ClientRFB()
    files = client.list_files(competencia)
    names = sorted({name for name in files if _valid_name(name)})
    if not names:
        raise EmptyCompetenciaError(
            f"competência {competencia} não possui arquivos publicados"
        )

    selected = select_files(names, limite)
    return RawListing(
        competencia=competencia,
        encontrados=len(names),
        selecionados=len(selected),
        limite=limite,
        parcial=limite is not None and limite < len(names),
        nomes=tuple(selected),
    )


async def download_raw_file_async(
    competencia: str,
    nome: str,
    *,
    store: ObjectStore,
    client: ClientRFB | None = None,
    scratch_dir: str | Path | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> RawFileResult:
    """Download one ZIP to ``raw/<competencia>/<nome>`` or reuse it."""

    competencia = validate_competencia(competencia)
    if not _valid_name(nome):
        raise ValueError(f"nome de arquivo inválido: {nome!r}")

    started = time.monotonic()
    raw_key = raw_key_for(competencia, nome)

    try:
        store.ping()
        metadata = store.head(raw_key)
        if metadata is not None and metadata.size > 0:
            return RawFileResult(
                nome=nome,
                raw_key=raw_key,
                status="reutilizado",
                bytes_baixados=0,
                duracao_segundos=max(0.0, time.monotonic() - started),
            )

        client = client or ClientRFB()
        remote_path = (
            f"{client.base_path.rstrip('/')}/{competencia}/{nome}"
        )
        url = client.get_file_url(remote_path)

        with tempfile.TemporaryDirectory(
            prefix="cnpj-raw-", dir=scratch_dir
        ) as scratch:
            report = await download_files(
                [DownloadTarget(nome=nome, url=url)],
                scratch,
                concorrencia=1,
                chunk_size=chunk_size,
            )
            result = report.resultados[0]
            if result.status == "falhou":
                return RawFileResult(
                    nome=nome,
                    raw_key=None,
                    status="falhou",
                    bytes_baixados=0,
                    duracao_segundos=max(0.0, time.monotonic() - started),
                    erro=result.erro,
                )

            local_path = Path(scratch) / nome
            published = store.put_atomic(raw_key, local_path)
            return RawFileResult(
                nome=nome,
                raw_key=raw_key,
                status="baixado",
                bytes_baixados=max(result.bytes, published.size),
                duracao_segundos=max(0.0, time.monotonic() - started),
            )
    except Exception as exc:  # noqa: BLE001 - one file must not stop the others
        return RawFileResult(
            nome=nome,
            raw_key=None,
            status="falhou",
            bytes_baixados=0,
            duracao_segundos=max(0.0, time.monotonic() - started),
            erro=describe_exception(exc),
        )


def download_raw_file(
    competencia: str,
    nome: str,
    *,
    store: ObjectStore,
    client: ClientRFB | None = None,
    scratch_dir: str | Path | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> RawFileResult:
    """Synchronous wrapper used by Airflow task callables."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            download_raw_file_async(
                competencia,
                nome,
                store=store,
                client=client,
                scratch_dir=scratch_dir,
                chunk_size=chunk_size,
            )
        )
    raise RuntimeError(
        "download_raw_file não pode ser chamado dentro de um event loop; "
        "use download_raw_file_async"
    )
