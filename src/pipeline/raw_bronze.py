"""Pure orchestration helpers shared by the DAG and the local E2E tests.

The module does no I/O at import time. It combines per-file Raw and Bronze results
into a small, JSON-serializable summary and raises once at the end when any file
failed, so independent mapped instances can still finish.
"""

from __future__ import annotations

import time
from typing import Any, Iterable

from src.conversao.bronze import BronzeFileResult, convert_raw_file
from src.download_utils.download_files import DEFAULT_CHUNK_SIZE
from src.extracao_rfb.download_raw import (
    RawFileResult,
    RawListing,
    download_raw_file,
    download_raw_file_async,
    listar_arquivos,
)
from src.storage.protocol import ObjectStore

DOWNLOAD_OK = {"baixado", "reutilizado"}


class PipelineFailedError(RuntimeError):
    """Raised at the end of a run when at least one file failed."""

    def __init__(self, message: str, summary: dict[str, Any] | None = None) -> None:
        self.summary = summary
        super().__init__(message)


def _as_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return dict(value)


def listar_arquivos_payload(
    competencia: str,
    *,
    limite: int | None = None,
    store: ObjectStore | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    return listar_arquivos(
        competencia, limite=limite, store=store, client=client
    ).to_dict()


def _combined(
    nome: str,
    download: dict[str, Any],
    conversion: dict[str, Any],
) -> dict[str, Any]:
    download_status = download.get("status_download", "falhou")
    download_failed = download_status not in DOWNLOAD_OK
    conversion_status = conversion.get("status_conversao", "falhou")

    if download_failed:
        conversion_status = "não executado"
        bronze_key = None
        bytes_parquet = 0
        linhas = None
        raw_key = None
    else:
        bronze_key = conversion.get("bronze_key")
        bytes_parquet = int(conversion.get("bytes", 0) or 0)
        linhas = conversion.get("linhas")
        raw_key = download.get("raw_key") or conversion.get("raw_key")

    return {
        "nome": nome,
        "status_download": download_status,
        "status_conversao": conversion_status,
        "bytes": int(download.get("bytes", 0) or 0),
        "bytes_parquet": bytes_parquet,
        "linhas": linhas,
        "raw_key": raw_key,
        "bronze_key": bronze_key,
        "duracao_segundos": float(
            download.get("duracao_segundos", 0.0) or 0.0
        )
        + float(conversion.get("duracao_segundos", 0.0) or 0.0),
        "erro": download.get("erro") or conversion.get("erro"),
    }


def combinar_resultados(
    downloads: Iterable[Any] | None,
    conversoes: Iterable[Any] | None,
) -> list[dict[str, Any]]:
    """Merge download and conversion payloads by file name.

    Missing results are treated as failures instead of silently disappearing.
    """

    download_map = {item["nome"]: item for item in map(_as_dict, downloads or [])}
    conversion_map = {item["nome"]: item for item in map(_as_dict, conversoes or [])}

    names = sorted(set(download_map) | set(conversion_map))
    combined: list[dict[str, Any]] = []
    for nome in names:
        download = download_map.get(nome)
        conversion = conversion_map.get(nome)

        if download is None:
            download = {
                "nome": nome,
                "status_download": "falhou",
                "raw_key": None,
                "bytes": 0,
                "duracao_segundos": 0.0,
                "erro": f"resultado de download ausente para {nome}",
            }
        if conversion is None:
            conversion = {
                "nome": nome,
                "status_conversao": "falhou",
                "raw_key": download.get("raw_key"),
                "bronze_key": None,
                "bytes": 0,
                "linhas": None,
                "duracao_segundos": 0.0,
                "erro": f"resultado de conversão ausente para {nome}",
            }

        combined.append(_combined(nome, download, conversion))
    return combined


def _sync_one(
    competencia: str,
    nome: str,
    download: RawFileResult,
    *,
    store: ObjectStore,
    scratch_dir: str | None,
    chunk_size: int | None,
) -> dict[str, Any]:
    if download.status not in DOWNLOAD_OK:
        return {
            "nome": nome,
            "status_download": download.status,
            "status_conversao": "não executado",
            "bytes": download.bytes_baixados,
            "bytes_parquet": 0,
            "linhas": None,
            "raw_key": download.raw_key,
            "bronze_key": None,
            "duracao_segundos": download.duracao_segundos,
            "erro": download.erro or "download falhou",
        }

    conversion: BronzeFileResult = convert_raw_file(
        competencia,
        nome,
        download.raw_key,
        store=store,
        scratch_dir=scratch_dir,
        chunk_size=chunk_size,
    )
    return _combined(nome, download.to_dict(), conversion.to_dict())


def sincronizar_arquivo(
    competencia: str,
    nome: str,
    *,
    store: ObjectStore,
    client: Any | None = None,
    scratch_dir: str | None = None,
    chunk_size: int | None = None,
) -> dict[str, Any]:
    """Download one file and, only on success, convert the same file."""

    effective_chunk = (
        chunk_size if chunk_size is not None else DEFAULT_CHUNK_SIZE
    )
    download = download_raw_file(
        competencia,
        nome,
        store=store,
        client=client,
        scratch_dir=scratch_dir,
        chunk_size=effective_chunk,
    )
    return _sync_one(
        competencia,
        nome,
        download,
        store=store,
        scratch_dir=scratch_dir,
        chunk_size=chunk_size,
    )


async def sincronizar_arquivo_async(
    competencia: str,
    nome: str,
    *,
    store: ObjectStore,
    client: Any | None = None,
    scratch_dir: str | None = None,
    chunk_size: int | None = None,
) -> dict[str, Any]:
    """Async counterpart of :func:`sincronizar_arquivo`."""

    effective_chunk = (
        chunk_size if chunk_size is not None else DEFAULT_CHUNK_SIZE
    )
    download = await download_raw_file_async(
        competencia,
        nome,
        store=store,
        client=client,
        scratch_dir=scratch_dir,
        chunk_size=effective_chunk,
    )
    return _sync_one(
        competencia,
        nome,
        download,
        store=store,
        scratch_dir=scratch_dir,
        chunk_size=chunk_size,
    )


def consolidar_resumo(
    listing: dict[str, Any],
    resultados: Iterable[dict[str, Any]],
    *,
    duracao_segundos: float | None = None,
) -> dict[str, Any]:
    """Build the run summary from the listing and per-file results."""

    arquivos = sorted(list(resultados), key=lambda item: item["nome"])
    bytes_download = sum(int(item.get("bytes", 0) or 0) for item in arquivos)
    bytes_parquet = sum(
        int(item.get("bytes_parquet", 0) or 0) for item in arquivos
    )
    linhas = sum(
        int(item["linhas"])
        for item in arquivos
        if item.get("linhas") is not None
    )

    totais = {
        "baixados": sum(
            1 for item in arquivos if item.get("status_download") == "baixado"
        ),
        "reutilizados": sum(
            1
            for item in arquivos
            if item.get("status_download") == "reutilizado"
        ),
        "falhas_download": sum(
            1 for item in arquivos if item.get("status_download") == "falhou"
        ),
        "publicados": sum(
            1
            for item in arquivos
            if item.get("status_conversao") == "publicado"
        ),
        "falhas_conversao": sum(
            1 for item in arquivos if item.get("status_conversao") == "falhou"
        ),
        "nao_executados": sum(
            1
            for item in arquivos
            if item.get("status_conversao") == "não executado"
        ),
        "bytes": bytes_download,
        "bytes_parquet": bytes_parquet,
        "linhas": linhas,
    }
    falhas = totais["falhas_download"] + totais["falhas_conversao"]

    if duracao_segundos is None:
        duracao_segundos = sum(
            float(item.get("duracao_segundos", 0.0) or 0.0)
            for item in arquivos
        )

    return {
        "competencia": listing.get("competencia"),
        "encontrados": int(listing.get("encontrados", 0)),
        "selecionados": int(listing.get("selecionados", 0)),
        "limite": listing.get("limite"),
        "parcial": bool(listing.get("parcial", False)),
        "arquivos": arquivos,
        "totais": totais,
        "falhas": falhas,
        "duracao_segundos": max(0.0, float(duracao_segundos)),
    }


def raise_if_failed(summary: dict[str, Any]) -> None:
    """Raise once when at least one file failed, preserving the summary."""

    if int(summary.get("falhas", 0)) <= 0:
        return

    names = [
        item["nome"]
        for item in summary.get("arquivos", [])
        if item.get("status_download") == "falhou"
        or item.get("status_conversao") == "falhou"
    ]
    detail = ", ".join(names) if names else "arquivo desconhecido"
    raise PipelineFailedError(
        f"{summary.get('falhas', 0)} arquivo(s) falharam na competência "
        f"{summary.get('competencia')}: {detail}",
        summary=summary,
    )


def executar_competencia(
    competencia: str,
    *,
    limite: int | None = None,
    store: ObjectStore | None = None,
    client: Any | None = None,
    scratch_dir: str | None = None,
) -> dict[str, Any]:
    """Run listing + per-file pipeline locally (used by tests and operators)."""

    if store is None:
        from src.storage.factory import build_store_from_env

        store = build_store_from_env()
    listing: RawListing = listar_arquivos(
        competencia, limite=limite, store=store, client=client
    )
    started = time.monotonic()
    resultados = [
        sincronizar_arquivo(
            competencia,
            nome,
            store=store,
            client=client,
            scratch_dir=scratch_dir,
        )
        for nome in listing.nomes
    ]
    summary = consolidar_resumo(
        listing.to_dict(),
        resultados,
        duracao_segundos=time.monotonic() - started,
    )
    raise_if_failed(summary)
    return summary


async def executar_competencia_async(
    competencia: str,
    *,
    limite: int | None = None,
    store: ObjectStore | None = None,
    client: Any | None = None,
    scratch_dir: str | None = None,
) -> dict[str, Any]:
    """Async counterpart of :func:`executar_competencia`."""

    if store is None:
        from src.storage.factory import build_store_from_env

        store = build_store_from_env()
    listing: RawListing = listar_arquivos(
        competencia, limite=limite, store=store, client=client
    )
    started = time.monotonic()
    resultados = [
        await sincronizar_arquivo_async(
            competencia,
            nome,
            store=store,
            client=client,
            scratch_dir=scratch_dir,
        )
        for nome in listing.nomes
    ]
    summary = consolidar_resumo(
        listing.to_dict(),
        resultados,
        duracao_segundos=time.monotonic() - started,
    )
    raise_if_failed(summary)
    return summary
