"""Convert one Raw ZIP into one Bronze Parquet object.

The function is deliberately stateless: it receives the competence, the original
name, the Raw key and the object store. It downloads the ZIP to a local scratch
directory, converts exactly one CSV member with the central schema, uploads the
Parquet and removes the scratch directory on success or failure.
"""

from __future__ import annotations

import tempfile
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from conversor import convert_csv_to_parquet
from read_schema import test_regex
from src.download_utils.redaction import describe_exception
from src.extracao_rfb.competencia import validate_competencia
from src.storage.protocol import ObjectStore, StorageOperationError
from utils import get_available_memory

DEFAULT_CHUNK_SIZE = 1024 * 1024
MAX_CHUNK_SIZE = 64 * 1024 * 1024

BronzeStatus = Literal["publicado", "falhou"]


class ConversionError(RuntimeError):
    """Raised for a ZIP/schema/CSV problem that prevents publishing Bronze."""


@dataclass(frozen=True)
class BronzeFileResult:
    nome: str
    raw_key: str
    bronze_key: str
    status: BronzeStatus
    bytes_parquet: int = 0
    linhas: int | None = None
    duracao_segundos: float = 0.0
    erro: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "nome": self.nome,
            "raw_key": self.raw_key,
            "bronze_key": self.bronze_key,
            "status_conversao": self.status,
            "bytes": self.bytes_parquet,
            "linhas": self.linhas,
            "duracao_segundos": self.duracao_segundos,
            "erro": self.erro,
        }


def bronze_key_for(competencia: str, nome: str) -> str:
    """Return the deterministic Bronze key for a Raw ZIP name."""

    return f"bronze/{competencia}/{Path(nome).stem}.parquet"


def _valid_name(name: str) -> bool:
    return (
        bool(name)
        and name not in {".", ".."}
        and "/" not in name
        and "\\" not in name
    )


def _default_chunk_size() -> int:
    try:
        available = int(get_available_memory() * 0.05)
    except Exception:  # noqa: BLE001 - fall back to a safe constant
        return DEFAULT_CHUNK_SIZE
    return max(DEFAULT_CHUNK_SIZE, min(available, MAX_CHUNK_SIZE))


def _columns_for(nome: str) -> list[str]:
    try:
        info = test_regex(nome)
    except LookupError:
        raise ConversionError(f"schema desconhecido para o arquivo {nome}") from None
    columns = info.get("columns") or []
    if not columns:
        raise ConversionError(f"schema sem colunas para o arquivo {nome}")
    return list(columns)


def _sanitize_conversion_error(exc: BaseException) -> str:
    """Describe only domain errors; never echo CSV values (e.g. CPF).

    PyArrow's ``ArrowInvalid`` can quote the offending field, which may contain a
    CPF from the source file. Non-domain exceptions therefore expose only their
    type name.
    """

    if isinstance(exc, (ConversionError, StorageOperationError)):
        return describe_exception(exc)
    return f"falha na conversão CSV para Parquet ({type(exc).__name__})"


def _single_csv_member(archive: zipfile.ZipFile):
    """Return the only file member of ``archive`` or raise ``ConversionError``.

    RFB ZIPs name their CSV member without a ``.csv`` suffix, for example
    ``F.K03200$Z.D61010.CNAECSV`` or ``K3241.K03200Y0.D61010.EMPRECSV``. The
    member name therefore cannot be used to decide whether the payload is CSV;
    the single-member invariant plus the CSV parser are the effective checks.
    """

    members = [info for info in archive.infolist() if not info.is_dir()]
    if len(members) != 1:
        raise ConversionError(
            "ZIP deve conter exatamente um CSV; "
            f"encontrados {len(members)}"
        )
    return members[0]


def convert_raw_file(
    competencia: str,
    nome: str,
    raw_key: str,
    *,
    store: ObjectStore,
    scratch_dir: str | Path | None = None,
    chunk_size: int | None = None,
) -> BronzeFileResult:
    """Convert ``raw_key`` into ``bronze/<competencia>/<stem>.parquet``."""

    competencia = validate_competencia(competencia)
    if not _valid_name(nome):
        raise ValueError(f"nome de arquivo inválido: {nome!r}")
    if not isinstance(raw_key, str) or not raw_key:
        raise ValueError("raw_key deve ser uma chave não vazia")

    chunk_size = _default_chunk_size() if chunk_size is None else chunk_size
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError("chunk_size deve ser um inteiro >= 1")

    bronze_key = bronze_key_for(competencia, nome)
    started = time.monotonic()

    try:
        store.ping()
        columns = _columns_for(nome)

        with tempfile.TemporaryDirectory(
            prefix="cnpj-bronze-", dir=scratch_dir
        ) as scratch:
            scratch_path = Path(scratch)
            zip_path = scratch_path / nome
            store.get_to_path(raw_key, zip_path)

            parquet_path = scratch_path / f"{Path(nome).stem}.parquet"
            try:
                with zipfile.ZipFile(zip_path) as archive:
                    member = _single_csv_member(archive)
                    with archive.open(member) as csv_handle:
                        stats = convert_csv_to_parquet(
                            csv_handle,
                            parquet_path,
                            columns,
                            chunk_size=chunk_size,
                            encoding="latin1",
                            delimiter=";",
                        )
            except zipfile.BadZipFile as exc:
                raise ConversionError(
                    f"ZIP inválido: {describe_exception(exc)}"
                ) from None

            if not parquet_path.is_file() or parquet_path.stat().st_size == 0:
                raise ConversionError("Parquet vazio não é publicado")

            published = store.put_atomic(bronze_key, parquet_path)
            return BronzeFileResult(
                nome=nome,
                raw_key=raw_key,
                bronze_key=bronze_key,
                status="publicado",
                bytes_parquet=published.size,
                linhas=int(stats.get("processed_lines", 0)),
                duracao_segundos=max(0.0, time.monotonic() - started),
            )
    except Exception as exc:  # noqa: BLE001 - one file must not stop the others
        return BronzeFileResult(
            nome=nome,
            raw_key=raw_key,
            bronze_key=bronze_key,
            status="falhou",
            bytes_parquet=0,
            linhas=None,
            duracao_segundos=max(0.0, time.monotonic() - started),
            erro=_sanitize_conversion_error(exc),
        )
