"""Configuração de runtime da ingestão paralela (FR-014, FR-023)."""

from __future__ import annotations

import hashlib
import os
import subprocess
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from src.query.config import validate_competencia

ENV_COMPETENCIA = "CNPJ_INGEST_COMPETENCIA"
ENV_BRONZE_DIR = "CNPJ_INGEST_BRONZE_DIR"
ENV_SERVING_DSN = "CNPJ_INGEST_SERVING_DSN"
ENV_CATALOG_URI = "CNPJ_INGEST_CATALOG_URI"
ENV_CATALOG_NAME = "CNPJ_INGEST_CATALOG_NAME"
ENV_S3_ENDPOINT = "CNPJ_INGEST_S3_ENDPOINT"
ENV_S3_BUCKET = "CNPJ_INGEST_S3_BUCKET"
ENV_S3_ACCESS_KEY = "CNPJ_INGEST_S3_ACCESS_KEY"
ENV_S3_SECRET_KEY = "CNPJ_INGEST_S3_SECRET_KEY"
ENV_MAX_WORKERS = "CNPJ_INGEST_MAX_WORKERS"
ENV_TARGET_FILE_SIZE_BYTES = "CNPJ_INGEST_TARGET_FILE_SIZE_BYTES"
ENV_BATCH_ROWS = "CNPJ_INGEST_BATCH_ROWS"
ENV_CODE_VERSION = "CNPJ_INGEST_CODE_VERSION"

DEFAULT_MAX_WORKERS = 8
DEFAULT_TARGET_FILE_SIZE_BYTES = 2**29  # 512 MB (contracts/lakehouse_silver.md §3)
DEFAULT_BATCH_ROWS = 100_000


class ConfigError(ValueError):
    """Raised when ingest configuration is invalid."""


def derive_load_id(competencia: str, data_version_id: str) -> str:
    """Identidade determinística da carga: `{competencia}-{data_version_id[:8]}`."""

    competencia = validate_competencia(competencia)
    version = (data_version_id or "").strip().lower()
    if len(version) < 8 or not all(c in "0123456789abcdef" for c in version):
        raise ConfigError(f"data_version_id inválido: {data_version_id!r}")
    return f"{competencia}-{version[:8]}"


@lru_cache(maxsize=1)
def code_version() -> str:
    """Git SHA da lógica em execução (configurável por env para reprodutibilidade)."""

    explicit = os.environ.get(ENV_CODE_VERSION, "").strip()
    if explicit:
        return explicit
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


@dataclass(frozen=True)
class IngestConfig:
    """Configuração completa de uma execução de ingestão."""

    competencia: str
    bronze_dir: Path
    serving_dsn: str = field(default="", repr=False)
    catalog_uri: str = ""
    catalog_name: str = "prod"
    s3_endpoint: str = ""
    s3_bucket: str = ""
    max_workers: int = DEFAULT_MAX_WORKERS
    target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES
    batch_rows: int = DEFAULT_BATCH_ROWS

    def __post_init__(self) -> None:
        object.__setattr__(self, "competencia", validate_competencia(self.competencia))
        if not (1 <= self.max_workers <= 128):
            raise ConfigError(f"max_workers deve estar entre 1 e 128: {self.max_workers}")
        if self.target_file_size_bytes < 2**27:
            raise ConfigError("target_file_size_bytes mínimo de 128 MB")

    @property
    def bronze_source_dir(self) -> Path:
        return Path(self.bronze_dir) / self.competencia

    @property
    def code_version_id(self) -> str:
        """Alias compatível: versão do código (git SHA)."""

        return code_version()

    @property
    def code_version(self) -> str:
        return code_version()


def _env_path(env, key: str) -> Path | None:  # noqa: ANN001
    value = env.get(key, "")
    return Path(value) if value else None


def load_config(env=None) -> IngestConfig:  # noqa: ANN001
    env = os.environ if env is None else env
    competencia = env.get(ENV_COMPETENCIA, "")
    if not competencia:
        raise ConfigError(f"{ENV_COMPETENCIA} não definido")
    bronze_dir = env.get(ENV_BRONZE_DIR, "")
    if not bronze_dir:
        raise ConfigError(f"{ENV_BRONZE_DIR} não definido")
    max_workers = int(env.get(ENV_MAX_WORKERS, DEFAULT_MAX_WORKERS))
    return IngestConfig(
        competencia=competencia,
        bronze_dir=Path(bronze_dir),
        serving_dsn=env.get(ENV_SERVING_DSN, ""),
        catalog_uri=env.get(ENV_CATALOG_URI, ""),
        catalog_name=env.get(ENV_CATALOG_NAME, "prod"),
        s3_endpoint=env.get(ENV_S3_ENDPOINT, ""),
        s3_bucket=env.get(ENV_S3_BUCKET, ""),
        max_workers=max_workers,
        target_file_size_bytes=int(
            env.get(ENV_TARGET_FILE_SIZE_BYTES, DEFAULT_TARGET_FILE_SIZE_BYTES)
        ),
        batch_rows=int(env.get(ENV_BATCH_ROWS, DEFAULT_BATCH_ROWS)),
    )


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
