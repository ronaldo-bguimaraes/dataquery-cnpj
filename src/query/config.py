"""Runtime configuration for the query portal."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from src.query.domain.normalization import competence_end_date


class ConfigError(ValueError):
    """Raised when portal configuration is invalid."""


def validate_competencia(value: str) -> str:
    text = (value or "").strip()
    try:
        year_text, month_text = text.split("-", 1)
        year, month = int(year_text), int(month_text)
        if len(year_text) != 4 or not 1 <= month <= 12:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise ConfigError(f"competência inválida: {value!r}") from None
    competence_end_date(text)
    return text


def _positive_int(value: str | None, default: int, name: str) -> int:
    if value is None or not str(value).strip():
        return default
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ConfigError(f"{name} deve ser um inteiro >= 1") from None
    if number < 1 or number > 100:
        raise ConfigError(f"{name} deve estar entre 1 e 100")
    return number


@dataclass(frozen=True)
class QueryConfig:
    competencia: str = "2026-10"
    database_url: str = field(default="", repr=False)
    fonte: str = "Receita Federal"
    page_size: int = 25
    data_root: Path = field(default_factory=lambda: Path("/data"))
    storage_backend: str = "filesystem"

    def __post_init__(self) -> None:
        object.__setattr__(self, "competencia", validate_competencia(self.competencia))
        if self.page_size < 1 or self.page_size > 100:
            raise ConfigError("page_size deve estar entre 1 e 100")

    @property
    def source_dir(self) -> Path:
        return self.data_root / "bronze" / self.competencia

    @property
    def end_date(self):
        return competence_end_date(self.competencia)

    @classmethod
    def from_env(cls, *, require_database: bool = True) -> "QueryConfig":
        database_url = os.getenv("CNPJ_QUERY_DATABASE_URL", "").strip()
        if require_database and not database_url:
            raise ConfigError("CNPJ_QUERY_DATABASE_URL não configurada")
        return cls(
            competencia=os.getenv("CNPJ_QUERY_COMPETENCIA", "2026-10"),
            database_url=database_url,
            fonte=os.getenv("CNPJ_QUERY_SOURCE", "Receita Federal"),
            page_size=_positive_int(
                os.getenv("CNPJ_QUERY_PAGE_SIZE"), 25, "CNPJ_QUERY_PAGE_SIZE"
            ),
            data_root=Path(os.getenv("CNPJ_DATA_ROOT", "/data")),
            storage_backend=os.getenv("CNPJ_STORAGE_BACKEND", "filesystem"),
        )
