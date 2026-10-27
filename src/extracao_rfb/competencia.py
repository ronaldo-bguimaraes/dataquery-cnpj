from __future__ import annotations

import re

_COMPETENCIA_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class InvalidCompetenciaError(ValueError):
    """Raised when the competência is not a valid ``AAAA-MM`` value."""


class InvalidLimitError(ValueError):
    """Raised when the optional file limit is not a positive integer."""


def validate_competencia(value: str | None) -> str:
    """Validate and normalize a competência (``AAAA-MM``, month 01-12)."""

    if not isinstance(value, str):
        raise InvalidCompetenciaError(
            "competência inválida: informe o formato AAAA-MM (mês de 01 a 12)"
        )

    normalized = value.strip()
    if not _COMPETENCIA_PATTERN.fullmatch(normalized):
        raise InvalidCompetenciaError(
            f"competência inválida: {normalized!r}; informe o formato AAAA-MM (mês de 01 a 12)"
        )

    return normalized


def validate_limit(value: int | None) -> int | None:
    """Validate the optional file limit (``None`` or an integer ``>= 1``)."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise InvalidLimitError("limite inválido: informe um inteiro maior ou igual a 1")
    return value


def select_files(paths, limite: int | None) -> list[str]:
    """Return the first ``limite`` names in stable lexicographic order.

    Selection happens before any existence check: a file already present on disk
    does not free a slot for the next name.
    """

    limite = validate_limit(limite)
    ordered = sorted(paths)
    if limite is None:
        return ordered
    return ordered[:limite]
