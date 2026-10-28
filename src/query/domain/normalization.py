"""Normalization helpers for the CNPJ query layer."""

from __future__ import annotations

import calendar
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def normalize_cnpj(value: Any) -> str:
    """Return only the digits of a CNPJ-like value."""

    return re.sub(r"\D", "", str(value or ""))


def is_valid_cnpj(value: Any) -> bool:
    """Validate the 14 digits and the two check digits of a CNPJ."""

    digits = normalize_cnpj(value)
    if len(digits) != 14 or len(set(digits)) == 1:
        return False

    def digit(base: str, weights: list[int]) -> str:
        total = sum(int(number) * weight for number, weight in zip(base, weights))
        result = 11 - (total % 11)
        return "0" if result >= 10 else str(result)

    first = digit(digits[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    second = digit(digits[:12] + first, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return digits[-2:] == first + second


def build_cnpj(basico: Any, ordem: Any, dv: Any) -> str:
    """Build a 14-digit CNPJ from its three source parts."""

    cnpj = normalize_cnpj(f"{basico or ''}{ordem or ''}{dv or ''}")
    if len(cnpj) != 14:
        raise ValueError(f"CNPJ completo deve ter 14 dígitos: {cnpj!r}")
    return cnpj


def parse_date(value: Any) -> date | None:
    """Parse RFB ``AAAAMMDD`` dates, treating zero/empty values as null."""

    if value is None or isinstance(value, date):
        return value
    text = str(value).strip().strip('"')
    if text in {"", "0", "00000000", "None", "null"}:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_decimal_br(value: Any) -> Decimal | None:
    """Parse Brazilian decimal text such as ``5000,00``."""

    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).strip().strip('"')
    if not text:
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def split_cnaes(value: Any) -> list[str]:
    """Split and deduplicate principal/secondary CNAE codes."""

    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts = [str(part) for part in value]
    else:
        parts = re.split(r"[,;]+", str(value))
    result: list[str] = []
    for part in parts:
        code = re.sub(r"\D", "", part.strip().strip('"'))
        if len(code) == 7 and code not in result:
            result.append(code)
    return result


def normalize_text(value: Any) -> str:
    """Lowercase, remove accents/punctuation and collapse whitespace."""

    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def competence_end_date(competencia: str) -> date:
    """Return the last calendar day of an ``AAAA-MM`` competence."""

    try:
        year_text, month_text = competencia.split("-", 1)
        year, month = int(year_text), int(month_text)
        if not 1 <= month <= 12:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise ValueError(f"competência inválida: {competencia!r}") from None
    return date(year, month, calendar.monthrange(year, month)[1])


def parse_flag(value: Any) -> bool:
    return str(value or "").strip().upper() == "S"


def is_optante(flag: Any, exclusion: date | None, reference: date) -> bool:
    """Whether an option is active at the end of the competence."""

    if not parse_flag(flag):
        return False
    if exclusion is None:
        return True
    return exclusion > reference


def format_date(value: date | None) -> str:
    return value.strftime("%d/%m/%Y") if value else ""


def format_cnpj(value: Any) -> str:
    digits = normalize_cnpj(value)
    if len(digits) != 14:
        return digits
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
