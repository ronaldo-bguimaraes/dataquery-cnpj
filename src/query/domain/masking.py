"""Privacy helpers for CNPJ/CPF display."""

from __future__ import annotations

import re
from typing import Any

CPF_MASK = "***.***.***-**"

_CPF_PATTERN = re.compile(r"(?<!\d)(?:\d{3}\.?\d{3}\.?\d{3}-?\d{2})(?!\d)")


def mask_document(value: Any) -> str:
    """Return a fully masked document, preserving only an empty value."""

    text = str(value or "").strip()
    if not text:
        return ""
    return CPF_MASK


def redact_cpf_patterns(value: Any) -> str:
    """Defensively replace CPF-like sequences in free-text fields."""

    return _CPF_PATTERN.sub(CPF_MASK, str(value or ""))


def has_full_cpf(value: Any) -> bool:
    """Return True when a text still contains an 11-digit CPF."""

    return bool(_CPF_PATTERN.search(str(value or "")))
