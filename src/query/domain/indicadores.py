"""Derived prospecting indicators."""

from __future__ import annotations

from datetime import date, timedelta


def idade_em_anos(data_abertura: date | None, referencia: date) -> int | None:
    """Complete years between the opening date and the reference date."""

    if data_abertura is None:
        return None
    years = referencia.year - data_abertura.year
    if (referencia.month, referencia.day) < (data_abertura.month, data_abertura.day):
        years -= 1
    return max(years, 0)


def deslocar_anos(referencia: date, anos: int) -> date:
    """Shift a date back by ``anos`` years, tolerating 29 February."""

    try:
        return referencia.replace(year=referencia.year - anos)
    except ValueError:
        return date(referencia.year - anos, 2, 28)


def limites_idade(
    referencia: date,
    idade_min: int | None,
    idade_max: int | None,
) -> tuple[date | None, date | None]:
    """Translate an age range into inclusive opening-date bounds.

    Returns ``(abertura_de, abertura_ate)`` so the repository can reuse the
    indexed ``data_inicio_atividade`` column. A company aged ``n`` complete
    years at ``referencia`` was opened after ``referencia - (n + 1) years``.
    """

    abertura_de = (
        deslocar_anos(referencia, idade_max + 1) + timedelta(days=1)
        if idade_max is not None
        else None
    )
    abertura_ate = deslocar_anos(referencia, idade_min) if idade_min is not None else None
    return abertura_de, abertura_ate


def tem_contato(
    ddd: str | None,
    telefone: str | None,
    email: str | None,
) -> bool:
    """Whether the establishment has a cadastral phone or e-mail."""

    telefone_preenchido = bool(str(ddd or "").strip()) and bool(
        str(telefone or "").strip()
    )
    email_preenchido = bool(str(email or "").strip())
    return telefone_preenchido or email_preenchido
