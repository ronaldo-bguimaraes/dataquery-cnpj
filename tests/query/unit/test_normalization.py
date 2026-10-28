from __future__ import annotations

from datetime import date
from decimal import Decimal

from src.query.domain.normalization import (
    build_cnpj,
    competence_end_date,
    is_optante,
    is_valid_cnpj,
    normalize_cnpj,
    normalize_text,
    parse_date,
    parse_decimal_br,
    split_cnaes,
)


def test_normalize_cnpj_accepts_mask_and_spaces():
    assert normalize_cnpj("11.222.333/0001-81") == "11222333000181"
    assert normalize_cnpj(" 11 222 333 0001 81 ") == "11222333000181"


def test_build_cnpj_uses_basico_ordem_and_dv():
    assert build_cnpj("11222333", "0001", "81") == "11222333000181"


def test_is_valid_cnpj_rejects_bad_check_digits():
    assert is_valid_cnpj("11.222.333/0001-81") is True
    assert is_valid_cnpj("11.222.333/0001-82") is False
    assert is_valid_cnpj("not-a-cnpj") is False


def test_parse_date_handles_rfb_formats():
    assert parse_date("20200101") == date(2020, 1, 1)
    assert parse_date("00000000") is None
    assert parse_date("") is None
    assert parse_date(None) is None
    assert parse_date("20201301") is None


def test_parse_decimal_br_handles_comma_and_thousands():
    assert parse_decimal_br("5000,00") == Decimal("5000.00")
    assert parse_decimal_br("1.234,56") == Decimal("1234.56")
    assert parse_decimal_br("0,00") == Decimal("0.00")
    assert parse_decimal_br("") is None


def test_split_cnaes_accepts_comma_semicolon_and_deduplicates():
    assert split_cnaes("2222201,3333301;4444401,2222201") == [
        "2222201",
        "3333301",
        "4444401",
    ]
    assert split_cnaes("") == []


def test_normalize_text_removes_case_accents_and_extra_spaces():
    assert normalize_text("  Comércio   Açaí  ") == "comercio acai"


def test_competence_end_date_uses_last_day_of_month():
    assert competence_end_date("2026-10") == date(2026, 10, 31)
    assert competence_end_date("2024-02") == date(2024, 2, 29)


def test_is_optante_respects_exclusion_until_competence_end():
    reference = date(2026, 10, 31)
    assert is_optante("S", None, reference) is True
    assert is_optante("S", date(2023, 1, 1), reference) is False
    assert is_optante("N", None, reference) is False
    assert is_optante("N", date(2023, 1, 1), reference) is False
