import pytest

from src.extracao_rfb.competencia import (
    InvalidCompetenciaError,
    InvalidLimitError,
    validate_competencia,
    validate_limit,
)


@pytest.mark.parametrize("value", ["2026-02", " 2026-02 ", "1999-12", "2026-01"])
def test_validate_competencia_accepts_valid_values(value):
    assert validate_competencia(value) == value.strip()


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "   ",
        "2026-2",
        "02-2026",
        "2026-13",
        "2026-00",
        "2026/02",
        "2026-02-01",
        "202602",
        "abc",
        202602,
    ],
)
def test_validate_competencia_rejects_invalid_values(value):
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia(value)


def test_validate_limit_accepts_none():
    assert validate_limit(None) is None


@pytest.mark.parametrize("value", [1, 37])
def test_validate_limit_accepts_positive_integers(value):
    assert validate_limit(value) == value


@pytest.mark.parametrize("value", [0, -1, True, False, "1", 1.5])
def test_validate_limit_rejects_invalid_values(value):
    with pytest.raises(InvalidLimitError):
        validate_limit(value)
