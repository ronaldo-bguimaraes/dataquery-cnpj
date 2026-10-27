import pytest

from src.extracao_rfb.competencia import InvalidLimitError, select_files


def test_select_files_sorts_lexicographically_regardless_of_input_order():
    assert select_files(["c.zip", "a.zip", "b.zip"], None) == ["a.zip", "b.zip", "c.zip"]


def test_select_files_without_limit_returns_everything():
    assert select_files(["b.zip", "a.zip"], None) == ["a.zip", "b.zip"]


def test_select_files_with_limit_returns_first_n():
    assert select_files(["d.zip", "b.zip", "a.zip", "c.zip"], 2) == ["a.zip", "b.zip"]


def test_select_files_limit_above_total_returns_all():
    assert select_files(["b.zip", "a.zip"], 100) == ["a.zip", "b.zip"]


def test_select_files_limit_equal_to_total_returns_all():
    assert select_files(["b.zip", "a.zip"], 2) == ["a.zip", "b.zip"]


@pytest.mark.parametrize("limite", [0, -1, True, False, "1", 1.5])
def test_select_files_rejects_invalid_limit(limite):
    with pytest.raises(InvalidLimitError):
        select_files(["a.zip"], limite)


def test_selection_happens_before_existence_checks():
    # The first name is selected even if the caller already has it on disk; an
    # existing file must not free a slot for the next name.
    paths = ["a.zip", "b.zip", "c.zip"]
    existing = {"a.zip"}

    selected = select_files(paths, 1)

    assert selected == ["a.zip"]
    assert [name for name in selected if name in existing] == ["a.zip"]
    assert "b.zip" not in selected


def test_existing_raw_does_not_free_a_slot_in_the_limit():
    paths = ["a.zip", "b.zip", "c.zip"]
    existing_raw = {"a.zip"}

    selected = select_files(paths, 2)

    assert existing_raw == {"a.zip"}
    assert selected == ["a.zip", "b.zip"]
    assert len(selected) == 2


def test_limit_selection_uses_original_list_not_a_filtered_one():
    paths = ["Cnaes.zip", "Empresas0.zip", "Socios0.zip"]

    assert select_files(paths, 1) == ["Cnaes.zip"]
    assert select_files(paths, 3) == ["Cnaes.zip", "Empresas0.zip", "Socios0.zip"]
