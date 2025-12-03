"""Tests for the project_id_handler module."""

import pytest
from impl.project_id_handler import get_unique_project_id


def test_get_unique_project_found() -> None:
    """Test get_unique_project_id returns the correct project ID."""
    raw_file_name = (
        "20240524_OA2_Evo01_ViAl_SA_FAIMS40_IO17_A556_MOMI_APEM_P81_A11_R01.raw"
    )
    project_ids = ["A123", "A556"]
    assert get_unique_project_id(raw_file_name, project_ids) == "A556"


def test_get_unique_project_found_no_token() -> None:
    """Test get_unique_project_id returns the correct project ID if no token is provided."""
    raw_file_name = (
        "20240524_OA2_Evo01_ViAl_SA_FAIMS40_IO17_A556_MOMI_APEM_P81_A11_R01.raw"
    )
    project_ids = ["A123", "A556"]
    assert get_unique_project_id(raw_file_name, project_ids, initial_token="") == "A556"


@pytest.mark.parametrize(
    "project_ids",
    [
        [],
        ["A123", "A124"],
        [
            "A123",
            "20240524",
            "OA2_Evo01",
            "ViAl",
        ],  # matches before "SA"
        [
            "A123",
            "SA",
        ],  # "SA" itself
        ["A123", "FAIMS40", "A556"],  # multiple matches
        [
            "A123",
            "20240524",
            "OA2_Evo01",
            "ViAl",
            "SA",
            "FAIMS40",
            "A556",
        ],  # all of the above
    ],
)
def test_get_unique_project_id_not_found(project_ids: list[str]) -> None:
    """Test get_unique_project_id returns None if no project ID is found."""
    raw_file_name = (
        "20240524_OA2_Evo01_ViAl_SA_FAIMS40_IO17_A556_MOMI_APEM_P81_A11_R01.raw"
    )
    assert (
        get_unique_project_id(raw_file_name, project_ids, initial_token="_SA_") is None  # noqa: S106
    )


def test_get_unique_project_case_insensitive_mixed_case() -> None:
    """Test that mixed-case token in filename matches different mixed-case project ID."""
    raw_file_name = "20240524_OA2_Evo01_ViAl_SA_FAIMS40_IO17_ProJ123_MOMI_APEM.raw"
    project_ids = ["TEST999", "PROJ123"]
    assert get_unique_project_id(raw_file_name, project_ids) == "PROJ123"
