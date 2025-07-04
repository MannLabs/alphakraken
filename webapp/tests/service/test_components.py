"""Tests for the service components."""

from datetime import datetime, timedelta

# ruff: noqa: PD901 #  Avoid using the generic variable name `df` for DataFrames
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pandas import DataFrame
from service.components import (
    _get_color,
    display_status,
    get_full_backup_path,
    highlight_status_cell,
    show_date_select,
    show_filter,
)


@pytest.mark.parametrize(
    ("filter_value"),
    [
        ("SOME_t"),
        ("!other_t"),
        ("so(.*)text"),
        ("column1=[0.5,1]"),
        ("column2=so(.*)text"),
    ],
)
@patch("streamlit.text_input")
def test_input_filter_happy_path(
    mock_text_input: MagicMock,
    filter_value: str,
) -> None:
    """Test that the filter returns the correct DataFrame when a match is found."""
    mock_text_input.return_value = filter_value
    df = pd.DataFrame(
        {
            "column1": [1, 2],
            "column2": ["some_text", "other_text"],
        }
    )

    # when
    filtered_df, returned_filter_value, errors = show_filter(
        df, text_to_display="Some Filter"
    )

    assert len(filtered_df) == 1
    assert "some_text" in filtered_df["column2"].to_numpy()
    assert filter_value == returned_filter_value
    assert errors == []


@patch("streamlit.text_input")
def test_input_filter_happy_path_exclusive(mock_text_input: MagicMock) -> None:
    """Test that the filter returns the correct DataFrame when a match is found."""
    mock_text_input.return_value = "!FILTER_text"
    df = pd.DataFrame(
        {
            "column1": ["filter_text", "other_text"],
            "column2": ["other_text", "other_text"],
        }
    )

    # when
    filtered_df, filter_value, errors = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 1
    assert "filter_text" not in filtered_df["column1"].to_numpy()
    assert filter_value == "!FILTER_text"
    assert errors == []


@patch("streamlit.text_input")
def test_input_filter_no_match(mock_text_input: MagicMock) -> None:
    """Test that the filter returns an empty DataFrame when no match is found."""
    mock_text_input.return_value = "no_match"
    df = pd.DataFrame(
        {
            "column1": ["filter_text", "other_text"],
            "column2": ["other_text", "filter_text"],
        }
    )

    # when
    filtered_df, filter_value, errors = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 0
    assert filter_value == "no_match"
    assert errors == []


@patch("streamlit.text_input")
def test_input_filter_bad_regexp(mock_text_input: MagicMock) -> None:
    """Test that the filter returns the correct DataFrame when a match is found but bad regexpt is ignored."""
    mock_text_input.return_value = "FILTER_text & (bad_regexp"
    df = pd.DataFrame(
        {
            "column1": ["filter_text", "other_text"],
            "column2": ["other_text", "other_text"],
        }
    )

    # when
    filtered_df, filter_value, errors = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 1
    assert "filter_text" in filtered_df["column1"].to_numpy()
    assert filter_value == "FILTER_text & (bad_regexp"
    assert errors == [
        "Could not parse filter (bad_regexp: ignoring it. <class 're.error'>: 'missing ), unterminated subpattern at position 0'"
    ]


@patch("streamlit.text_input")
def test_input_filter_empty_input(mock_text_input: MagicMock) -> None:
    """Test that the filter returns the original DataFrame when no input is given."""
    mock_text_input.return_value = None
    df = pd.DataFrame(
        {
            "column1": ["filter_text", "other_text"],
            "column2": ["other_text", "filter_text"],
        }
    )

    # when
    filtered_df, filter_value, errors = show_filter(df, text_to_display="Some Filter")

    pd.testing.assert_frame_equal(filtered_df, df)
    assert filter_value is None
    assert errors == []


@patch("streamlit.text_input")
def test_input_filter_combination(mock_text_input: MagicMock) -> None:
    """Test that the filter returns the original DataFrame when inclusive and exclusive filtering are combined."""
    mock_text_input.return_value = "incl & !excl"
    df = pd.DataFrame(
        {
            "column1": ["incl", "incl", "something"],
            "column2": ["something", "excl", "something"],
        }
    )

    # when
    filtered_df, filter_value, errors = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 1
    assert "incl" in filtered_df["column1"].to_numpy()
    assert "excl" not in filtered_df["column2"].to_numpy()
    assert filter_value == "incl & !excl"
    assert errors == []


@patch("streamlit.date_input")
def test_date_input_happy_path(mock_date_input: MagicMock) -> None:
    """Test that the date filter returns the correct DataFrame when a match is found."""
    mock_date_input.return_value = datetime(2021, 1, 5)  # noqa: DTZ001
    df = pd.DataFrame(
        {
            "created_at": [
                datetime(2021, 1, 1),  # noqa: DTZ001
                datetime(2021, 1, 10),  # noqa: DTZ001
            ],
            "column2": ["file1", "file2"],
        }
    )

    # when
    filtered_df = show_date_select(df)

    assert len(filtered_df) == 1
    assert "file2" in filtered_df["column2"].to_numpy()


@patch("streamlit.dataframe")
def test_display_status_with_multiple_instruments(mock_st_dataframe: MagicMock) -> None:
    """Test that the display_status function works correctly."""
    ts1 = datetime(2022, 1, 1, 11, 0, 0)  # noqa: DTZ001
    ts2 = datetime(2022, 1, 1, 10, 0, 0)  # noqa: DTZ001
    df = pd.DataFrame(
        {
            "instrument_id": ["inst1", "inst1", "inst2", "inst2"],
            "created_at": [ts1, ts2, ts1, ts2],
            "updated_at_": [ts1, ts2, ts1, ts2],
        }
    )

    status_data_df = pd.DataFrame(
        pd.DataFrame(
            {
                "_id": ["inst1", "inst2"],
                "updated_at_": [
                    ts1,
                    ts2,
                ],
                "status": ["ok", "error"],
                "free_space_gb": [100, 200],
                "status_details": ["", ""],
            }
        )
    )

    # when
    display_status(df, status_data_df)

    mock_st_dataframe.assert_called_once()
    result_df = mock_st_dataframe.call_args_list[0].args[0].data

    assert result_df["instrument_id"].tolist() == ["inst1", "inst2"]
    assert result_df["last_file_creation"].tolist() == [ts1, ts1]
    assert result_df["last_status_update"].tolist() == [ts1, ts1]
    assert result_df["last_health_check"].tolist() == [ts1, ts2]
    assert result_df["free_space_gb"].tolist() == [100, 200]
    assert "last_file_creation_text" in result_df.columns
    assert "last_status_update_text" in result_df.columns
    assert "last_health_check_text" in result_df.columns


def test_get_color() -> None:
    """Test that the color is returned correctly."""
    now = datetime.now()  # noqa: DTZ005
    row = pd.Series(
        {
            "last_file_creation": now - timedelta(hours=1),
            "last_status_update": now - timedelta(hours=3),
            "ignored": "value",
        }
    )

    # when
    result = _get_color(
        row,
        columns=["last_file_creation", "last_status_update"],
        green_ages_m=[2 * 60, 2 * 60],
        red_ages_m=[8 * 60, 8 * 60],
        colormaps=["summer", "summer"],
    )
    assert result == ["background-color: #008066", "background-color: #2a9466", None]


def get_status_cell_style() -> None:
    """Test that the status cell is highlighted correctly."""
    status_to_expected_style = {
        "error": ["background-color: red"],
        "done": ["background-color: green"],
        "ignored": ["background-color: lightgray"],
        "other": ["background-color: #aed989"],
    }

    for status, expected_style in status_to_expected_style.items():
        # when
        style = highlight_status_cell(pd.Series({"status": status}))
        assert style == expected_style


def test_get_full_backup_path_handles_thermo_files() -> None:
    """Test that the function handles Thermo files correctly."""
    df = DataFrame(
        {
            "backup_base_path": ["/backup/path"],
            "file_info": [{"file1.raw": [1, "hash1"]}],
        }
    )
    paths, is_multiple_types = get_full_backup_path(df)
    assert paths == ["/backup/path/file1.raw"]
    assert not is_multiple_types


def test_get_full_backup_path_handles_sciex_files() -> None:
    """Test that the function handles Sciex files correctly."""
    df = DataFrame(
        {
            "backup_base_path": ["/backup/path"],
            "file_info": [
                {"file1.wiff": [1, "hash1"], "file2.wiff.scan": [2, "hash2"]}
            ],
        }
    )
    paths, is_multiple_types = get_full_backup_path(df)
    assert paths == ["/backup/path/file1.wiff", "/backup/path/file2.wiff.scan"]
    assert not is_multiple_types


def test_get_full_backup_path_handles_bruker_files() -> None:
    """Test that the function handles Bruker files correctly."""
    df = DataFrame(
        {
            "backup_base_path": ["/backup/path"],
            "file_info": [
                {"folder.d/file1": [1, "hash1"], "folder.d/file2": [1, "hash2"]}
            ],
        }
    )
    paths, is_multiple_types = get_full_backup_path(df)
    assert paths == ["/backup/path/folder.d"]
    assert not is_multiple_types


def test_get_full_backup_path_raises_on_missing_data() -> None:
    """Test that the function raises an error when both backup_base_path and file_info are missing."""
    df = DataFrame(
        {"_id": ["some_file.raw"], "backup_base_path": [None], "file_info": [None]}
    )
    with pytest.raises(
        ValueError,
        match="Missing backup_base_path_str=None or file_info=None for file some_file.raw. Please exclude from selection.",
    ):
        get_full_backup_path(df)


def test_get_full_backup_path_detects_multiple_instrument_types() -> None:
    """Test that the function detects multiple instrument types."""
    df = DataFrame(
        {
            "backup_base_path": ["/backup/path", "/backup/path"],
            "file_info": [{"file1.raw": [1, "hash1"]}, {"file1.wiff": [1, "hash1"]}],
        }
    )
    paths, is_multiple_types = get_full_backup_path(df)
    assert is_multiple_types
