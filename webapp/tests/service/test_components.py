"""Tests for the service components."""

from datetime import datetime, timedelta

# ruff: noqa: PD901 #  Avoid using the generic variable name `df` for DataFrames
from unittest.mock import MagicMock, patch

import pandas as pd
from service.components import (
    _get_color,
    display_status,
    highlight_status_cell,
    show_date_select,
    show_filter,
)


@patch("streamlit.text_input")
def test_input_filter_happy_path(mock_text_input: MagicMock) -> None:
    """Test that the filter returns the correct DataFrame when a match is found."""
    mock_text_input.return_value = "FILTER_text"
    df = pd.DataFrame(
        {
            "column1": ["filter_text", "other_text"],
            "column2": ["other_text", "other_text"],
        }
    )

    # when
    filtered_df = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 1
    assert "filter_text" in filtered_df["column1"].to_numpy()


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
    filtered_df = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 1
    assert "filter_text" not in filtered_df["column1"].to_numpy()


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
    filtered_df = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 0


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
    filtered_df = show_filter(df, text_to_display="Some Filter")

    assert filtered_df is df


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
    filtered_df = show_filter(df, text_to_display="Some Filter")

    assert len(filtered_df) == 1
    assert "incl" in filtered_df["column1"].to_numpy()
    assert "excl" not in filtered_df["column2"].to_numpy()


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
                "last_error_occurred_at": [
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
    assert result_df["last_health_check_error"].tolist() == [ts1, ts2]
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
