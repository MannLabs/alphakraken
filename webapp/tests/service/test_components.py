"""Tests for the service components."""

# ruff: noqa: PD901 #  Avoid using the generic variable name `df` for DataFrames
from unittest.mock import MagicMock, patch

import pandas as pd
from service.components import show_filter


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
