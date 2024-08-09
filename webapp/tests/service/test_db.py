"""Tests for the db module."""

from unittest.mock import MagicMock

import pandas as pd
from service.db import df_from_db_data


def test_df_from_db_data() -> None:
    """Test the df_from_db_data function."""
    mock_query = MagicMock()
    mock_query.to_mongo.side_effect = [
        {"a": 1, "b": 2, "created_at_": 3},
        {"a": 4, "b": 5, "created_at_": 6},
    ]

    mock_query_set = [mock_query, mock_query]

    # when
    result = df_from_db_data(mock_query_set)

    expected_data = pd.DataFrame({"a": [4, 1], "b": [5, 2], "created_at_": [6, 3]})
    pd.testing.assert_frame_equal(expected_data, result)


def test_df_from_db_data_all_parameters() -> None:
    """Test the df_from_db_data function with all parameters."""
    mock_query = MagicMock()
    mock_query.to_mongo.side_effect = [
        {"a": 1, "b": 2, "created_at_": 3},
        {"a": 1, "b": 5, "created_at_": 6},
    ]

    mock_query_set = [mock_query, mock_query]

    # when
    result = df_from_db_data(mock_query_set, drop_duplicates=["a"], drop_columns=["b"])

    expected_data = pd.DataFrame({"a": [1], "created_at_": [6]})
    pd.testing.assert_frame_equal(expected_data, result)
