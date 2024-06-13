"""Tests for the db module."""

from unittest.mock import MagicMock

from service.db import df_from_db_data


def test_df_from_db_data() -> None:
    """Test the df_from_db_data function."""
    mock_query = MagicMock()
    mock_query.to_mongo.return_value = {"a": 1, "b": 2, "c": 3}

    mock_query_set = [mock_query, mock_query]

    # when
    result = df_from_db_data(mock_query_set)

    expected_data = {"a": {0: 1, 1: 1}, "b": {0: 2, 1: 2}, "c": {0: 3, 1: 3}}
    assert result.to_dict() == expected_data


def test_df_from_db_data_all_parameters() -> None:
    """Test the df_from_db_data function with all parameters."""
    mock_query = MagicMock()
    mock_query.to_mongo.return_value = {"a": 1, "b": 2, "c": 3}

    mock_query_set = [mock_query, mock_query]

    # when
    result = df_from_db_data(mock_query_set, drop_duplicates=["a"], drop_columns=["b"])

    expected_data = {"a": {0: 1}, "c": {0: 3}}
    assert result.to_dict() == expected_data
