"""Tests for the db module."""

from unittest.mock import MagicMock, patch

import pandas as pd
from service.db import df_from_db_data, get_output_folders


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


@patch("service.db.connect_db")
@patch("service.db.Metrics")
def test_get_output_folders(
    mock_metrics: MagicMock,
    mock_connect_db: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_output_folders returns one row per metrics doc, respecting the N:1 relation."""
    m1, m2, m3 = MagicMock(), MagicMock(), MagicMock()
    m1.to_mongo.return_value = {
        "raw_file": "f1",
        "settings_name": "s1",
        "settings_version": 1,
        "type": "alphadia",
        "output_path": "/out/f1/alphadia",
    }
    m2.to_mongo.return_value = {
        "raw_file": "f2",
        "settings_name": "s1",
        "settings_version": 1,
        "type": "alphadia",
        "output_path": "/out/f2/alphadia",
    }
    # same raw file f1 run with different settings -> its own output folder
    m3.to_mongo.return_value = {
        "raw_file": "f1",
        "settings_name": "s2",
        "settings_version": 3,
        "type": "custom",
        "output_path": "/out/f1/custom",
    }
    mock_metrics.objects.filter.return_value.only.return_value = [m1, m2, m3]

    # when
    result = get_output_folders(["f1", "f2"])

    # then
    mock_metrics.objects.filter.assert_called_once_with(raw_file__in=["f1", "f2"])
    assert list(result.columns) == [
        "raw_file_id",
        "settings_name",
        "settings_version",
        "type",
        "output_path",
    ]
    assert result["raw_file_id"].tolist() == ["f1", "f2", "f1"]
    assert result["output_path"].tolist() == [
        "/out/f1/alphadia",
        "/out/f2/alphadia",
        "/out/f1/custom",
    ]


@patch("service.db.connect_db")
@patch("service.db.Metrics")
def test_get_output_folders_no_metrics(
    mock_metrics: MagicMock,
    mock_connect_db: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_output_folders returns an empty, well-formed DataFrame when no metrics exist."""
    mock_metrics.objects.filter.return_value.only.return_value = []

    # when
    result = get_output_folders(["f1"])

    # then
    assert result.empty
    assert list(result.columns) == [
        "raw_file_id",
        "settings_name",
        "settings_version",
        "type",
        "output_path",
    ]
