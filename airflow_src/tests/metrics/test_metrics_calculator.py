"""Tests for the metrics calculator."""

from unittest.mock import MagicMock, patch

import pandas as pd
from plugins.metrics.metrics_calculator import BasicStats, DataStore, calc_metrics


@patch("os.path.join")
@patch("plugins.metrics.metrics_calculator.file_name_to_read_method_mapping")
def test_datastore_getitem_loads_data_when_key_not_in_data(
    mock_mapping: MagicMock, mock_join: MagicMock
) -> None:
    """Test that __getitem__ loads data when key not in data store."""
    mock_join.return_value = "file_path"
    mock_mapping.__getitem__.return_value = lambda _: "data"
    datastore = DataStore("data_dir")

    # when
    result = datastore["key"]

    assert result == "data"


@patch("os.path.join")
@patch("plugins.metrics.metrics_calculator.file_name_to_read_method_mapping")
def test_datastore_getitem_returns_data_when_key_in_data(
    mock_mapping: MagicMock, mock_join: MagicMock
) -> None:
    """Test that __getitem__ returns data when key in data store."""
    mock_join.return_value = "file_path"
    mock_mapping.__getitem__.return_value = lambda _: "data"
    datastore = DataStore("data_dir")
    datastore._data["key"] = "existing_data"  # noqa: SLF001

    # when
    result = datastore["key"]

    assert result == "existing_data"


@patch("plugins.metrics.metrics_calculator.DataStore")
@patch("plugins.metrics.metrics_calculator.BasicStats")
def test_calc_metrics_happy_path(
    mock_basic_stats: MagicMock, mock_data_store: MagicMock
) -> None:
    """Test the happy path of calc_metrics."""
    mock_data_store_instance = MagicMock()
    mock_data_store.return_value = mock_data_store_instance
    mock_basic_stats_instance = MagicMock()
    mock_basic_stats.return_value = mock_basic_stats_instance
    mock_basic_stats_instance.get.return_value = {"metric": "value"}

    # when
    result = calc_metrics("output_directory")

    assert result == {"metric": "value"}


@patch("plugins.metrics.metrics_calculator.DataStore")
def test_basic_stats_calculation(mock_datastore: MagicMock) -> None:
    """Test basic stats calculation."""
    mock_df = pd.DataFrame(
        {
            "proteins": [1.0],
            "precursors": [2.0],
            "ms1_accuracy": [3.0],
            "fwhm_rt": [4.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df
    basic_stats = BasicStats(mock_datastore)

    assert basic_stats.get() == {
        "BasicStats_proteins_mean": 1.0,
        "BasicStats_precursors_mean": 2.0,
        "BasicStats_ms1_accuracy_mean": 3.0,
        "BasicStats_fwhm_rt_mean": 4.0,
    }
