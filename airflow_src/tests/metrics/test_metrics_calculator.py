"""Tests for the metrics calculator."""

# ruff: noqa: PLR2004 # magic numbers are fine in tests
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from metrics.metrics_calculator import (
    PrecursorStatsIntensity,
    PrecursorStatsMeanLenSequence,
)
from plugins.metrics.metrics_calculator import (
    BasicStats,
    DataStore,
    PrecursorStatsAgg,
    PrecursorStatsSum,
    calc_metrics,
)


@patch("os.path.join")
@patch("plugins.metrics.metrics_calculator.file_name_to_read_method_mapping")
def test_datastore_getitem_loads_data_when_key_not_in_data(
    mock_mapping: MagicMock, mock_join: MagicMock
) -> None:
    """Test that __getitem__ loads data when key not in data store."""
    mock_join.return_value = "file_path"
    mock_mapping.__getitem__.return_value = lambda _: "data"
    datastore = DataStore(Path("data_dir"))

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
    datastore = DataStore(Path("data_dir"))
    datastore._data["key"] = "existing_data"  # noqa: SLF001

    # when
    result = datastore["key"]

    assert result == "existing_data"


@patch("plugins.metrics.metrics_calculator.DataStore")
@patch("plugins.metrics.metrics_calculator.BasicStats")
@patch("plugins.metrics.metrics_calculator.PrecursorStatsSum")
@patch("plugins.metrics.metrics_calculator.PrecursorStatsAgg")
@patch("plugins.metrics.metrics_calculator.PrecursorStatsIntensity")
@patch("plugins.metrics.metrics_calculator.PrecursorStatsMeanLenSequence")
@patch("plugins.metrics.metrics_calculator.InternalStats")
def test_calc_metrics_happy_path(  # noqa: PLR0913
    mock_internal_stats: MagicMock,
    mock_precursor_stats_mean_len_sequence: MagicMock,
    mock_precursor_stats_intensity: MagicMock,
    mock_precursor_stats_mean: MagicMock,
    mock_precursor_stats_sum: MagicMock,
    mock_basic_stats: MagicMock,
    mock_data_store: MagicMock,
) -> None:
    """Test the happy path of calc_metrics."""
    mock_data_store_instance = MagicMock()
    mock_data_store.return_value = mock_data_store_instance

    mock_basic_stats_instance = MagicMock()
    mock_basic_stats.return_value = mock_basic_stats_instance
    mock_basic_stats_instance.get.return_value = {"basic_metric": "value1"}

    mock_precursor_stats_sum_instance = MagicMock()
    mock_precursor_stats_sum.return_value = mock_precursor_stats_sum_instance
    mock_precursor_stats_sum_instance.get.return_value = {
        "precursor_sum_metric": "value2"
    }

    mock_precursor_stats_mean_instance = MagicMock()
    mock_precursor_stats_mean.return_value = mock_precursor_stats_mean_instance
    mock_precursor_stats_mean_instance.get.return_value = {
        "precursor_mean_metric": "value3"
    }

    mock_precursor_stats_mean_len_sequence_instance = MagicMock()
    mock_precursor_stats_mean_len_sequence.return_value = (
        mock_precursor_stats_mean_len_sequence_instance
    )
    mock_precursor_stats_mean_len_sequence_instance.get.return_value = {
        "precursor_mean_len_sequence_metric": "value4"
    }

    mock_precursor_stats_intensity_instance = MagicMock()
    mock_precursor_stats_intensity.return_value = (
        mock_precursor_stats_intensity_instance
    )
    mock_precursor_stats_intensity_instance.get.return_value = {
        "precursor_intensity_metric": "value5"
    }

    mock_internal_stats_instance = MagicMock()
    mock_internal_stats.return_value = mock_internal_stats_instance
    mock_internal_stats_instance.get.return_value = {"internal_metric": "value6"}

    # when
    result = calc_metrics(Path("output_directory"))

    assert result == {
        "basic_metric": "value1",
        "precursor_sum_metric": "value2",
        "precursor_mean_metric": "value3",
        "precursor_mean_len_sequence_metric": "value4",
        "precursor_intensity_metric": "value5",
        "internal_metric": "value6",
    }
    mock_data_store.assert_called_once_with(Path("output_directory"))
    mock_basic_stats.assert_called_once_with(mock_data_store.return_value)
    mock_precursor_stats_sum.assert_called_once_with(mock_data_store.return_value)
    mock_internal_stats.assert_called_once_with(mock_data_store.return_value)


# just used for manual testing so far
# def test_calc_metrics_real_data(
#
# ) -> None:
#     """Test the happy path of calc_metrics with real_data."""
#     # when
#     result = calc_metrics(Path("alphakraken/airflow_test_folders/_data"))


@patch("plugins.metrics.metrics_calculator.DataStore")
def test_basic_stats_calculation(mock_datastore: MagicMock) -> None:
    """Test basic stats calculation."""
    data = {col: [i] for i, col in enumerate(BasicStats._columns)}  # noqa: SLF001
    mock_df = pd.DataFrame(data)

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = BasicStats(mock_datastore).get()

    expected_metrics = {col: i for i, col in enumerate(BasicStats._columns)}  # noqa: SLF001
    assert metrics == expected_metrics


@patch("plugins.metrics.metrics_calculator.DataStore")
def test_precursor_stats_calculation(mock_datastore: MagicMock) -> None:
    """Test precursor stats calculation."""
    mock_df = pd.DataFrame(
        {
            "weighted_ms1_intensity": [1.0, 2.0],
            "intensity": [10.0, 20.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsSum(mock_datastore).get()

    assert metrics["weighted_ms1_intensity_sum"] == 3.0
    assert metrics["intensity_sum"] == 30.0


@patch("plugins.metrics.metrics_calculator.DataStore")
def test_precursor_stats_calculation_column_missing(mock_datastore: MagicMock) -> None:
    """Test precursor stats calculation os gracefully handling a missing column."""
    mock_df = pd.DataFrame(
        {
            "weighted_ms1_intensity": [1.0, 2.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsSum(mock_datastore).get()

    assert metrics["weighted_ms1_intensity_sum"] == 3.0
    assert "intensity_sum" not in metrics


@patch("plugins.metrics.metrics_calculator.DataStore")
def test_precursor_stats_mean_calculation(mock_datastore: MagicMock) -> None:
    """Test precursor stats mean calculation."""
    mock_df = pd.DataFrame(
        {
            "charge": [1.0, 2.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsAgg(mock_datastore).get()

    assert metrics["charge_mean"] == 1.5
    assert metrics["charge_std"] == 0.7071067811865476
    assert metrics["charge_median"] == 1.5


@patch("plugins.metrics.metrics_calculator.DataStore")
def test_precursor_stats_sequence_len_mean_calculation(
    mock_datastore: MagicMock,
) -> None:
    """Test precursor stats sequence length mean calculation."""
    mock_df = pd.DataFrame(
        {
            "sequence": ["A", "AB"],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsMeanLenSequence(mock_datastore).get()

    assert metrics["sequence_len_mean"] == 1.5
    assert metrics["sequence_len_std"] == 0.7071067811865476
    assert metrics["sequence_len_median"] == 1.5


@patch("plugins.metrics.metrics_calculator.DataStore")
def test_precursor_stats_intensity_median_calculation(
    mock_datastore: MagicMock,
) -> None:
    """Test precursor stats sequence length mean calculation."""
    mock_df = pd.DataFrame(
        {
            "sum_b_ion_intensity": [1.0, 2.0],
            "sum_y_ion_intensity": [10.0, 20.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsIntensity(mock_datastore).get()

    assert metrics["precursor_intensity_median"] == 33 / 2
