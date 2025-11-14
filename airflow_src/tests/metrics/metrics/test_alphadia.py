"""Tests for the metrics calculator."""

# ruff: noqa: PLR2004 # magic numbers are fine in tests
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from plugins.metrics.metrics.alphadia import (
    BasicStats,
    IntensityStatsSum,
    PrecursorStatsAgg,
    PrecursorStatsIntensity,
    PrecursorStatsMeanLenSequence,
    calc_alphadia_metrics,
)


@patch("plugins.metrics.metrics.alphadia.BasicStats")
@patch("plugins.metrics.metrics.alphadia.IntensityStatsSum")
@patch("plugins.metrics.metrics.alphadia.PrecursorStatsAgg")
@patch("plugins.metrics.metrics.alphadia.PrecursorStatsIntensity")
@patch("plugins.metrics.metrics.alphadia.PrecursorStatsMeanLenSequence")
@patch("plugins.metrics.metrics.alphadia.InternalStats")
def test_calc_alphadia_metrics_happy_path(  # noqa: PLR0913
    mock_internal_stats: MagicMock,
    mock_precursor_stats_mean_len_sequence: MagicMock,
    mock_precursor_stats_intensity: MagicMock,
    mock_precursor_stats_mean: MagicMock,
    mock_intensity_stats_sum: MagicMock,
    mock_basic_stats: MagicMock,
) -> None:
    """Test the happy path of calc_metrics with mock metrics."""
    # map metrics name against mock getters and mock return value
    mock_metrics_and_getters = {
        "basic_metric": (
            mock_basic_stats,
            "value1",
        ),
        "intensity_sum_metric": (
            mock_intensity_stats_sum,
            "value2",
        ),
        "precursor_mean_metric": (
            mock_precursor_stats_mean,
            "value3",
        ),
        "precursor_mean_len_sequence_metric": (
            mock_precursor_stats_mean_len_sequence,
            "value4",
        ),
        "precursor_intensity_metric": (
            mock_precursor_stats_intensity,
            "value5",
        ),
        "internal_metric": (
            mock_internal_stats,
            "value6",
        ),
    }

    # set up mock return values
    for key, (getter, value) in mock_metrics_and_getters.items():
        getter.return_value = MagicMock()
        getter.return_value.get.return_value = {key: value}

    # when
    result = calc_alphadia_metrics(Path("output_directory"))

    assert result == {
        key: value for key, (getter, value) in mock_metrics_and_getters.items()
    }


# just used for manual testing so far
# def test_calc_metrics_real_data() -> None:
#     """Test the happy path of calc_metrics with real_data."""
#     # when
#     import os
#
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#
#     result = calc_metrics(Path(current_dir) / "../../../airflow_test_folders/_data")
#     print(result)


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_basic_stats_calculation(mock_datastore: MagicMock) -> None:
    """Test basic stats calculation."""
    data = {col: [i] for i, col in enumerate(BasicStats._columns)}
    mock_df = pd.DataFrame(data)

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = BasicStats(mock_datastore).get()

    expected_metrics = {col: i for i, col in enumerate(BasicStats._columns)}
    assert metrics == expected_metrics


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_intensity_stats_calculation(mock_datastore: MagicMock) -> None:
    """Test intensity stats calculation."""
    mock_df = pd.DataFrame(
        {
            "weighted_ms1_intensity": [1.0, 2.0],
            "pg.intensity": [10.0, 20.0],
            "precursor.intensity": [5.0, 15.0],
            "peptide.intensity": [7.0, 13.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = IntensityStatsSum(mock_datastore).get()

    assert metrics["weighted_ms1_intensity_sum"] == 3.0
    assert metrics["pg.intensity_sum"] == 30.0
    assert metrics["precursor.intensity_sum"] == 20.0
    assert metrics["peptide.intensity_sum"] == 20.0


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_intensity_stats_calculation_column_missing(mock_datastore: MagicMock) -> None:
    """Test intensity stats calculation gracefully handling missing columns."""
    mock_df = pd.DataFrame(
        {
            "weighted_ms1_intensity": [1.0, 2.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = IntensityStatsSum(mock_datastore).get()

    assert metrics["weighted_ms1_intensity_sum"] == 3.0
    assert "pg.intensity_sum" not in metrics
    assert "precursor.intensity_sum" not in metrics
    assert "peptide.intensity_sum" not in metrics


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_precursor_stats_mean_calculation(mock_datastore: MagicMock) -> None:
    """Test precursor stats mean calculation."""
    mock_df = pd.DataFrame(
        {
            "precursor.charge": [1.0, 2.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsAgg(mock_datastore).get()

    assert metrics["precursor.charge_mean"] == 1.5
    assert metrics["precursor.charge_std"] == 0.7071067811865476
    assert metrics["precursor.charge_median"] == 1.5


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_precursor_stats_sequence_len_mean_calculation(
    mock_datastore: MagicMock,
) -> None:
    """Test precursor stats sequence length mean calculation."""
    mock_df = pd.DataFrame(
        {
            "precursor.sequence": ["A", "AB"],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsMeanLenSequence(mock_datastore).get()

    assert metrics["precursor.sequence_len_mean"] == 1.5
    assert metrics["precursor.sequence_len_std"] == 0.7071067811865476
    assert metrics["precursor.sequence_len_median"] == 1.5


@patch("plugins.metrics.metrics.alphadia.DataStore")
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
