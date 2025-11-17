"""Tests for the metrics calculator."""

# ruff: noqa: PLR2004 # magic numbers are fine in tests
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from plugins.metrics.metrics.alphadia import (
    BasicStats,
    PrecursorStatsAgg,
    PrecursorStatsIntensity,
    PrecursorStatsMeanLenSequence,
    PrecursorStatsSum,
    calc_alphadia_metrics,
)


@patch("plugins.metrics.metrics.alphadia.BasicStats")
@patch("plugins.metrics.metrics.alphadia.PrecursorStatsSum")
@patch("plugins.metrics.metrics.alphadia.PrecursorStatsAgg")
@patch("plugins.metrics.metrics.alphadia.PrecursorStatsIntensity")
@patch("plugins.metrics.metrics.alphadia.PrecursorStatsMeanLenSequence")
@patch("plugins.metrics.metrics.alphadia.InternalStats")
def test_calc_alphadia_metrics_happy_path(  # noqa: PLR0913
    mock_internal_stats: MagicMock,
    mock_precursor_stats_mean_len_sequence: MagicMock,
    mock_precursor_stats_intensity: MagicMock,
    mock_precursor_stats_mean: MagicMock,
    mock_precursor_stats_sum: MagicMock,
    mock_basic_stats: MagicMock,
) -> None:
    """Test the happy path of calc_metrics with mock metrics."""
    # map metrics name against mock getters and mock return value
    mock_metrics_and_getters = {
        "basic_metric": (
            mock_basic_stats,
            "value1",
        ),
        "precursor_sum_metric": (
            mock_precursor_stats_sum,
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
    """Test basic stats calculation with alphadia v2 column names."""
    data = {
        "search.proteins": [100.0],
        "search.precursors": [1000.0],
        "search.fwhm_rt": [0.5],
        "search.fwhm_mobility": [0.02],
        "optimization.ms1_error": [0.1],
        "optimization.ms2_error": [0.15],
        "optimization.rt_error": [0.05],
        "optimization.mobility_error": [0.01],
        "calibration.ms1_bias": [5.0],
        "calibration.ms2_bias": [7.0],
        "raw.gradient_length": [60.0],
    }
    mock_df = pd.DataFrame(data)
    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = BasicStats(mock_datastore).get()

    # then - verify output uses mapped metric names (preserving backward compatibility)
    assert metrics["proteins"] == 100.0
    assert metrics["precursors"] == 1000.0
    assert metrics["fwhm_rt"] == 0.5
    assert metrics["fwhm_mobility"] == 0.02
    assert metrics["optimization.ms1_error"] == 0.1
    assert metrics["optimization.ms2_error"] == 0.15
    assert metrics["optimization.rt_error"] == 0.05
    assert metrics["optimization.mobility_error"] == 0.01
    assert metrics["calibration.ms1_bias"] == 5.0
    assert metrics["calibration.ms2_bias"] == 7.0
    assert metrics["gradient_length"] == 60.0


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_precursor_stats_calculation(mock_datastore: MagicMock) -> None:
    """Test precursor stats calculation with alphadia v2 column names."""
    mock_df = pd.DataFrame(
        {
            "weighted_ms1_intensity": [1.0, 2.0],
            "pg.intensity": [10.0, 20.0],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsSum(mock_datastore).get()

    assert metrics["weighted_ms1_intensity_sum"] == 3.0
    assert metrics["pg.intensity_sum"] == 30.0


@patch("plugins.metrics.metrics.alphadia.DataStore")
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


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_precursor_stats_mean_calculation(mock_datastore: MagicMock) -> None:
    """Test precursor stats mean calculation with alphadia v2 column names."""
    mock_df = pd.DataFrame(
        {
            "precursor.charge": [1.0, 2.0],
            "precursor.proba": [0.9, 0.95],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsAgg(mock_datastore).get()

    # then - v2 columns output with their full names
    assert metrics["precursor.charge_mean"] == 1.5
    assert metrics["precursor.charge_std"] == 0.7071067811865476
    assert metrics["precursor.charge_median"] == 1.5
    assert metrics["precursor.proba_mean"] == 0.925


@patch("plugins.metrics.metrics.alphadia.DataStore")
def test_precursor_stats_sequence_len_mean_calculation(
    mock_datastore: MagicMock,
) -> None:
    """Test precursor stats sequence length mean calculation with alphadia v2 column names."""
    mock_df = pd.DataFrame(
        {
            "precursor.sequence": ["A", "AB"],
        }
    )

    mock_datastore.__getitem__.return_value = mock_df

    # when
    metrics = PrecursorStatsMeanLenSequence(mock_datastore).get()

    # then - v2 column outputs with full name
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
