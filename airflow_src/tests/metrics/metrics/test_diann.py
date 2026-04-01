"""Tests for DIA-NN metrics."""

# ruff: noqa: PLR2004 # magic numbers are fine in tests
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from plugins.metrics.metrics.diann import (
    BasicStats,
    PrecursorMedianRT,
    calc_diann_metrics,
)


@patch("plugins.metrics.metrics.diann.PrecursorMedianRT")
@patch("plugins.metrics.metrics.diann.BasicStats")
def test_calc_diann_metrics_happy_path(
    mock_basic_stats: MagicMock, mock_precursor_median_rt: MagicMock
) -> None:
    """Test the happy path of calc_diann_metrics with mock metrics."""
    mock_basic_stats.return_value = MagicMock()
    mock_basic_stats.return_value.get.return_value = {"proteins": 500, "peptides": 3000}
    mock_precursor_median_rt.return_value = MagicMock()
    mock_precursor_median_rt.return_value.get.return_value = {
        "median_precursor_rt": 42.0
    }

    result = calc_diann_metrics(Path("output_directory"))

    assert result == {"proteins": 500, "peptides": 3000, "median_precursor_rt": 42.0}


@patch("plugins.metrics.metrics.diann.DataStore")
def test_basic_stats_calculates_mean(mock_datastore: MagicMock) -> None:
    """Test that BasicStats calculates mean values per column."""
    mock_df = pd.DataFrame(
        {
            "Precursors.Identified": [1000, 2000, 3000, 4000],
            "Proteins.Identified": [100, 200, 300, 400],
            "Total.Quantity": [1e6, 2e6, 3e6, 4e6],
            "MS1.Signal": [1e5, 2e5, 3e5, 4e5],
            "MS2.Signal": [2e5, 3e5, 4e5, 5e5],
            "FWHM.Scans": [5.0, 6.0, 7.0, 8.0],
            "FWHM.RT": [0.1, 0.2, 0.3, 0.4],
            "Median.Mass.Acc.MS1": [1.0, 2.0, 3.0, 4.0],
            "Median.Mass.Acc.MS1.Corrected": [0.5, 1.0, 1.5, 2.0],
            "Median.Mass.Acc.MS2": [2.0, 3.0, 4.0, 5.0],
            "Median.Mass.Acc.MS2.Corrected": [1.0, 1.5, 2.0, 2.5],
            "Normalisation.Instability": [0.01, 0.02, 0.03, 0.04],
            "Median.RT.Prediction.Acc": [0.5, 0.6, 0.7, 0.8],
            "Average.Peptide.Length": [10.0, 12.0, 14.0, 16.0],
            "Average.Peptide.Charge": [2.0, 2.5, 3.0, 3.5],
            "Average.Missed.Tryptic.Cleavages": [0.1, 0.2, 0.3, 0.4],
        }
    )
    mock_datastore.get.return_value = mock_df

    metrics = BasicStats(mock_datastore).get()

    assert metrics["precursors"] == 2500.0
    assert metrics["proteins"] == 250.0
    assert metrics["total_quantity"] == 2.5e6
    assert metrics["ms1_signal"] == 2.5e5
    assert metrics["ms2_signal"] == 3.5e5
    assert metrics["fwhm_scans"] == 6.5
    assert metrics["fwhm_rt"] == 0.25
    assert metrics["median_mass_acc_ms1"] == 2.5
    assert metrics["median_mass_acc_ms1_corrected"] == 1.25
    assert metrics["median_mass_acc_ms2"] == 3.5
    assert metrics["median_mass_acc_ms2_corrected"] == 1.75
    assert metrics["normalisation_instability"] == 0.025
    assert metrics["median_rt_prediction_acc"] == 0.65
    assert metrics["average_peptide_length"] == 13.0
    assert metrics["average_peptide_charge"] == 2.75
    assert metrics["average_missed_tryptic_cleavages"] == 0.25


@patch("plugins.metrics.metrics.diann.DataStore")
def test_basic_stats_single_row(mock_datastore: MagicMock) -> None:
    """Test BasicStats with a single row."""
    mock_df = pd.DataFrame(
        {
            "Precursors.Identified": [3000],
            "Proteins.Identified": [500],
            "Total.Quantity": [1e6],
            "MS1.Signal": [1e5],
            "MS2.Signal": [2e5],
            "FWHM.Scans": [5.0],
            "FWHM.RT": [0.1],
            "Median.Mass.Acc.MS1": [1.0],
            "Median.Mass.Acc.MS1.Corrected": [0.5],
            "Median.Mass.Acc.MS2": [2.0],
            "Median.Mass.Acc.MS2.Corrected": [1.0],
            "Normalisation.Instability": [0.01],
            "Median.RT.Prediction.Acc": [0.5],
            "Average.Peptide.Length": [10.0],
            "Average.Peptide.Charge": [2.0],
            "Average.Missed.Tryptic.Cleavages": [0.1],
        }
    )
    mock_datastore.get.return_value = mock_df

    metrics = BasicStats(mock_datastore).get()

    assert metrics["proteins"] == 500.0
    assert metrics["precursors"] == 3000.0


@patch("plugins.metrics.metrics.diann.PrecursorMedianRT")
@patch("plugins.metrics.metrics.diann.BasicStats")
def test_calc_diann_metrics_missing_report_parquet(
    mock_basic_stats: MagicMock, mock_precursor_median_rt: MagicMock
) -> None:
    """Test that calc_diann_metrics returns BasicStats even when report.parquet is missing."""
    mock_basic_stats.return_value = MagicMock()
    mock_basic_stats.return_value.get.return_value = {"proteins": 500, "peptides": 3000}
    mock_precursor_median_rt.return_value = MagicMock()
    mock_precursor_median_rt.return_value.get.side_effect = FileNotFoundError(
        "report.parquet"
    )

    result = calc_diann_metrics(Path("output_directory"))

    assert result == {"proteins": 500, "peptides": 3000}


@patch("plugins.metrics.metrics.diann.DataStore")
def test_precursor_median_rt(mock_datastore: MagicMock) -> None:
    """Test that PrecursorMedianRT calculates the median RT."""
    mock_df = pd.DataFrame(
        {
            "RT": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )
    mock_datastore.get.return_value = mock_df

    metrics = PrecursorMedianRT(mock_datastore).get()

    assert metrics["median_precursor_rt"] == 30.0
