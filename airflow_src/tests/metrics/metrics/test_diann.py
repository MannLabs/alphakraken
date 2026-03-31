"""Tests for DIA-NN metrics."""

# ruff: noqa: PLR2004 # magic numbers are fine in tests
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from plugins.metrics.metrics.diann import BasicStats, calc_diann_metrics


@patch("plugins.metrics.metrics.diann.BasicStats")
def test_calc_diann_metrics_happy_path(mock_basic_stats: MagicMock) -> None:
    """Test the happy path of calc_diann_metrics with mock metrics."""
    mock_basic_stats.return_value = MagicMock()
    mock_basic_stats.return_value.get.return_value = {"proteins": 500, "peptides": 3000}

    result = calc_diann_metrics(Path("output_directory"))

    assert result == {"proteins": 500, "peptides": 3000}


@patch("plugins.metrics.metrics.diann.DataStore")
def test_basic_stats_calculates_mean(mock_datastore: MagicMock) -> None:
    """Test that BasicStats calculates mean values per column."""
    mock_df = pd.DataFrame(
        {
            "Proteins.Identified": [100, 200, 300, 400],
            "Precursors.Identified": [1000, 2000, 3000, 4000],
        }
    )
    mock_datastore.__getitem__.return_value = mock_df

    metrics = BasicStats(mock_datastore).get()

    assert metrics["proteins"] == 250.0
    assert metrics["peptides"] == 2500.0


@patch("plugins.metrics.metrics.diann.DataStore")
def test_basic_stats_single_row(mock_datastore: MagicMock) -> None:
    """Test BasicStats with a single row."""
    mock_df = pd.DataFrame(
        {
            "Proteins.Identified": [500],
            "Precursors.Identified": [3000],
        }
    )
    mock_datastore.__getitem__.return_value = mock_df

    metrics = BasicStats(mock_datastore).get()

    assert metrics["proteins"] == 500.0
    assert metrics["peptides"] == 3000.0
