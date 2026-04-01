"""Metrics for DIA-NN."""

import logging
from pathlib import Path
from typing import ClassVar

import pandas as pd
from metrics.metrics.base import DataStore, Metrics, read_parquet, read_tsv

REPORT_FILE_NAME = "report.stats.tsv"
REPORT_PARQUET_FILE_NAME = "report.parquet"

file_name_to_read_method_mapping = {
    REPORT_FILE_NAME: read_tsv,
    REPORT_PARQUET_FILE_NAME: read_parquet,
}


class BasicStats(Metrics):
    """Basic statistics from DIA-NN report."""

    _file = REPORT_FILE_NAME

    _columns: ClassVar[dict[str, str | None]] = {
        "Precursors.Identified": "precursors",
        "Proteins.Identified": "proteins",
        "Total.Quantity": "total_quantity",
        "MS1.Signal": "ms1_signal",
        "MS2.Signal": "ms2_signal",
        "FWHM.Scans": "fwhm_scans",
        "FWHM.RT": "fwhm_rt",
        "Median.Mass.Acc.MS1": "median_mass_acc_ms1",
        "Median.Mass.Acc.MS1.Corrected": "median_mass_acc_ms1_corrected",
        "Median.Mass.Acc.MS2": "median_mass_acc_ms2",
        "Median.Mass.Acc.MS2.Corrected": "median_mass_acc_ms2_corrected",
        "Normalisation.Instability": "normalisation_instability",
        "Median.RT.Prediction.Acc": "median_rt_prediction_acc",
        "Average.Peptide.Length": "average_peptide_length",
        "Average.Peptide.Charge": "average_peptide_charge",
        "Average.Missed.Tryptic.Cleavages": "average_missed_tryptic_cleavages",
    }

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics by counting unique values."""
        self._metrics[target_column] = df[source_column].mean()


class PrecursorMedianRT(Metrics):
    """Median retention time of precursors from report.parquet."""

    _file = REPORT_PARQUET_FILE_NAME

    _columns: ClassVar[dict[str, str | None]] = {
        "RT": "median_precursor_rt",
    }

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate median of column."""
        self._metrics[target_column] = df[source_column].median()


def calc_diann_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate DIA-NN metrics."""
    data_store = DataStore(output_directory, file_name_to_read_method_mapping)

    metrics = BasicStats(data_store).get()
    try:
        metrics.update(PrecursorMedianRT(data_store).get())
    except FileNotFoundError:
        logging.warning("report.parquet not found, skipping PrecursorMedianRT metrics")
    return metrics
