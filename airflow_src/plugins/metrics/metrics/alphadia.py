"""Metrics for AlphaDIA."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from metrics.metrics.base import DataStore, Metrics, read_parquet, read_tsv


class OutputFiles:
    """String constants for the output file names."""

    PG_MATRIX = "pg.matrix.parquet"
    PRECURSORS = "precursors.parquet"
    STAT = "stat.tsv"
    INTERNAL = "internal.tsv"
    LOG = "log.txt"


file_name_to_read_method_mapping = {
    OutputFiles.PG_MATRIX: read_parquet,
    OutputFiles.PRECURSORS: read_parquet,
    OutputFiles.STAT: read_tsv,
    OutputFiles.INTERNAL: read_tsv,
}


class BasicStats(Metrics):
    """Basic statistics."""

    _file = OutputFiles.STAT
    _tolerate_missing = True

    _columns = (
        "proteins",
        "precursors",
        "fwhm_rt",
        "fwhm_mobility",
        "optimization.ms1_error",
        "optimization.ms2_error",
        "optimization.rt_error",
        "optimization.mobility_error",
        "calibration.ms1_median_accuracy",
        "calibration.ms2_median_accuracy",
        "raw.gradient_length_m",
    )

    def _calc(self, df: pd.DataFrame, column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{column}"] = df[column].mean()


class InternalStats(Metrics):
    """Internal statistics."""

    _file = OutputFiles.INTERNAL
    _columns = ("duration_optimization", "duration_extraction")
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{column}"] = df[column].mean()


class PrecursorStatsSum(Metrics):
    """Precursor statistics (sum)."""

    _file = OutputFiles.PRECURSORS
    _columns = ("weighted_ms1_intensity", "intensity")
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{column}_sum"] = df[column].sum()


class PrecursorStatsAgg(Metrics):
    """Precursor statistics (aggregates)."""

    _file = OutputFiles.PRECURSORS
    _columns = ("charge", "proba")
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{column}_mean"] = df[column].mean()
        self._metrics[f"{column}_std"] = df[column].std()
        self._metrics[f"{column}_median"] = df[column].median()


class PrecursorStatsIntensity(Metrics):
    """Precursor statistics (intensity)."""

    _file = OutputFiles.PRECURSORS

    def _calc_metrics(self) -> None:
        """Calculate all the metrics."""
        df = self._data_store[self._file]
        try:
            self._metrics["precursor_intensity_median"] = (
                df["sum_b_ion_intensity"] + df["sum_y_ion_intensity"]
            ).median()
        except KeyError as e:
            logging.warning(f"Error calculating precursor_intensity_median: {e}")

    def _calc(self, df: pd.DataFrame, column: str) -> None:
        pass


class PrecursorStatsMeanLenSequence(Metrics):
    """Precursor statistics (mean length sequence)."""

    _file = OutputFiles.PRECURSORS
    _columns = ("sequence",)
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, column: str) -> None:
        """Calculate metrics."""
        sequence_lengths = np.array([len(x) for x in df[column]])

        self._metrics[f"{column}_len_mean"] = sequence_lengths.mean()
        self._metrics[f"{column}_len_std"] = sequence_lengths.std(ddof=1)
        self._metrics[f"{column}_len_median"] = np.median(sequence_lengths)


def calc_alphadia_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate standard alphaDIA metrics."""
    data_store = DataStore(output_directory, file_name_to_read_method_mapping)

    metrics = BasicStats(data_store).get()
    metrics |= PrecursorStatsSum(data_store).get()
    metrics |= PrecursorStatsAgg(data_store).get()
    metrics |= PrecursorStatsIntensity(data_store).get()
    metrics |= PrecursorStatsMeanLenSequence(data_store).get()
    metrics |= InternalStats(data_store).get()

    return metrics
