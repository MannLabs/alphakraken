"""Metrics for AlphaDIA."""

import logging
from pathlib import Path
from typing import ClassVar

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

    _columns: ClassVar[dict[str, str | None]] = {
        "proteins": None,  # alphadia < 2
        "precursors": None,  # alphadia < 2
        "search.proteins": "proteins",  # alphadia >= 2
        "search.precursors": "precursors",  # alphadia >= 2
        "search.fwhm_rt": "fwhm_rt",  # alphadia >= 2
        "search.fwhm_mobility": "fwhm_mobility",  # alphadia >= 2
        "optimization.ms1_error": None,
        "optimization.ms2_error": None,
        "optimization.rt_error": None,
        "optimization.mobility_error": None,
        "calibration.ms1_median_accuracy": "calibration.ms1_bias",  # alphadia < 2
        "calibration.ms2_median_accuracy": "calibration.ms2_bias",  # alphadia < 2
        "calibration.ms1_bias": None,  # alphadia >= 2
        "calibration.ms2_bias": None,  # alphadia >= 2
        "raw.gradient_length_m": "gradient_length",  # alphadia < 2
        "raw.gradient_length": "gradient_length",  # alphadia >= 2
    }

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics."""
        # gradient_length conversion for alphadia > 2  # TODO: this is a hack!
        factor = 1 / 60 if source_column == "raw.gradient_length" else 1

        self._metrics[f"{target_column}"] = df[source_column].mean() * factor


class InternalStats(Metrics):
    """Internal statistics."""

    _file = OutputFiles.INTERNAL
    _columns: ClassVar[dict[str, str | None]] = {
        "duration_optimization": None,
        "duration_extraction": None,
    }
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{target_column}"] = df[source_column].mean()


class PrecursorStatsSum(Metrics):
    """Precursor statistics (sum)."""

    _file = OutputFiles.PRECURSORS
    _columns: ClassVar[dict[str, str | None]] = {
        "weighted_ms1_intensity": None,
        "intensity": "pg.intensity",
        "pg.intensity": None,
    }
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{target_column}_sum"] = df[source_column].sum()


class PrecursorStatsAgg(Metrics):
    """Precursor statistics (aggregates)."""

    _file = OutputFiles.PRECURSORS
    _columns: ClassVar[dict[str, str | None]] = {
        "precursor.charge": None,
        "precursor.proba": None,
        "charge": "precursor.charge",
        "proba": "precursor.proba",
    }
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{target_column}_mean"] = df[source_column].mean()
        self._metrics[f"{target_column}_std"] = df[source_column].std()
        self._metrics[f"{target_column}_median"] = df[source_column].median()


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

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        pass


class PrecursorStatsMeanLenSequence(Metrics):
    """Precursor statistics (mean length sequence)."""

    _file = OutputFiles.PRECURSORS
    _columns: ClassVar[dict[str, str | None]] = {
        "precursor.sequence": None,
        "sequence": "precursor.sequence",
    }
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics."""
        sequence_lengths = np.array([len(x) for x in df[source_column]])

        self._metrics[f"{target_column}_len_mean"] = sequence_lengths.mean()
        self._metrics[f"{target_column}_len_std"] = sequence_lengths.std(ddof=1)
        self._metrics[f"{target_column}_len_median"] = np.median(sequence_lengths)


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
