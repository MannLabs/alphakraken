"""Calculate metrics.

To extend the metrics, create a new class that inherits from Metrics and implement the _calc() method.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _load_tsv(file_path: Path) -> pd.DataFrame:
    """Load a tsv file."""
    return pd.read_csv(file_path, sep="\t")


class OutputFiles:
    """String constants for the output file names."""

    PG_MATRIX = "pg.matrix.tsv"
    PRECURSORS = "precursors.tsv"
    STAT = "stat.tsv"
    INTERNAL = "internal.tsv"
    LOG = "log.txt"


file_name_to_read_method_mapping = {
    OutputFiles.PG_MATRIX: _load_tsv,
    OutputFiles.PRECURSORS: _load_tsv,
    OutputFiles.STAT: _load_tsv,
    OutputFiles.INTERNAL: _load_tsv,
}


class DataStore:
    """Data store to read and cache data."""

    def __init__(self, data_path: Path):
        """Initialize the data store.

         Output files defined in `file_name_to_read_method_mapping` can be accessed as attributes, e.g.
            `stat_df = DataStore('/home/output')["stat.tsv"]`

        :param data_path: Absolute path to the directory containing alphaDIA output data.
        """
        self._data_path = data_path
        self._data = {}

    def __getitem__(self, key: str) -> pd.DataFrame:
        """Get data from the data store."""
        if key not in self._data:
            file_path = self._data_path / key
            logging.info(f"loading {file_path}")
            self._data[key] = file_name_to_read_method_mapping[key](file_path)
        return self._data[key]


class Metrics(ABC):
    """Abstract class for metrics."""

    _file: str
    _columns: list[str]
    _tolerate_missing: bool = False

    def __init__(self, data_store: DataStore):
        """Initialize Metrics.

        :param data_store: Data store to get the data from.
        """
        self._data_store = data_store
        self._metrics = {}
        self._name = self.__class__.__name__

    def get(self) -> dict[str, Any]:
        """Get the metrics."""
        if not self._metrics:
            self._calc_metrics()

        return self._metrics

    def _calc_metrics(self) -> None:
        """Calculate all the metrics."""
        df = self._data_store[self._file]

        for col in self._columns:
            try:
                self._calc(df, col)
            except KeyError as e:  # noqa: PERF203
                if not self._tolerate_missing:
                    raise e from e
                logging.warning(f"Column {col} not found in {df.columns}. Error: {e}")

    @abstractmethod
    def _calc(self, df: pd.DataFrame, column: str) -> None:
        """Calculate a single metrics."""
        raise NotImplementedError


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


class PrecursorStatsMean(Metrics):
    """Precursor statistics (mean)."""

    _file = OutputFiles.PRECURSORS
    _columns = ("charge",)
    _tolerate_missing = True

    def _calc(self, df: pd.DataFrame, column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"{column}_mean"] = df[column].mean()
        self._metrics[f"{column}_std"] = df[column].std()


class PrecursorStatsIntensity(Metrics):
    """Precursor statistics (intensity)."""

    _file = OutputFiles.PRECURSORS

    def _calc_metrics(self) -> None:
        """Calculate all the metrics."""
        df = self._data_store[self._file]
        self._metrics["precursor_intensity_median"] = (
            df["sum_b_ion_intensity"] + df["sum_y_ion_intensity"]
        ).median()

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


def calc_metrics(output_directory: Path) -> dict[str, Any]:
    """Calculate metrics for the given output directory."""
    data_store = DataStore(output_directory)

    metrics = BasicStats(data_store).get()
    metrics |= PrecursorStatsSum(data_store).get()
    metrics |= PrecursorStatsMean(data_store).get()
    metrics |= PrecursorStatsIntensity(data_store).get()
    metrics |= PrecursorStatsMeanLenSequence(data_store).get()
    metrics |= InternalStats(data_store).get()

    # MongoDB field names cannot contain dots (".") or null characters ("\0"), and they must not start with a dollar sign ("$").
    metrics_cleaned = {k.replace(".", ":"): v for k, v in metrics.items()}

    logging.info(f"Calculated metrics: {metrics_cleaned}")
    return metrics_cleaned
