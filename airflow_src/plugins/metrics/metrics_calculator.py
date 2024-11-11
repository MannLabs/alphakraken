"""Calculate metrics.

To extend the metrics, create a new class that inherits from Metrics and implement the _calc() method.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd


def _load_tsv(file_path: str) -> pd.DataFrame:
    """Load a tsv file."""
    return pd.read_csv(file_path, sep="\t")


class OutputFiles:
    """String constants for the output file names."""

    PG_MATRIX = "pg.matrix.tsv"
    PRECURSORS = "precursors.tsv"
    STAT = "stat.tsv"
    LOG = "log.txt"


file_name_to_read_method_mapping = {
    OutputFiles.PG_MATRIX: _load_tsv,
    OutputFiles.PRECURSORS: _load_tsv,
    OutputFiles.STAT: _load_tsv,
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
            self._calc()
        return self._metrics

    @abstractmethod
    def _calc(self) -> None:
        """Calculate the metrics."""
        raise NotImplementedError


class BasicStats(Metrics):
    """Basic statistics."""

    def _calc(self) -> None:
        """Calculate metrics."""
        stat_df = self._data_store[OutputFiles.STAT]

        for col in ["proteins", "precursors", "ms1_accuracy", "fwhm_rt"]:
            self._metrics[f"{col}"] = stat_df[col].mean()


class PrecursorStats(Metrics):
    """Precursor statistics."""

    def _calc(self) -> None:
        """Calculate metrics."""
        stat_df = self._data_store[OutputFiles.PRECURSORS]

        for col in ["weighted_ms1_intensity", "intensity"]:
            try:
                self._metrics[f"{col}_sum"] = stat_df[col].sum()
            except KeyError as e:  # noqa: PERF203
                logging.warning(
                    f"Column {col} not found in {stat_df.columns}. Error: {e}"
                )


def calc_metrics(output_directory: Path) -> dict[str, Any]:
    """Calculate metrics for the given output directory."""
    data_store = DataStore(output_directory)
    metrics = BasicStats(data_store).get()
    metrics |= PrecursorStats(data_store).get()

    logging.info(f"Calculated metrics: {metrics}")
    return metrics
