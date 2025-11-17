"""Base class for metrics."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd


def read_tsv(file_path: Path) -> pd.DataFrame:
    """Read a tsv file."""
    return pd.read_csv(file_path, sep="\t")


def read_parquet(file_path: Path) -> pd.DataFrame:
    """Read a parquet file."""
    return pd.read_parquet(file_path)


class DataStore:
    """Data store to read and cache data."""

    def __init__(self, data_path: Path, file_name_to_read_method_mapping: dict):
        """Initialize the data store.

         Output files defined in `file_name_to_read_method_mapping` can be accessed as attributes, e.g.
            `stat_df = DataStore('/home/output')["stat.tsv"]`

        :param data_path: Absolute path to the directory containing alphaDIA output data.
        :param file_name_to_read_method_mapping: Mapping of file names to read methods.
        """
        self._file_name_to_read_method_mapping = file_name_to_read_method_mapping
        self._data_path = data_path
        self._data = {}

    def __getitem__(self, key: str) -> pd.DataFrame:
        """Get data from the data store."""
        if key not in self._data:
            file_path = self._data_path / key
            logging.info(f"loading {file_path}")
            self._data[key] = self._file_name_to_read_method_mapping[key](file_path)
        return self._data[key]


class Metrics(ABC):
    """Abstract class for metrics."""

    _file: str
    _columns: ClassVar[dict[str, str | None]]
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
        """Calculate all the metrics.

        If _columns is a dict, keys are source column names (from the file)
        and values are target column names (for internal use). If value is None,
        the source column name is used as-is.
        """
        df = self._data_store[self._file]

        if isinstance(self._columns, dict):
            for source_col, target_col in self._columns.items():
                target_col_ = target_col if target_col is not None else source_col
                try:
                    self._calc(df, source_col, target_col_)
                except KeyError as e:
                    if not self._tolerate_missing:
                        raise e from e
                    logging.warning(
                        f"Column {source_col} not found in {df.columns}. Error: {e}"
                    )
        else:
            for col in self._columns:
                try:
                    self._calc(df, col, col)
                except KeyError as e:  # noqa: PERF203
                    if not self._tolerate_missing:
                        raise e from e
                    logging.warning(
                        f"Column {col} not found in {df.columns}. Error: {e}"
                    )

    @abstractmethod
    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate a single metrics.

        :param df: DataFrame containing the data
        :param source_column: Column name in the DataFrame
        :param target_column: Column name to use for metrics keys
        """
        raise NotImplementedError
