"""Metrics for DIA-NN."""

from pathlib import Path
from typing import ClassVar

import pandas as pd
from metrics.metrics.base import DataStore, Metrics, read_tsv

REPORT_FILE_NAME = "report.stats.tsv"

file_name_to_read_method_mapping = {
    REPORT_FILE_NAME: read_tsv,
}


class BasicStats(Metrics):
    """Basic statistics from DIA-NN report."""

    _file = REPORT_FILE_NAME

    _columns: ClassVar[dict[str, str | None]] = {
        "Proteins.Identified": "proteins",
        "Precursors.Identified": "peptides",
    }

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics by counting unique values."""
        self._metrics[target_column] = df[source_column].mean()


def calc_diann_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate DIA-NN metrics."""
    data_store = DataStore(output_directory, file_name_to_read_method_mapping)

    return BasicStats(data_store).get()
