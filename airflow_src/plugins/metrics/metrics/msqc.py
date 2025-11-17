"""MSQC metrics calculation module."""

from pathlib import Path

import pandas as pd
from metrics.metrics.base import DataStore, Metrics, read_tsv


class OutputFiles:
    """String constants for the output file names."""

    RESULTS = "msqc_results.tsv"


file_name_to_read_method_mapping = {
    OutputFiles.RESULTS: read_tsv,
}


class BasicStats(Metrics):
    """Basic statistics."""

    _file = OutputFiles.RESULTS
    _tolerate_missing = True

    _columns = (
        "ms1_median_injection_time",
        "ms1_scans",
        "ms1_median_tic",
        "ms2_median_injection_time",
        "ms2_scans",
        "ms2_median_tic",
        "evosep_pump_hp_pressure_max",
    )

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        """Calculate metrics."""
        self._metrics[f"msqc_{target_column}"] = df[source_column].mean()


def calc_msqc_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate MSQC metrics."""
    data_store = DataStore(output_directory, file_name_to_read_method_mapping)

    return BasicStats(data_store).get()
