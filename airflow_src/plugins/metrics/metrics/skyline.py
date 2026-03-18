"""Skyline iRT metrics calculation module."""

from pathlib import Path
from typing import ClassVar

import numpy as np
import pandas as pd
from metrics.metrics.base import DataStore, Metrics

SKYLINE_REPORT_FILE_NAME = "custom_iRT_report.csv"


def read_csv(file_path: Path) -> pd.DataFrame:
    """Read a CSV file."""
    return pd.read_csv(file_path, sep=",")


file_name_to_read_method_mapping = {
    SKYLINE_REPORT_FILE_NAME: read_csv,
}


class RetentionTimeStats(Metrics):
    """Retention time metrics (heavy isotopes only)."""

    _file = SKYLINE_REPORT_FILE_NAME
    _columns: ClassVar = {}

    def _calc_metrics(self) -> None:
        """Calculate retention time metrics from Skyline iRT report."""
        df = self._data_store[self._file]
        df_heavy = df[df["IsotopeLabelType"] == "heavy"]

        delta_rt = np.abs(
            df_heavy["PredictedRetentionTime"]
            - df_heavy["AverageMeasuredRetentionTime"]
        )
        self._metrics["irt_delta_rt_mean"] = delta_rt.nanmean()
        self._metrics["irt_delta_rt_max"] = delta_rt.nanmax()

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        pass


class AreaStats(Metrics):
    """Peak area metrics (heavy isotopes only)."""

    _file = SKYLINE_REPORT_FILE_NAME
    _columns: ClassVar = {}

    def _calc_metrics(self) -> None:
        """Calculate peak area metrics from Skyline iRT report."""
        df = self._data_store[self._file]
        df_heavy = df[df["IsotopeLabelType"] == "heavy"]
        self._metrics["irt_mean_total_area_median"] = df_heavy[
            "MeanTotalArea"
        ].nanmedian()
        self._metrics["irt_cv_total_area_mean"] = df_heavy["CvTotalArea"].nanmean()
        self._metrics["irt_cv_total_area_max"] = df_heavy["CvTotalArea"].nanmax()

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        pass


def calc_skyline_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate Skyline iRT metrics."""
    data_store = DataStore(output_directory, file_name_to_read_method_mapping)

    metrics = RetentionTimeStats(data_store).get()
    metrics |= AreaStats(data_store).get()

    return metrics
