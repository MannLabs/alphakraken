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
        self._metrics["irt_delta_rt_mean"] = delta_rt.mean()
        self._metrics["irt_delta_rt_max"] = delta_rt.max()

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
        self._metrics["irt_mean_total_area_median"] = df_heavy["MeanTotalArea"].median()
        self._metrics["irt_cv_total_area_mean"] = df_heavy["CvTotalArea"].mean()
        self._metrics["irt_cv_total_area_max"] = df_heavy["CvTotalArea"].max()

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        pass


def _sanitize_peptide_name(sequence: str) -> str:
    """Sanitize peptide sequence for use as a MongoDB field name."""
    return sequence.replace("[", "_").replace("]", "").replace(".", ":")


class PeptideCountStats(Metrics):
    """Number of detected heavy/light peptides."""

    _file = SKYLINE_REPORT_FILE_NAME
    _columns: ClassVar = {}

    def _calc_metrics(self) -> None:
        df = self._data_store[self._file]
        self._metrics["irt_n_heavy_peptides"] = len(
            df[df["IsotopeLabelType"] == "heavy"]
        )
        self._metrics["irt_n_light_peptides"] = len(
            df[df["IsotopeLabelType"] == "light"]
        )

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        pass


class PerPeptideStats(Metrics):
    """Per-peptide metrics (heavy isotopes only)."""

    _file = SKYLINE_REPORT_FILE_NAME
    _columns: ClassVar = {}

    def _calc_metrics(self) -> None:
        df = self._data_store[self._file]
        df_heavy = df[df["IsotopeLabelType"] == "heavy"]

        for _, row in df_heavy.iterrows():
            name = _sanitize_peptide_name(row["PeptideModifiedSequence"])
            prefix = f"irt_{name}"

            delta_rt = abs(
                row["PredictedRetentionTime"] - row["AverageMeasuredRetentionTime"]
            )
            self._metrics[f"{prefix}_delta_rt"] = delta_rt
            self._metrics[f"{prefix}_mean_total_area"] = row["MeanTotalArea"]
            if not pd.isna(row["CvTotalArea"]):
                self._metrics[f"{prefix}_cv_total_area"] = row["CvTotalArea"]

    def _calc(self, df: pd.DataFrame, source_column: str, target_column: str) -> None:
        pass


def calc_skyline_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate Skyline iRT metrics."""
    data_store = DataStore(output_directory, file_name_to_read_method_mapping)

    metrics = RetentionTimeStats(data_store).get()
    metrics |= AreaStats(data_store).get()
    metrics |= PeptideCountStats(data_store).get()
    metrics |= PerPeptideStats(data_store).get()

    return metrics
