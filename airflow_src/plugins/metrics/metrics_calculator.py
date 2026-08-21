"""Calculate metrics.

To extend the metrics, create a new class that inherits from Metrics and implement the _calc() method.
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
from metrics.metrics.alphadia import calc_alphadia_metrics
from metrics.metrics.base import read_csv
from metrics.metrics.custom import calc_custom_metrics
from metrics.metrics.diann import calc_diann_metrics
from metrics.metrics.msqc import calc_msqc_metrics
from metrics.metrics.skyline import calc_skyline_metrics

from shared.keys import MetricsTypes

# optional file in the output directory in which the quanting software can report metrics itself,
# one column per metric. They are stored under the configured metrics type, just like the metrics
# Kraken calculates itself, which is the only way to get metrics out of a software that Kraken has
# no metrics calculation for.
REPORTED_METRICS_FILE_NAME = "metrics.csv"


def calc_metrics(output_directory: Path, *, metrics_type: str) -> dict[str, Any]:
    """Get all metrics for the given output directory, calculated ones and reported ones.

    On a name clash, the metrics the quanting software reported itself win, as it is the
    authority on its own numbers.

    :param output_directory: Path to the output directory
    :param metrics_type: Type of metrics to calculate ("alphadia" or "custom")
    """
    metrics = {
        MetricsTypes.ALPHADIA: calc_alphadia_metrics,
        MetricsTypes.MSQC: calc_msqc_metrics,
        MetricsTypes.SKYLINE: calc_skyline_metrics,
        MetricsTypes.DIANN: calc_diann_metrics,
        MetricsTypes.CUSTOM: calc_custom_metrics,
    }[metrics_type](output_directory)

    calculated_metrics = _clean_metrics(metrics)
    logging.info(f"Calculated {metrics_type} metrics: {calculated_metrics}")

    reported_metrics = _get_reported_metrics(output_directory)

    if overlapping := sorted(set(calculated_metrics) & set(reported_metrics)):
        logging.warning(f"Reported metrics override calculated ones: {overlapping}")

    return calculated_metrics | reported_metrics


def _get_reported_metrics(output_directory: Path) -> dict[str, Any]:
    """Get the metrics the quanting software reported in `REPORTED_METRICS_FILE_NAME`.

    :param output_directory: Path to the output directory
    :return: The metrics of the file's first row, empty if the file does not exist or has no rows.
    """
    file_path = output_directory / REPORTED_METRICS_FILE_NAME
    if not file_path.exists():
        return {}

    df = read_csv(file_path)
    if df.empty:
        logging.warning(f"No rows in {file_path}, ignoring it.")
        return {}
    if len(df) > 1:
        logging.warning(f"Found {len(df)} rows in {file_path}, using the first one.")

    metrics_cleaned = _clean_metrics(df.iloc[0].to_dict())

    logging.info(f"Read reported metrics: {metrics_cleaned}")

    return metrics_cleaned


def _clean_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    """Make metrics keys storable in MongoDB and their values JSON-serializable."""
    # MongoDB field names cannot contain dots ("."), and they must not start with a dollar sign ("$").
    metrics_cleaned = {str(k).replace(".", ":"): v for k, v in metrics.items()}

    # required to prevent TypeError: Object of type float32 is not JSON serializable
    return _convert_numpy_types(metrics_cleaned)


def _convert_numpy_types(data: Any) -> Any:  # noqa: PLR0911 (Too many return statements)
    """Recursively convert numpy types to native Python types for JSON serialization.

    Note: no need to handle tuples or sets since JSON doesn't have native tuple/set types.

    Parameters
    ----------
    data : any
        Data structure potentially containing numpy types

    Returns
    -------
    any
        Same structure with numpy types converted to native Python types

    """
    if isinstance(data, dict):
        return {key: _convert_numpy_types(value) for key, value in data.items()}
    if isinstance(data, list):
        return [_convert_numpy_types(item) for item in data]
    if isinstance(data, float) and np.isnan(data):
        return None
    if isinstance(data, np.generic):
        value = data.item()
        if isinstance(value, float) and np.isnan(value):
            return None
        return value
    if isinstance(data, np.ndarray):
        return data.tolist()
    if isinstance(data, tuple | set):
        raise NotImplementedError("Tuples and sets are not supported in serialization.")
    return data
