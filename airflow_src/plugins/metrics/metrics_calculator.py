"""Calculate metrics.

To extend the metrics, create a new class that inherits from Metrics and implement the _calc() method.
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
from metrics.metrics.alphadia import calc_alphadia_metrics
from metrics.metrics.custom import calc_custom_metrics
from metrics.metrics.msqc import calc_msqc_metrics

from shared.keys import MetricsTypes


def calc_metrics(output_directory: Path, *, metrics_type: str) -> dict[str, Any]:
    """Calculate metrics for the given output directory.

    :param output_directory: Path to the output directory
    :param metrics_type: Type of metrics to calculate ("alphadia" or "custom")
    """
    metrics = {
        MetricsTypes.ALPHADIA: calc_alphadia_metrics,
        MetricsTypes.MSQC: calc_msqc_metrics,
        MetricsTypes.CUSTOM: calc_custom_metrics,
    }[metrics_type](output_directory)

    # MongoDB field names cannot contain dots (".") or null characters ("\0"), and they must not start with a dollar sign ("$").
    metrics_cleaned = {k.replace(".", ":"): v for k, v in metrics.items()}

    logging.info(f"Calculated {metrics_type} metrics: {metrics_cleaned}")

    # required to prevent TypeError: Object of type float32 is not JSON serializable
    return _convert_numpy_types(metrics_cleaned)


def _convert_numpy_types(data: Any) -> Any:
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
    if isinstance(data, np.generic):
        return data.item()
    if isinstance(data, np.ndarray):
        return data.tolist()
    if isinstance(data, list | set):
        raise NotImplementedError("Tuples and sets are not supported in serialization.")
    return data
