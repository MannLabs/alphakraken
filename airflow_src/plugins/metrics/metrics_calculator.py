"""Calculate metrics.

To extend the metrics, create a new class that inherits from Metrics and implement the _calc() method.
"""

import logging
from pathlib import Path
from typing import Any

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
    return metrics_cleaned
