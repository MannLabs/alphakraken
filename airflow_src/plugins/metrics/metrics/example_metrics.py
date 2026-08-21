"""Example Metrics Module containing dummy code for adding new metrics.

This module is deliberately not wired into `metrics_calculator.py`, it is a template to copy,
cf. docs/customization.md.
"""

from pathlib import Path


def calc_example_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate example metrics.

    Return a dictionary of metrics where keys are metric names and values are the metric values.
    """
    del output_directory  # unused

    metrics = {}

    # Add any calculations here which return metrics as a dictionary.
    # Example:
    # from metrics.metrics.example_metrics import YourMetricsClass
    # metrics |= YourMetricsClass(output_directory).get()

    return metrics  # noqa: RET504
