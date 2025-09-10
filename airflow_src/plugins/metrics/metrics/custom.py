"""Custom Metrics Module."""

from pathlib import Path


def _calc_custom_metrics(output_directory: Path) -> dict[str, str | int | float]:
    """Calculate custom metrics.

    This method can be extended to add custom metrics calculation logic.
    Return a dictionary of metrics where keys are metric names and values are the metric values.
    """
    del output_directory  # unused

    metrics = {}

    # Add any custom calculations here
    # Example:
    # from metrics.custom_metrics import YourCustomMetricsClass
    # metrics |= YourCustomMetricsClass(output_directory).get()

    return metrics  # noqa: RET504
