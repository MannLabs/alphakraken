"""Response shapes shared by the read APIs (REST API, MCP server)."""

from typing import Any

from shared.display_paths import get_display_output_path
from shared.domain.metrics import Metrics

OUTPUT_PATH_KEY = "output_path"
METRICS_TYPE_KEY = "type"


def to_metrics_dict(metrics: Metrics) -> dict[str, Any]:
    """Build a metrics response dict: the type, the values and the output folder as users see it."""
    data = {METRICS_TYPE_KEY: metrics.type, **metrics.values}
    if (output_path := get_display_output_path(metrics)) is not None:
        data[OUTPUT_PATH_KEY] = output_path
    return data
