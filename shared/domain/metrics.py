"""Metrics domain type."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Metrics:
    """Metrics of one type calculated for a raw file. Datetimes are UTC-aware."""

    raw_file_id: str
    # e.g. "alphadia", cf. `MetricsTypes`
    type: str
    settings_name: str
    settings_version: int
    # relative to the `output` location, cf. path_layout
    relative_output_path: str | None
    # `display_paths.output` at processing time: records which output file system holds the results
    output_display_base_path: str | None
    created_at: datetime
    # the metric values by name; which names exist depends on the type and its calculator version
    values: dict[str, Any]
