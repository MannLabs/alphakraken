"""Database query and metrics augmentation logic for the REST API."""

from collections import defaultdict
from datetime import datetime
from typing import Any

from shared.db.engine import connect_db
from shared.db.models import Metrics, RawFile

METRICS_INTERNAL_KEYS = {
    "_id",
    "raw_file",
    "created_at_",
    "type",
    "settings_name",
    "settings_version",
}

RAW_FILE_EXCLUDED_KEYS = {"file_info", "backup_base_path", "created_at_"}


def _flatten_metrics(
    nested_dict: dict[str, dict[str, Any]],
) -> dict[str, float | int | str]:
    """Flatten metrics from different types into a single dict with type prefixes for conflicts."""
    flattened = {}
    if not nested_dict:
        return flattened

    for metrics_type, metrics_data in nested_dict.items():
        for key, value in metrics_data.items():
            if key in METRICS_INTERNAL_KEYS:
                continue

            if key not in flattened:
                flattened[key] = value
            else:
                prefixed_key = f"{metrics_type}_{key}"
                flattened[prefixed_key] = value

    return flattened


def query_raw_files(  # noqa: PLR0913
    *,
    instrument_id: str | None = None,
    name_contains: str | None = None,
    project_id: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[RawFile], int]:
    """Query raw files with filtering and pagination.

    Returns:
        Tuple of (paginated raw files, total count matching the filters).

    """
    connect_db()

    query = RawFile.objects.exclude("file_info", "backup_base_path")

    if instrument_id is not None:
        query = query.filter(instrument_id=instrument_id)
    if name_contains is not None:
        query = query.filter(id__icontains=name_contains)
    if project_id is not None:
        query = query.filter(project_id=project_id)
    if date_from is not None:
        query = query.filter(created_at__gte=date_from)
    if date_to is not None:
        query = query.filter(created_at__lte=date_to)

    total = query.count()
    raw_files = list(query.order_by("-created_at").skip(offset).limit(limit))

    return raw_files, total


def augment_with_metrics(raw_files: list[RawFile]) -> dict[str, dict[str, Any]]:
    """Augment raw files with their latest flattened metrics.

    Returns:
        Dict keyed by raw file ID, value is a flat dict of raw file fields + metrics fields.

    """
    raw_files_dict: dict = {
        raw_file_mongo["_id"]: raw_file_mongo
        for raw_file in raw_files
        if (raw_file_mongo := dict(raw_file.to_mongo()))
    }

    if not raw_files_dict:
        return {}

    for metrics_ in Metrics.objects.filter(
        raw_file__in=list(raw_files_dict.keys())
    ).order_by("-created_at_"):
        metrics = dict(metrics_.to_mongo())
        raw_file_id = metrics["raw_file"]

        if "metrics" not in raw_files_dict[raw_file_id]:
            raw_files_dict[raw_file_id]["metrics"] = defaultdict(dict)

        metrics_type = metrics.get("type", "default")

        # keep only the latest metrics per type (query is ordered by -created_at_)
        if metrics_type not in raw_files_dict[raw_file_id]["metrics"]:
            raw_files_dict[raw_file_id]["metrics"][metrics_type] = metrics

    return raw_files_dict


def get_raw_files_with_metrics(  # noqa: PLR0913
    *,
    instrument_id: str | None = None,
    name_contains: str | None = None,
    project_id: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Query raw files and return them with flattened metrics.

    Returns:
        Tuple of (list of raw file dicts with metrics, total count).

    """
    raw_files, total = query_raw_files(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )

    augmented = augment_with_metrics(raw_files)

    results = []
    for raw_file_data in augmented.values():
        metrics = _flatten_metrics(raw_file_data.pop("metrics", None))

        # harmonize legacy field name
        if "raw:gradient_length_m" in metrics:
            metrics["gradient_length"] = metrics.pop("raw:gradient_length_m")

        result = {
            "id": raw_file_data["_id"],
            **{
                k: v
                for k, v in raw_file_data.items()
                if k not in RAW_FILE_EXCLUDED_KEYS and k != "_id"
            },
            **metrics,
        }

        results.append(result)

    return results, total
