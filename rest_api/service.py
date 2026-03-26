"""Database query and metrics augmentation logic for the REST API."""

from datetime import datetime
from typing import Any

from shared.db.engine import connect_db
from shared.db.interface import augment_raw_files_with_metrics
from shared.db.models import RawFile

METRICS_INTERNAL_KEYS = {
    "_id",
    "raw_file",
    "created_at_",
    "settings_name",
    "settings_version",
}

RAW_FILE_EXCLUDED_KEYS = {"file_info", "backup_base_path", "created_at_"}


def _query_raw_files(  # noqa: PLR0913
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


def _to_metrics_list(raw_file_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract metrics dicts from raw file data into a list, removing internal keys."""
    metrics_list = []
    for key in list(raw_file_data):
        if not isinstance(raw_file_data[key], dict):
            continue
        metrics = raw_file_data.pop(key)

        # harmonize legacy field name
        if "raw:gradient_length_m" in metrics:
            metrics["gradient_length"] = metrics.pop("raw:gradient_length_m")

        metrics_list.append(
            {k: v for k, v in metrics.items() if k not in METRICS_INTERNAL_KEYS}
        )
    return metrics_list


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
    """Query raw files and return them with metrics.

    Returns:
        Tuple of (list of raw file dicts with metrics list, total count).

    """
    raw_files, total = _query_raw_files(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )

    augmented = augment_raw_files_with_metrics(raw_files, prefix="")

    results = []
    for raw_file_data in augmented.values():
        metrics = _to_metrics_list(raw_file_data)

        result = {
            "id": raw_file_data["_id"],
            **{
                k: v
                for k, v in raw_file_data.items()
                if k not in RAW_FILE_EXCLUDED_KEYS and k != "_id"
            },
            "metrics": metrics,
        }

        results.append(result)

    return results, total
