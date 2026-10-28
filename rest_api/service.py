"""Database query and metrics augmentation logic for the REST API."""

from datetime import datetime
from typing import Any

from shared.db.engine import connect_db
from shared.db.interface import augment_raw_files_with_metrics
from shared.db.models import (
    Project,
    ProjectSettings,
    ProjectStatus,
    RawFile,
    Settings,
    SettingsStatus,
    split_unique_settings_name,
)
from shared.display_paths import get_display_backup_path, get_display_output_path

METRICS_EXCLUDED_KEYS = {
    "_id",
    "raw_file",
    "created_at_",
    "settings_name",
    "settings_version",
    "relative_output_path",  # served resolved as `output_path`
    "output_display_base_path",  # served resolved as `output_path`
}

FILE_INFO_KEY = "file_info"
# file_info paths are relative to this folder, so the two are returned together
BACKUP_PATH_KEY = "backup_path"
OUTPUT_PATH_KEY = "output_path"

RAW_FILE_EXCLUDED_KEYS = {"_id", "created_at_", FILE_INFO_KEY}


def _query_raw_files(  # noqa: PLR0913
    *,
    instrument_id: str | None = None,
    name_contains: str | None = None,
    project_id: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
    include_file_info: bool = False,
) -> tuple[list[RawFile], int]:
    """Query raw files with filtering and pagination.

    Returns:
        Tuple of (paginated raw files, total count matching the filters).

    """
    connect_db()

    query = RawFile.objects
    if not include_file_info:
        query = query.exclude(FILE_INFO_KEY)

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
    """Extract metrics dicts from raw file data into a list, removing internal keys.

    Metrics entries are identified by having a "type" key (added by augment_raw_files_with_metrics).
    """
    metrics_list = []
    for key in list(raw_file_data):
        value = raw_file_data[key]
        if not isinstance(value, dict) or "type" not in value:
            continue
        raw_file_data.pop(key)

        # harmonize legacy field name
        if "raw:gradient_length_m" in value:
            value["gradient_length"] = value.pop("raw:gradient_length_m")

        if (relative_output_path := value.get("relative_output_path")) is not None:
            value[OUTPUT_PATH_KEY] = get_display_output_path(
                relative_output_path, value.get("output_display_base_path")
            )

        metrics_list.append(
            {k: v for k, v in value.items() if k not in METRICS_EXCLUDED_KEYS}
        )
    return metrics_list


def _to_raw_file_dict(
    raw_file_data: dict[str, Any], *, excluded_keys: set[str]
) -> dict[str, Any]:
    """Build a raw file response dict from augmented data, excluding internal keys."""
    return {
        "id": raw_file_data["_id"],
        **{k: v for k, v in raw_file_data.items() if k not in excluded_keys},
    }


def get_raw_files_with_metrics(  # noqa: PLR0913
    *,
    instrument_id: str | None = None,
    name_contains: str | None = None,
    project_id: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
    include_metrics: bool = True,
    include_file_info: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    """Query raw files and return them, optionally with metrics.

    Returns:
        Tuple of (list of raw file dicts, total count). Each dict carries a "metrics"
        list when include_metrics is True, and no "metrics" key otherwise. The
        "file_info" mapping and "backup_path", the folder its paths are relative to,
        are included only when include_file_info is True.

    """
    raw_files, total = _query_raw_files(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
        include_file_info=include_file_info,
    )

    excluded_keys = RAW_FILE_EXCLUDED_KEYS
    if include_file_info:
        excluded_keys = excluded_keys - {FILE_INFO_KEY}

    if include_metrics:
        results = []
        for raw_file_data in augment_raw_files_with_metrics(
            raw_files, prefix=""
        ).values():
            metrics = _to_metrics_list(raw_file_data)
            results.append(
                {
                    **_to_raw_file_dict(raw_file_data, excluded_keys=excluded_keys),
                    "metrics": metrics,
                }
            )
    else:
        results = [
            _to_raw_file_dict(dict(raw_file.to_mongo()), excluded_keys=excluded_keys)
            for raw_file in raw_files
        ]

    if include_file_info:
        for raw_file, result in zip(raw_files, results, strict=True):
            result[BACKUP_PATH_KEY] = get_display_backup_path(raw_file)

    return results, total


def _project_to_dict(project: Project) -> dict[str, Any]:
    data = project.to_mongo().to_dict()
    data["id"] = data.pop("_id")
    return data


def _settings_to_dict(settings: Settings) -> dict[str, Any]:
    """Convert settings to a dict, with the unique name as `id` instead of the ObjectId."""
    data = settings.to_mongo().to_dict()
    data.pop("_id", None)
    return {"id": settings.unique_name, **data}


def _assignment_to_dict(ps: ProjectSettings) -> dict[str, Any]:
    return {
        "settings_id": ps.settings.unique_name,
        "name": ps.settings.name,
        "version": ps.settings.version,
        "scopes": ps.scopes,
        "excluded_scopes": ps.excluded_scopes,
        "raw_file_id_filter": ps.raw_file_id_filter,
        "raw_file_id_exclude_filter": ps.raw_file_id_exclude_filter,
    }


def get_projects(
    *, project_id: str | None = None, limit: int = 100, offset: int = 0
) -> tuple[list[dict[str, Any]], int]:
    """Query non-deleted projects with their settings assignments.

    Returns:
        Tuple of (list of project dicts, total count matching the filters).

    """
    connect_db()

    query = Project.objects(status__ne=ProjectStatus.DELETED)
    if project_id is not None:
        query = query.filter(id=project_id)

    total = query.count()
    projects = list(query.order_by("-created_at_").skip(offset).limit(limit))

    assignments: dict[str, list[dict[str, Any]]] = {p.id: [] for p in projects}
    for ps in ProjectSettings.objects(project__in=list(assignments)).order_by(
        "created_at_"
    ):
        assignments[ps.project.id].append(_assignment_to_dict(ps))

    return [
        {**_project_to_dict(p), "settings": assignments[p.id]} for p in projects
    ], total


def get_project(project_id: str) -> dict[str, Any] | None:
    """Get a non-deleted project by ID, or None if it does not exist."""
    projects, _ = get_projects(project_id=project_id, limit=1)
    return projects[0] if projects else None


def get_settings(
    *, name: str | None = None, limit: int = 100, offset: int = 0
) -> tuple[list[dict[str, Any]], int]:
    """Query non-deleted settings.

    Returns:
        Tuple of (list of settings dicts, total count matching the filters).

    """
    connect_db()

    query = Settings.objects(status__ne=SettingsStatus.DELETED)
    if name is not None:
        query = query.filter(name=name)

    total = query.count()
    settings = query.order_by("-created_at_").skip(offset).limit(limit)

    return [_settings_to_dict(s) for s in settings], total


def get_settings_by_unique_name(unique_name: str) -> dict[str, Any] | None:
    """Get non-deleted settings by unique name (e.g. `fallback_v7`), or None if it does not exist."""
    connect_db()
    try:
        name, version = split_unique_settings_name(unique_name)
    except ValueError:
        return None
    settings = Settings.objects(
        name=name, version=version, status__ne=SettingsStatus.DELETED
    ).first()
    return None if settings is None else _settings_to_dict(settings)
