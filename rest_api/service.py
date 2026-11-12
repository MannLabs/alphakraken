"""Database query and metrics augmentation logic for the REST API."""

from datetime import datetime
from typing import Any

from shared.db import metrics as metrics_store
from shared.db import project_settings as project_settings_store
from shared.db import projects as project_store
from shared.db import raw_files as raw_file_store
from shared.db import settings as settings_store
from shared.display_paths import get_display_backup_path, get_display_output_path
from shared.domain.errors import NotFoundError
from shared.domain.metrics import Metrics
from shared.domain.projects import Project, ProjectSettings
from shared.domain.raw_files import RawFile
from shared.domain.settings import Settings, SettingsStatus

FILE_INFO_KEY = "file_info"
# file_info paths are relative to this folder, so the two are returned together
BACKUP_PATH_KEY = "backup_path"
OUTPUT_PATH_KEY = "output_path"
METRICS_TYPE_KEY = "type"

RAW_FILE_EXCLUDED_KEYS = {"id", "created_at_", FILE_INFO_KEY}


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
    where = raw_file_store.RawFileFilter(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        created_after=date_from,
        created_before=date_to,
    )
    found = raw_file_store.find(
        where, limit=limit, offset=offset, include_file_info=include_file_info
    )
    return found, raw_file_store.count(where)


def _to_metrics_dict(type_metrics: Metrics) -> dict[str, Any]:
    """Build a metrics response dict: the type, the values and the output folder as users see it."""
    data = {METRICS_TYPE_KEY: type_metrics.type, **type_metrics.values}
    if type_metrics.relative_output_path is not None:
        data[OUTPUT_PATH_KEY] = get_display_output_path(
            type_metrics.relative_output_path, type_metrics.output_display_base_path
        )
    return data


def _to_raw_file_dict(raw_file: RawFile, *, excluded_keys: set[str]) -> dict[str, Any]:
    """Build a raw file response dict, excluding internal keys and unset fields."""
    return {
        "id": raw_file.id,
        **{
            k: v
            for k, v in vars(raw_file).items()
            if k not in excluded_keys and v is not None
        },
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
    found, total = _query_raw_files(
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

    results = [
        _to_raw_file_dict(raw_file, excluded_keys=excluded_keys) for raw_file in found
    ]

    if include_metrics:
        latest_metrics = metrics_store.latest_by_raw_file(
            [raw_file.id for raw_file in found]
        )
        for raw_file, result in zip(found, results, strict=True):
            result["metrics"] = [
                _to_metrics_dict(type_metrics)
                for type_metrics in latest_metrics.get(raw_file.id, {}).values()
            ]

    if include_file_info:
        for raw_file, result in zip(found, results, strict=True):
            result[BACKUP_PATH_KEY] = get_display_backup_path(raw_file)

    return results, total


def _to_dict(entity: Project | Settings) -> dict[str, Any]:
    """Build a response dict from a project or settings, leaving out unset fields."""
    return {k: v for k, v in vars(entity).items() if v is not None}


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
    ids = None if project_id is None else [project_id]
    found = project_store.find(ids=ids, limit=limit, offset=offset)
    total = project_store.count(ids=ids)

    assignments: dict[str, list[dict[str, Any]]] = {p.id: [] for p in found}
    for ps in project_settings_store.find(list(assignments)):
        assignments[ps.project_id].append(_assignment_to_dict(ps))

    return [{**_to_dict(p), "settings": assignments[p.id]} for p in found], total


def get_project(project_id: str) -> dict[str, Any] | None:
    """Get a non-deleted project by ID, or None if it does not exist."""
    found, _ = get_projects(project_id=project_id, limit=1)
    return found[0] if found else None


def get_settings(
    *, name: str | None = None, limit: int = 100, offset: int = 0
) -> tuple[list[dict[str, Any]], int]:
    """Query non-deleted settings.

    Returns:
        Tuple of (list of settings dicts, total count matching the filters).

    """
    found = settings_store.find(name=name, limit=limit, offset=offset)
    return [_to_dict(s) for s in found], settings_store.count(name=name)


def get_settings_by_unique_name(unique_name: str) -> dict[str, Any] | None:
    """Get non-deleted settings by unique name (e.g. `fallback_v7`), or None if it does not exist."""
    try:
        found = settings_store.get(unique_name)
    except NotFoundError:
        return None
    return None if found.status == SettingsStatus.DELETED else _to_dict(found)
