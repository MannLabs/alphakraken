"""Database query and metrics augmentation logic for the REST API."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from shared.api_responses import to_metrics_dict
from shared.db import metrics as metrics_store
from shared.db import project_settings as project_settings_store
from shared.db import projects as project_store
from shared.db import raw_files as raw_file_store
from shared.db import settings as settings_store
from shared.display_paths import get_display_backup_path
from shared.domain.errors import NotFoundError
from shared.domain.projects import Project, ProjectSettings
from shared.domain.raw_files import RawFile
from shared.domain.settings import Settings, SettingsStatus


@dataclass(frozen=True)
class RawFileWithMetrics:
    """A raw file with the data joined to it; what was not requested is None."""

    raw_file: RawFile
    metrics: list[dict[str, Any]] | None
    # file_info paths are relative to this folder, so the two are returned together
    backup_path: str | None


@dataclass(frozen=True)
class ProjectWithAssignments:
    """A project with its settings assignments."""

    project: Project
    assignments: list[ProjectSettings]


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
) -> tuple[list[RawFileWithMetrics], int]:
    """Query raw files and return them, optionally with metrics.

    Returns:
        Tuple of (raw files, total count). `metrics` is None unless include_metrics is
        True. The `file_info` of the raw file and `backup_path`, the folder its paths
        are relative to, are None unless include_file_info is True.

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

    latest_metrics = (
        metrics_store.latest_by_raw_file([raw_file.id for raw_file in found])
        if include_metrics
        else {}
    )
    results = [
        RawFileWithMetrics(
            raw_file=raw_file,
            metrics=[
                to_metrics_dict(type_metrics)
                for type_metrics in latest_metrics.get(raw_file.id, {}).values()
            ]
            if include_metrics
            else None,
            backup_path=get_display_backup_path(raw_file)
            if include_file_info
            else None,
        )
        for raw_file in found
    ]

    return results, total


def get_projects(
    *, project_id: str | None = None, limit: int = 100, offset: int = 0
) -> tuple[list[ProjectWithAssignments], int]:
    """Query non-deleted projects with their settings assignments.

    Returns:
        Tuple of (projects, total count matching the filters).

    """
    ids = None if project_id is None else [project_id]
    found = project_store.find(ids=ids, limit=limit, offset=offset)
    total = project_store.count(ids=ids)

    assignments: dict[str, list[ProjectSettings]] = {p.id: [] for p in found}
    for ps in project_settings_store.find(list(assignments)):
        assignments[ps.project_id].append(ps)

    return [ProjectWithAssignments(p, assignments[p.id]) for p in found], total


def get_project(project_id: str) -> ProjectWithAssignments:
    """Get a non-deleted project by ID.

    Raises:
        NotFoundError: if there is none.

    """
    found, _ = get_projects(project_id=project_id, limit=1)
    if not found:
        raise NotFoundError(f"No project {project_id}")
    return found[0]


def get_settings(
    *, name: str | None = None, limit: int = 100, offset: int = 0
) -> tuple[list[Settings], int]:
    """Query non-deleted settings.

    Returns:
        Tuple of (settings, total count matching the filters).

    """
    found = settings_store.find(name=name, limit=limit, offset=offset)
    return found, settings_store.count(name=name)


def get_settings_by_unique_name(unique_name: str) -> Settings:
    """Get non-deleted settings by unique name (e.g. `fallback_v7`).

    Raises:
        NotFoundError: if there are none, also if `unique_name` is not of that form.

    """
    found = settings_store.get(unique_name)
    if found.status == SettingsStatus.DELETED:
        raise NotFoundError(f"No settings {unique_name}")
    return found
