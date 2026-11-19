"""REST API endpoint definitions."""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Query

from rest_api.schemas import (
    ProjectResponse,
    ProjectsListResponse,
    RawFileResponse,
    RawFilesListResponse,
    SettingsListResponse,
    SettingsResponse,
)
from rest_api.service import (
    get_project,
    get_projects,
    get_raw_files_with_metrics,
    get_settings,
    get_settings_by_unique_name,
)

router = APIRouter()

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


@router.get("/raw_files/")
def list_raw_files(  # noqa: PLR0913
    instrument_id: Annotated[
        str | None, Query(description="Filter by instrument ID (exact match)")
    ] = None,
    name_contains: Annotated[
        str | None,
        Query(description="Case-insensitive substring search on raw file ID"),
    ] = None,
    project_id: Annotated[
        str | None, Query(description="Filter by project ID (exact match)")
    ] = None,
    date_from: Annotated[
        datetime | None, Query(description="Filter: created_at >= date_from (ISO 8601)")
    ] = None,
    date_to: Annotated[
        datetime | None, Query(description="Filter: created_at <= date_to (ISO 8601)")
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0,
    *,
    include_metrics: Annotated[
        bool, Query(description="Include flattened metrics for each raw file")
    ] = True,
    include_file_info: Annotated[
        bool,
        Query(
            description="Include the file_info mapping and backup_path for each raw file"
        ),
    ] = False,
) -> RawFilesListResponse:
    """List raw files with optional filtering, including flattened metrics."""
    data, total = get_raw_files_with_metrics(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
        include_metrics=include_metrics,
        include_file_info=include_file_info,
    )

    return RawFilesListResponse(
        total=total,
        offset=offset,
        limit=limit,
        data=[RawFileResponse.of(item) for item in data],
    )


@router.get("/projects/")
def list_projects(
    project_id: Annotated[
        str | None, Query(description="Filter by project ID (exact match)")
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0,
) -> ProjectsListResponse:
    """List non-deleted projects with their settings assignments."""
    data, total = get_projects(project_id=project_id, limit=limit, offset=offset)
    return ProjectsListResponse(
        total=total,
        offset=offset,
        limit=limit,
        data=[ProjectResponse.of(item) for item in data],
    )


@router.get("/projects/{project_id}")
def read_project(project_id: str) -> ProjectResponse:
    """Get a project by ID."""
    return ProjectResponse.of(get_project(project_id))


@router.get("/settings/")
def list_settings(
    name: Annotated[
        str | None, Query(description="Filter by settings name (exact match)")
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0,
) -> SettingsListResponse:
    """List non-deleted settings."""
    data, total = get_settings(name=name, limit=limit, offset=offset)
    return SettingsListResponse(
        total=total,
        offset=offset,
        limit=limit,
        data=[SettingsResponse.model_validate(s) for s in data],
    )


@router.get("/settings/{settings_id}")
def read_settings(settings_id: str) -> SettingsResponse:
    """Get settings by ID, i.e. `<name>_v<version>`."""
    return SettingsResponse.model_validate(get_settings_by_unique_name(settings_id))
