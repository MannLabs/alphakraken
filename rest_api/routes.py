"""REST API endpoint definitions."""

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, status

from rest_api.schemas import (
    ProjectResponse,
    ProjectsListResponse,
    RawFilesListResponse,
    SettingsListResponse,
    SettingsResponse,
)
from rest_api.service import (
    get_project,
    get_projects,
    get_raw_files_with_metrics,
    get_settings,
    get_settings_by_id,
)

router = APIRouter()

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


@router.get("/raw_files/", response_model=RawFilesListResponse)
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
) -> dict[str, Any]:
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

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "data": data,
    }


@router.get("/projects/", response_model=ProjectsListResponse)
def list_projects(
    project_id: Annotated[
        str | None, Query(description="Filter by project ID (exact match)")
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0,
) -> dict[str, Any]:
    """List non-deleted projects with their settings assignments."""
    data, total = get_projects(project_id=project_id, limit=limit, offset=offset)
    return {"total": total, "offset": offset, "limit": limit, "data": data}


@router.get("/projects/{project_id}", response_model=ProjectResponse)
def read_project(project_id: str) -> dict[str, Any]:
    """Get a project by ID."""
    project = get_project(project_id)
    if project is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Project '{project_id}' not found"
        )
    return project


@router.get("/settings/", response_model=SettingsListResponse)
def list_settings(
    name: Annotated[
        str | None, Query(description="Filter by settings name (exact match)")
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0,
) -> dict[str, Any]:
    """List non-deleted settings."""
    data, total = get_settings(name=name, limit=limit, offset=offset)
    return {"total": total, "offset": offset, "limit": limit, "data": data}


@router.get("/settings/{settings_id}", response_model=SettingsResponse)
def read_settings(settings_id: str) -> dict[str, Any]:
    """Get settings by ID."""
    settings = get_settings_by_id(settings_id)
    if settings is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Settings '{settings_id}' not found"
        )
    return settings
