"""REST API endpoint definitions."""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from rest_api.dependencies import Pagination
from rest_api.schemas import (
    Page,
    ProjectResponse,
    RawFileResponse,
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


@router.get("/raw_files/")
def list_raw_files(  # noqa: PLR0913
    page: Annotated[Pagination, Depends()],
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
) -> Page[RawFileResponse]:
    """List raw files with optional filtering, including flattened metrics."""
    data, total = get_raw_files_with_metrics(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        date_from=date_from,
        date_to=date_to,
        limit=page.limit,
        offset=page.offset,
        include_metrics=include_metrics,
        include_file_info=include_file_info,
    )

    return Page(
        total=total,
        offset=page.offset,
        limit=page.limit,
        data=[RawFileResponse.of(item) for item in data],
    )


@router.get("/projects/")
def list_projects(
    page: Annotated[Pagination, Depends()],
    project_id: Annotated[
        str | None, Query(description="Filter by project ID (exact match)")
    ] = None,
) -> Page[ProjectResponse]:
    """List non-deleted projects with their settings assignments."""
    data, total = get_projects(
        project_id=project_id, limit=page.limit, offset=page.offset
    )
    return Page(
        total=total,
        offset=page.offset,
        limit=page.limit,
        data=[ProjectResponse.of(item) for item in data],
    )


@router.get("/projects/{project_id}")
def read_project(project_id: str) -> ProjectResponse:
    """Get a project by ID."""
    return ProjectResponse.of(get_project(project_id))


@router.get("/settings/")
def list_settings(
    page: Annotated[Pagination, Depends()],
    name: Annotated[
        str | None, Query(description="Filter by settings name (exact match)")
    ] = None,
) -> Page[SettingsResponse]:
    """List non-deleted settings."""
    data, total = get_settings(name=name, limit=page.limit, offset=page.offset)
    return Page(
        total=total,
        offset=page.offset,
        limit=page.limit,
        data=[SettingsResponse.model_validate(s) for s in data],
    )


@router.get("/settings/{settings_id}")
def read_settings(settings_id: str) -> SettingsResponse:
    """Get settings by ID, i.e. `<name>_v<version>`."""
    return SettingsResponse.model_validate(get_settings_by_unique_name(settings_id))
