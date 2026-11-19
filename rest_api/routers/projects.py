"""Project endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from rest_api.dependencies import Pagination
from rest_api.schemas import Page, ProjectResponse
from rest_api.service import get_project, get_projects

router = APIRouter(prefix="/projects")


@router.get("/")
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


@router.get("/{project_id}")
def read_project(project_id: str) -> ProjectResponse:
    """Get a project by ID."""
    return ProjectResponse.of(get_project(project_id))
