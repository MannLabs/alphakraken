"""Project endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from rest_api.auth import require_api_key
from rest_api.dependencies import Pagination
from rest_api.schemas import Page, ProjectResponse
from rest_api.service import get_project, get_projects
from shared.db import projects as project_store
from shared.domain.projects import NewProject

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


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_api_key)],
)
def create_project(new: NewProject) -> ProjectResponse:
    """Add an active project."""
    project_store.add(
        project_id=new.project_id,
        name=new.name,
        description=new.description,
        owner_email=new.owner_email,
        allow_fallback=False,
    )
    return ProjectResponse.of(get_project(new.project_id))


@router.post(
    "/{project_id}/archive",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_api_key)],
)
def archive_project(project_id: str) -> None:
    """Archive an active project."""
    project_store.archive(project_id)


@router.delete(
    "/{project_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_api_key)],
)
def delete_project(project_id: str) -> None:
    """Delete a project: it is kept in the database, but no longer returned."""
    project_store.delete(project_id)
