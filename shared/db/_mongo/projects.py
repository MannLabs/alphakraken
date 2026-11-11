"""Project queries on MongoDB."""

from mongoengine import QuerySet

from shared.db._mongo import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_project
from shared.domain.errors import NotFoundError
from shared.domain.projects import Project, ProjectStatus


def _query(*, ids: list[str] | None, include_deleted: bool) -> QuerySet:
    conditions = {}
    if not include_deleted:
        conditions["status__ne"] = ProjectStatus.DELETED
    if ids is not None:
        conditions["id__in"] = ids
    return models.Project.objects(**conditions)


@translate_errors
def find(
    *, ids: list[str] | None, include_deleted: bool, limit: int | None, offset: int
) -> list[Project]:
    """Return the projects, cf. `shared.db.projects`."""
    ensure_connected()
    query = _query(ids=ids, include_deleted=include_deleted).order_by("-created_at_")
    if offset:
        query = query.skip(offset)
    if limit is not None:
        query = query.limit(limit)
    return [to_project(doc) for doc in query]


@translate_errors
def count(*, ids: list[str] | None, include_deleted: bool) -> int:
    """Return the number of projects, cf. `shared.db.projects`."""
    ensure_connected()
    return _query(ids=ids, include_deleted=include_deleted).count()


@translate_errors
def add(
    *, project_id: str, name: str, description: str, owner_email: str | None
) -> None:
    """Insert a new project, cf. `shared.db.projects`."""
    ensure_connected()
    owner = models.User.objects.get(email=owner_email) if owner_email else None
    project = models.Project(
        id=project_id, name=name, description=description, owner=owner
    )
    # this will fail if the project id already exists
    project.save(force_insert=True)


@translate_errors
def set_status(project_id: str, status: str) -> None:
    """Set the status of a project."""
    ensure_connected()
    if not models.Project.objects(id=project_id).update_one(status=status):
        raise NotFoundError(f"No project {project_id}")
