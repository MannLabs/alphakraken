"""Project queries on MongoDB."""

from mongoengine import QuerySet

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import to_project
from shared.domain.errors import NotFoundError
from shared.domain.projects import NewProject, Project, ProjectStatus


def _query(
    *, ids: list[str] | None, statuses: list[str] | None, include_deleted: bool
) -> QuerySet:
    conditions = {}
    if not include_deleted:
        conditions["status__ne"] = ProjectStatus.DELETED
    if statuses is not None:
        conditions["status__in"] = statuses
    if ids is not None:
        conditions["id__in"] = ids
    return models.Project.objects(**conditions)


@db_backend_function
def find(
    *,
    ids: list[str] | None,
    statuses: list[str] | None,
    include_deleted: bool,
    limit: int | None,
    offset: int,
) -> list[Project]:
    """Return the projects, cf. `shared.db.projects`."""
    query = _query(
        ids=ids, statuses=statuses, include_deleted=include_deleted
    ).order_by("-created_at_")
    if offset:
        query = query.skip(offset)
    if limit is not None:
        query = query.limit(limit)
    return [to_project(doc) for doc in query]


@db_backend_function
def count(
    *, ids: list[str] | None, statuses: list[str] | None, include_deleted: bool
) -> int:
    """Return the number of projects, cf. `shared.db.projects`."""
    return _query(ids=ids, statuses=statuses, include_deleted=include_deleted).count()


@db_backend_function
def add(new: NewProject) -> None:
    """Insert a new project, cf. `shared.db.projects`."""
    owner = models.User.objects.get(email=new.owner_email) if new.owner_email else None
    project = models.Project(
        id=new.project_id, name=new.name, description=new.description, owner=owner
    )
    # this will fail if the project id already exists
    project.save(force_insert=True)


@db_backend_function
def set_status(project_id: str, status: str) -> None:
    """Set the status of a project."""
    if not models.Project.objects(id=project_id).update_one(status=status):
        raise NotFoundError(f"No project {project_id}")
