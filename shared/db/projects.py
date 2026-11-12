"""Project queries."""

import logging

from shared.db._mongo import projects as _backend
from shared.domain.errors import NotFoundError, ValidationError
from shared.domain.projects import Project, ProjectStatus


def find(
    *,
    ids: list[str] | None = None,
    include_deleted: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[Project]:
    """Return the projects, newest first.

    :param ids: only the projects with these ids
    """
    return _backend.find(
        ids=ids, include_deleted=include_deleted, limit=limit, offset=offset
    )


def count(*, ids: list[str] | None = None, include_deleted: bool = False) -> int:
    """Return the number of projects that `find` returns without limit and offset."""
    return _backend.count(ids=ids, include_deleted=include_deleted)


def get(project_id: str) -> Project:
    """Return the project with the given id, whatever its status.

    Raises:
        NotFoundError: if there is none.

    """
    found = find(ids=[project_id], include_deleted=True)
    if not found:
        raise NotFoundError(f"No project {project_id}")
    return found[0]


def add(
    *, project_id: str, name: str, description: str, owner_email: str | None = None
) -> None:
    """Add a new active project.

    Raises:
        AlreadyExistsError: if the project id is in use.
        NotFoundError: if there is no user with `owner_email`.
        ValidationError: if the project id was used by a deleted project or a field violates its constraints.

    """
    logging.info(f"Adding to DB: {project_id=} {name=} {description=} {owner_email=}")
    # moves to shared/services, cf. VALIDATION_DESIGN.md
    existing = find(ids=[project_id], include_deleted=True)
    if existing and existing[0].status == ProjectStatus.DELETED:
        raise ValidationError(
            [
                f"Project id '{project_id}' was used by a deleted project. Please choose a different one."
            ]
        )

    _backend.add(
        project_id=project_id,
        name=name,
        description=description,
        owner_email=owner_email,
    )


def archive(project_id: str) -> None:
    """Archive a project by setting its status to INACTIVE."""
    _backend.set_status(project_id, ProjectStatus.INACTIVE)
    logging.info(f"Archived project: {project_id=}")


def delete(project_id: str) -> None:
    """Soft-delete a project by setting its status to DELETED.

    Raises:
        NotFoundError: if there is no such project.
        ValidationError: if the project is archived.

    """
    # moves to shared/services, cf. VALIDATION_DESIGN.md
    if get(project_id).status == ProjectStatus.INACTIVE:
        raise ValidationError([f"Cannot delete archived project '{project_id}'"])

    _backend.set_status(project_id, ProjectStatus.DELETED)
    logging.info(f"Deleted project: {project_id=}")
