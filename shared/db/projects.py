"""Project queries."""

import logging
from typing import Any

from shared.db import users
from shared.db._checks import check_status_transition
from shared.db._mongo import projects as _backend
from shared.domain.errors import NotFoundError, ValidationError
from shared.domain.inputs import parse
from shared.domain.projects import (
    PROJECT_STATUS_TRANSITIONS,
    NewProject,
    Project,
    ProjectStatus,
)


def find(
    *,
    ids: list[str] | None = None,
    statuses: list[str] | None = None,
    include_deleted: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[Project]:
    """Return the projects, newest first.

    :param ids: only the projects with these ids
    :param statuses: only the projects with one of these statuses; deleted ones also need `include_deleted`
    """
    return _backend.find(
        ids=ids,
        statuses=statuses,
        include_deleted=include_deleted,
        limit=limit,
        offset=offset,
    )


def count(
    *,
    ids: list[str] | None = None,
    statuses: list[str] | None = None,
    include_deleted: bool = False,
) -> int:
    """Return the number of projects that `find` returns without limit and offset."""
    return _backend.count(ids=ids, statuses=statuses, include_deleted=include_deleted)


def get(project_id: str) -> Project:
    """Return the project with the given id, whatever its status.

    Raises:
        NotFoundError: if there is none.

    """
    found = find(ids=[project_id], include_deleted=True)
    if not found:
        raise NotFoundError(f"No project {project_id}")
    return found[0]


def add(**fields: Any) -> None:
    """Add a new active project.

    :param fields: the fields of `NewProject`

    Raises:
        AlreadyExistsError: if the project id is in use.
        NotFoundError: if there is no user with `owner_email`.
        ValidationError: if a field is missing or invalid, the project id was used by a deleted
            project, or the owner is inactive.

    """
    new = parse(NewProject, **fields)
    logging.info(f"Adding to DB: {new!r}")
    errors = []
    existing = find(ids=[new.project_id], include_deleted=True)
    if existing and existing[0].status == ProjectStatus.DELETED:
        errors.append(
            f"Project id '{new.project_id}' was used by a deleted project. Please choose a different one."
        )
    errors += users.check_active(new.owner_email)
    if errors:
        raise ValidationError(errors)

    _backend.add(new)


def archive(project_id: str) -> None:
    """Archive a project by setting its status to INACTIVE.

    Raises:
        NotFoundError: if there is no such project.
        ValidationError: if the project is not active.

    """
    _set_status(project_id, ProjectStatus.INACTIVE)


def delete(project_id: str) -> None:
    """Soft-delete a project by setting its status to DELETED.

    Raises:
        NotFoundError: if there is no such project.
        ValidationError: if the project is not active.

    """
    _set_status(project_id, ProjectStatus.DELETED)


def _set_status(project_id: str, status: str) -> None:
    check_status_transition(
        PROJECT_STATUS_TRANSITIONS,
        f"project '{project_id}'",
        get(project_id).status,
        status,
    )
    _backend.set_status(project_id, status)
    logging.info(f"Set project status: {project_id=} {status=}")
