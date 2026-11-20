"""Project queries."""

import logging

from shared.db import users
from shared.db._mongo import projects as _backend
from shared.domain.errors import AlreadyExistsError, InvalidInputError, NotFoundError
from shared.domain.inputs import parse
from shared.domain.projects import NewProject, Project, ProjectStatus
from shared.keys import FALLBACK_PROJECT_ID


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


def add(
    *,
    project_id: str,
    name: str,
    description: str | None = None,
    owner_email: str | None = None,
    allow_fallback: bool,
) -> None:
    """Add a new active project, cf. `NewProject`.

    :param allow_fallback: whether the caller may create the fallback project

    Raises:
        AlreadyExistsError: if the project id is in use.
        InvalidInputError: if a field is invalid, the project id is not available,
            the fallback project is not allowed, or the owner does not exist or is not active.

    """
    new = parse(
        NewProject,
        project_id=project_id,
        name=name,
        description=description,
        owner_email=owner_email,
    )
    errors = []
    if new.project_id == FALLBACK_PROJECT_ID and not allow_fallback:
        errors.append(f"Creating project '{FALLBACK_PROJECT_ID}' is not allowed.")
    existing = find(ids=[new.project_id], include_deleted=True)
    if existing and existing[0].status == ProjectStatus.DELETED:
        # the id stays blocked; the deleted project itself is not revealed, cf. `_set_status`
        errors.append(
            f"Project id '{new.project_id}' is not available. Please choose a different one."
        )
    errors += users.owner_errors(new.owner_email)
    if errors:
        raise InvalidInputError(errors)

    logging.info(f"Adding to DB: {new!r}")
    try:
        _backend.add(new)
    except AlreadyExistsError as e:
        raise AlreadyExistsError(f"Project '{new.project_id}' already exists.") from e


def archive(project_id: str) -> None:
    """Archive a project by setting its status to INACTIVE.

    Raises:
        NotFoundError: if there is no such project, or it is deleted.
        InvalidInputError: if the project is archived or is the fallback project.

    """
    _set_status(project_id, ProjectStatus.INACTIVE)


def delete(project_id: str) -> None:
    """Soft-delete a project by setting its status to DELETED.

    Raises:
        NotFoundError: if there is no such project, or it is deleted.
        InvalidInputError: if the project is archived or is the fallback project.

    """
    _set_status(project_id, ProjectStatus.DELETED)


def _set_status(project_id: str, status: str) -> None:
    """Set the status of an active project, cf. `archive` and `delete`."""
    # raw files without an active project are assigned to the fallback project
    if project_id == FALLBACK_PROJECT_ID:
        raise InvalidInputError(
            [f"Project '{FALLBACK_PROJECT_ID}' cannot be archived or deleted."]
        )
    current = get(project_id).status
    # a deleted project is gone for every client, so it is not revealed by a different error
    if current == ProjectStatus.DELETED:
        raise NotFoundError(f"No project {project_id}")
    # archiving and deleting are final; check and write are separate steps; a race between concurrent writes is accepted
    if current != ProjectStatus.ACTIVE:
        raise InvalidInputError(
            [f"Cannot set project '{project_id}' from '{current}' to '{status}'."]
        )
    _backend.set_status(project_id, status)
    logging.info(f"Set project status: {project_id=} {status=}")
