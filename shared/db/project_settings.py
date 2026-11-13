"""Queries on the assignments of settings to projects."""

import logging
from typing import Any

from shared.db import projects, settings
from shared.db._mongo import project_settings as _backend
from shared.domain.errors import NotFoundError, ValidationError
from shared.domain.inputs import parse
from shared.domain.projects import NewAssignment, ProjectSettings, ProjectStatus
from shared.domain.settings import SettingsStatus
from shared.keys import FALLBACK_PROJECT_ID
from shared.settings_scope_resolver import fully_excluded_scopes, known_scopes


def find(project_ids: list[str] | None = None) -> list[ProjectSettings]:
    """Return the settings assignments with their settings, oldest first.

    An assignment whose settings no longer exist is left out.

    :param project_ids: only the assignments of these projects, None for all, also those of
        archived and deleted projects
    """
    return _backend.find(project_ids)


def assign(
    project_id: str, settings_id: str, *, allow_fallback: bool, **fields: Any
) -> ProjectSettings:
    """Assign settings to a project.

    :param settings_id: the unique name of the settings, e.g. `fallback_v7`
    :param allow_fallback: whether the caller may change the assignments of the fallback project
    :param fields: the other fields of `NewAssignment`

    Raises:
        NotFoundError: if the project or the settings do not exist.
        ValidationError: if a field is invalid, a scope is unknown or fully excluded, the fallback project is not
            allowed, the project or the settings are archived or deleted, or the settings are
            already assigned to the project with the same file name filters.

    """
    new = parse(NewAssignment, project_id=project_id, settings_id=settings_id, **fields)
    return _assign(new, allow_fallback=allow_fallback, check_scopes=True)


def _assign(
    new: NewAssignment, *, allow_fallback: bool, check_scopes: bool
) -> ProjectSettings:
    """Store the assignment after the checks against the yaml and the database, cf. `assign`."""
    errors = _scope_errors(new) if check_scopes else []
    errors += _project_errors(new.project_id, allow_fallback=allow_fallback)
    assigned = settings.get(new.settings_id)
    if assigned.status != SettingsStatus.ACTIVE:
        errors.append(
            f"Cannot assign archived or deleted settings '{assigned.name}' version {assigned.version}"
        )
    errors += _duplicate_errors(new)
    if errors:
        raise ValidationError(errors)

    assignment = _backend.assign(new)
    logging.info(f"Created project-settings assignment: {new!r}")
    return assignment


def _scope_errors(new: NewAssignment) -> list[str]:
    """Return the errors of the scopes against alphakraken.yaml."""
    errors = []
    known = known_scopes()
    if unknown := [s for s in (*new.scopes, *new.excluded_scopes) if s not in known]:
        errors.append(f"Unknown scopes {unknown}, known are: {known}.")
    if excluded := fully_excluded_scopes(new.scopes, new.excluded_scopes):
        errors.append(
            f"All instruments of scopes {excluded} are excluded, they would never apply."
        )
    return errors


def _project_errors(project_id: str, *, allow_fallback: bool) -> list[str]:
    """Return why the assignments of the project may not be changed by the caller.

    Raises:
        NotFoundError: if there is no such project.

    """
    errors = []
    if project_id == FALLBACK_PROJECT_ID and not allow_fallback:
        errors.append(
            f"Changing the settings assignments of project '{FALLBACK_PROJECT_ID}' is not allowed."
        )
    if projects.get(project_id).status != ProjectStatus.ACTIVE:
        errors.append(
            f"Cannot change the settings assignments of archived or deleted project '{project_id}'"
        )
    return errors


def _duplicate_errors(new: NewAssignment) -> list[str]:
    """Return an error if the settings are already assigned to the project with identical file name filters."""
    return [
        f"Settings '{existing.settings.name}' version {existing.settings.version} already assigned "
        f"to project '{new.project_id}' with the same file name filters. "
        "Unlink the existing assignment and re-assign with the merged scopes."
        for existing in find([new.project_id])
        if existing.settings.id == new.settings_id
        and set(existing.raw_file_id_filter) == set(new.raw_file_id_filter)
        and _lower(existing.raw_file_id_exclude_filter)
        == _lower(new.raw_file_id_exclude_filter)
    ]


def _lower(filters: list[str]) -> set[str]:
    return {f.lower() for f in filters}


def unassign(assignment_id: str, *, allow_fallback: bool) -> None:
    """Remove a settings assignment.

    :param allow_fallback: whether the caller may change the assignments of the fallback project

    Raises:
        NotFoundError: if there is no such assignment.
        ValidationError: if its project is archived or deleted, or is the fallback project and
            that is not allowed.

    """
    project_id = _backend.get_project_id(assignment_id)
    if errors := _project_errors(project_id, allow_fallback=allow_fallback):
        raise ValidationError(errors)

    _backend.unassign(assignment_id)
    logging.info(f"Removed project-settings assignment: {assignment_id=}")


def update_to_latest(assignment_id: str, *, allow_fallback: bool) -> ProjectSettings:
    """Replace a settings assignment by one to the latest active version of its settings, keeping scopes and filters.

    :param allow_fallback: whether the caller may change the assignments of the fallback project

    Raises:
        NotFoundError: if there is no such assignment, or its settings no longer exist.
        ValidationError: if there is no newer active version, the project is archived or deleted, or
            the latest version is already assigned to the project with the same file name filters.

    """
    project_id = _backend.get_project_id(assignment_id)
    current = next((a for a in find([project_id]) if a.id == assignment_id), None)
    if current is None:
        # `find` leaves out an assignment whose settings no longer exist
        raise NotFoundError(f"Settings of assignment {assignment_id} do not exist")
    latest = settings.latest_active(current.settings.name)
    if latest is None or latest.version <= current.settings.version:
        raise ValidationError(
            [
                f"No newer active version of settings '{current.settings.name}' "
                f"than version {current.settings.version}"
            ]
        )

    # scopes unchecked: one that left alphakraken.yaml since is carried over
    new = parse(
        NewAssignment,
        project_id=project_id,
        settings_id=latest.id,
        scopes=current.scopes,
        excluded_scopes=current.excluded_scopes,
        raw_file_id_filter=current.raw_file_id_filter,
        raw_file_id_exclude_filter=current.raw_file_id_exclude_filter,
    )
    # assigning first: if it fails, the current assignment is kept
    updated = _assign(new, allow_fallback=allow_fallback, check_scopes=False)
    _backend.unassign(assignment_id)
    logging.info(f"Removed project-settings assignment: {assignment_id=}")
    return updated
