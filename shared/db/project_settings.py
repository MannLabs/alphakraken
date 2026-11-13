"""Queries on the assignments of settings to projects."""

import logging
from typing import Any

from shared.db import projects, settings
from shared.db._mongo import project_settings as _backend
from shared.domain.errors import ValidationError
from shared.domain.inputs import parse
from shared.domain.projects import NewAssignment, ProjectSettings, ProjectStatus
from shared.domain.settings import SettingsStatus
from shared.keys import FALLBACK_PROJECT_ID
from shared.settings_scope_resolver import known_scopes

_NON_ACTIVE_PROJECT_STATUSES = [ProjectStatus.INACTIVE, ProjectStatus.DELETED]


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
        ValidationError: if a field is invalid, a scope is unknown, the fallback project is not
            allowed, the project or the settings are archived or deleted, or the settings are
            already assigned to the project with the same file name filters.

    """
    new = parse(NewAssignment, project_id=project_id, settings_id=settings_id, **fields)
    known = known_scopes()
    if unknown := [s for s in (*new.scopes, *new.excluded_scopes) if s not in known]:
        raise ValidationError([f"Unknown scopes {unknown}, known are: {known}."])
    return _assign(new, allow_fallback=allow_fallback)


def _assign(new: NewAssignment, *, allow_fallback: bool) -> ProjectSettings:
    """Store the assignment after the checks against the database, cf. `assign`."""
    project_id, settings_id = new.project_id, new.settings_id
    _check_fallback_allowed(project_id, allow_fallback=allow_fallback)
    if projects.get(project_id).status in _NON_ACTIVE_PROJECT_STATUSES:
        raise ValidationError(
            [f"Cannot assign settings to archived or deleted project '{project_id}'"]
        )
    assigned = settings.get(settings_id)
    if assigned.status != SettingsStatus.ACTIVE:
        raise ValidationError(
            [
                f"Cannot assign archived or deleted settings '{assigned.name}' version {assigned.version}"
            ]
        )
    _check_not_assigned_with_same_filters(
        project_id, settings_id, new.raw_file_id_filter, new.raw_file_id_exclude_filter
    )

    assignment = _backend.assign(new)
    logging.info(f"Created project-settings assignment: {new!r}")
    return assignment


def _check_fallback_allowed(project_id: str, *, allow_fallback: bool) -> None:
    """Raise if the assignments of the project may not be changed by the caller."""
    if project_id == FALLBACK_PROJECT_ID and not allow_fallback:
        raise ValidationError(
            [
                f"Changing the settings assignments of project '{FALLBACK_PROJECT_ID}' is not allowed."
            ]
        )


def _check_not_assigned_with_same_filters(
    project_id: str,
    settings_id: str,
    raw_file_id_filter: list[str],
    raw_file_id_exclude_filter: list[str],
) -> None:
    """Raise if the settings are already assigned to the project with identical file name filters.

    Raises:
        ValidationError: if they are.

    """
    for existing in find([project_id]):
        if (
            existing.settings.id == settings_id
            and set(existing.raw_file_id_filter) == set(raw_file_id_filter)
            and _lower(existing.raw_file_id_exclude_filter)
            == _lower(raw_file_id_exclude_filter)
        ):
            raise ValidationError(
                [
                    f"Settings '{existing.settings.name}' version {existing.settings.version} already assigned "
                    f"to project '{project_id}' with the same file name filters. "
                    "Unlink the existing assignment and re-assign with the merged scopes."
                ]
            )


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
    _check_fallback_allowed(project_id, allow_fallback=allow_fallback)
    if projects.get(project_id).status in _NON_ACTIVE_PROJECT_STATUSES:
        raise ValidationError(
            [
                f"Cannot unassign settings from archived or deleted project '{project_id}'"
            ]
        )

    _backend.unassign(assignment_id)
    logging.info(f"Removed project-settings assignment: {assignment_id=}")


def update_to_latest(assignment_id: str, *, allow_fallback: bool) -> ProjectSettings:
    """Replace a settings assignment by one to the latest active version of its settings, keeping scopes and filters.

    :param allow_fallback: whether the caller may change the assignments of the fallback project

    Raises:
        NotFoundError: if there is no such assignment.
        ValidationError: if there is no newer active version, the project is archived or deleted, or
            the latest version is already assigned to the project with the same file name filters.

    """
    project_id = _backend.get_project_id(assignment_id)
    current = next(a for a in find([project_id]) if a.id == assignment_id)
    latest = settings.latest_active(current.settings.name)
    if latest is None or latest.version <= current.settings.version:
        raise ValidationError(
            [
                f"No newer active version of settings '{current.settings.name}' "
                f"than version {current.settings.version}"
            ]
        )

    # not through `assign`: a scope that left alphakraken.yaml since is carried over
    new = parse(
        NewAssignment,
        project_id=project_id,
        settings_id=latest.id,
        scopes=list(current.scopes),
        excluded_scopes=list(current.excluded_scopes),
        raw_file_id_filter=list(current.raw_file_id_filter),
        raw_file_id_exclude_filter=list(current.raw_file_id_exclude_filter),
    )
    # assigning first: if it fails, the current assignment is kept
    updated = _assign(new, allow_fallback=allow_fallback)
    _backend.unassign(assignment_id)
    logging.info(f"Removed project-settings assignment: {assignment_id=}")
    return updated
