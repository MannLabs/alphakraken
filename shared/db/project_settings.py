"""Queries on the assignments of settings to projects."""

import logging

from shared.db import projects, settings
from shared.db._mongo import project_settings as _backend
from shared.domain.errors import ValidationError
from shared.domain.projects import ProjectSettings, ProjectStatus
from shared.domain.settings import SettingsStatus
from shared.keys import DEFAULT_SCOPE

_NON_ACTIVE_PROJECT_STATUSES = [ProjectStatus.INACTIVE, ProjectStatus.DELETED]


def find(project_ids: list[str] | None = None) -> list[ProjectSettings]:
    """Return the settings assignments with their settings, oldest first.

    An assignment whose settings no longer exist is left out.

    :param project_ids: only the assignments of these projects, None for all, also those of
        archived and deleted projects
    """
    return _backend.find(project_ids)


def assign(  # noqa: PLR0913
    project_id: str,
    settings_id: str,
    *,
    scopes: list[str] | None = None,
    excluded_scopes: list[str] | None = None,
    raw_file_id_filter: list[str] | None = None,
    raw_file_id_exclude_filter: list[str] | None = None,
) -> ProjectSettings:
    """Assign settings to a project.

    :param settings_id: the unique name of the settings, e.g. `fallback_v7`

    Raises:
        NotFoundError: if the project or the settings do not exist.
        ValidationError: if the project or the settings are archived or deleted, or the settings are
            already assigned to the project with the same file name filters.

    """
    # moves to shared/services, cf. VALIDATION_DESIGN.md
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

    # the default scope matches everything, so any other entry next to it is redundant
    scopes = [DEFAULT_SCOPE] if not scopes or DEFAULT_SCOPE in scopes else scopes
    excluded_scopes = excluded_scopes or []
    raw_file_id_filter = raw_file_id_filter or []
    raw_file_id_exclude_filter = raw_file_id_exclude_filter or []

    _check_not_assigned_with_same_filters(
        project_id, settings_id, raw_file_id_filter, raw_file_id_exclude_filter
    )

    assignment = _backend.assign(
        project_id,
        settings_id,
        # TODO: validate here, not only on the frontend: reject DEFAULT_SCOPE in excluded_scopes
        #  and scopes overlapping excluded_scopes, both silently match nothing
        scopes=scopes,
        excluded_scopes=excluded_scopes,
        raw_file_id_filter=raw_file_id_filter,
        raw_file_id_exclude_filter=raw_file_id_exclude_filter,
    )
    logging.info(
        f"Created project-settings assignment: {project_id=} {assigned.name=} {scopes=} "
        f"{excluded_scopes=} {raw_file_id_filter=} {raw_file_id_exclude_filter=}"
    )
    return assignment


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


def unassign(assignment_id: str) -> None:
    """Remove a settings assignment.

    Raises:
        NotFoundError: if there is no such assignment.
        ValidationError: if its project is archived or deleted.

    """
    # moves to shared/services, cf. VALIDATION_DESIGN.md
    project_id = _backend.get_project_id(assignment_id)
    if projects.get(project_id).status in _NON_ACTIVE_PROJECT_STATUSES:
        raise ValidationError(
            [
                f"Cannot unassign settings from archived or deleted project '{project_id}'"
            ]
        )

    _backend.unassign(assignment_id)
    logging.info(f"Removed project-settings assignment: {assignment_id=}")


def update_to_latest(assignment_id: str) -> ProjectSettings:
    """Replace a settings assignment by one to the latest active version of its settings, keeping scopes and filters.

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

    # assigning first: if it fails, the current assignment is kept
    updated = assign(
        project_id,
        latest.id,
        scopes=list(current.scopes),
        excluded_scopes=list(current.excluded_scopes),
        raw_file_id_filter=list(current.raw_file_id_filter),
        raw_file_id_exclude_filter=list(current.raw_file_id_exclude_filter),
    )
    _backend.unassign(assignment_id)
    logging.info(f"Removed project-settings assignment: {assignment_id=}")
    return updated
