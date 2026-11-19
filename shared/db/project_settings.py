"""Queries on the assignments of settings to projects."""

import logging

from shared.db import projects, settings
from shared.db._checks import inactive_errors
from shared.db._mongo import project_settings as _backend
from shared.domain.errors import InvalidInputError, NotFoundError
from shared.domain.inputs import parse
from shared.domain.projects import NewAssignment, ProjectSettings, ProjectStatus
from shared.domain.settings import SettingsStatus
from shared.keys import DEFAULT_SCOPE, FALLBACK_PROJECT_ID
from shared.settings_scope_resolver import fully_excluded_scopes, known_scopes


def find(project_ids: list[str] | None = None) -> list[ProjectSettings]:
    """Return the settings assignments with their settings, oldest first.

    An assignment whose settings no longer exist is left out and logged as an error; the
    application never removes settings, so this needs a direct edit of the database.

    :param project_ids: only the assignments of these projects, None for all, also those of
        archived and deleted projects
    """
    return _backend.find(project_ids)


def assign(  # noqa: PLR0913
    project_id: str,
    settings_id: str,
    *,
    allow_fallback: bool,
    scopes: list[str] | None = None,
    excluded_scopes: list[str] | None = None,
    raw_file_id_filter: list[str] | None = None,
    raw_file_id_exclude_filter: list[str] | None = None,
) -> ProjectSettings:
    """Assign settings to a project, cf. `NewAssignment`.

    :param settings_id: the unique name of the settings, e.g. `fallback_v7`
    :param allow_fallback: whether the caller may change the assignments of the fallback project
    :param scopes: None for all instruments
    :param excluded_scopes: None for none
    :param raw_file_id_filter: None for all files
    :param raw_file_id_exclude_filter: None for none

    Raises:
        NotFoundError: if the project or the settings do not exist; raised instead of the
            InvalidInputError, so its rule errors are not reported then.
        InvalidInputError: if a field is invalid, a scope is unknown, a scope or file name filter
            entry would never apply, the fallback project is not allowed, the project or the
            settings are archived or deleted, or the settings are already assigned to the project
            with the same file name filters.

    """
    new = parse(
        NewAssignment,
        project_id=project_id,
        settings_id=settings_id,
        # not `or`: an empty list is an error, not all instruments
        scopes=[DEFAULT_SCOPE] if scopes is None else scopes,
        excluded_scopes=excluded_scopes or [],
        raw_file_id_filter=raw_file_id_filter or [],
        raw_file_id_exclude_filter=raw_file_id_exclude_filter or [],
    )
    errors = _scope_errors(new)
    errors += _project_errors(new.project_id, allow_fallback=allow_fallback)
    # check and write are separate steps; a race between concurrent writes is accepted, cf. `settings.delete`
    errors += _settings_errors(new.settings_id)
    errors += _duplicate_errors(
        new.project_id,
        new.settings_id,
        new.raw_file_id_filter,
        new.raw_file_id_exclude_filter,
    )
    if errors:
        raise InvalidInputError(errors)

    assignment = _backend.assign(new)
    logging.info(f"Created project-settings assignment: {new!r}")
    return assignment


def _scope_errors(new: NewAssignment) -> list[str]:
    """Return the errors of the scopes against alphakraken.yaml."""
    errors = []
    known = known_scopes()
    scopes = dict.fromkeys((*new.scopes, *new.excluded_scopes))
    if unknown := [s for s in scopes if s not in known]:
        errors.append(f"Unknown scopes {unknown}, known are: {known}.")
    if excluded := fully_excluded_scopes(new.scopes, new.excluded_scopes):
        errors.append(
            f"All instruments of scopes {excluded} are excluded, these entries would never apply."
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
    errors += inactive_errors(
        f"Project '{project_id}'", projects.get(project_id).status, ProjectStatus.ACTIVE
    )
    return errors


def _settings_errors(settings_id: str) -> list[str]:
    """Return an error if the settings may not be assigned.

    Raises:
        NotFoundError: if there are no such settings.

    """
    assigned = settings.get(settings_id)
    return inactive_errors(
        f"Settings '{assigned.name}' version {assigned.version}",
        assigned.status,
        SettingsStatus.ACTIVE,
    )


def _duplicate_errors(
    project_id: str,
    settings_id: str,
    raw_file_id_filter: list[str],
    raw_file_id_exclude_filter: list[str],
) -> list[str]:
    """Return an error if the settings are already assigned to the project with identical file name filters."""
    return [
        f"Settings '{existing.settings.name}' version {existing.settings.version} already assigned "
        f"to project '{project_id}' with the same file name filters. "
        "Unlink the existing assignment and re-assign with the merged scopes."
        for existing in find([project_id])
        if existing.settings.id == settings_id
        and set(existing.raw_file_id_filter) == set(raw_file_id_filter)
        and _lower(existing.raw_file_id_exclude_filter)
        == _lower(raw_file_id_exclude_filter)
    ]


def _lower(filters: list[str]) -> set[str]:
    return {f.lower() for f in filters}


def unassign(assignment_id: str, *, allow_fallback: bool) -> None:
    """Remove a settings assignment.

    :param allow_fallback: whether the caller may change the assignments of the fallback project

    Raises:
        NotFoundError: if there is no such assignment.
        InvalidInputError: if its project is archived or deleted, or is the fallback project and
            that is not allowed.

    """
    project_id = _backend.get_project_id(assignment_id)
    if errors := _project_errors(project_id, allow_fallback=allow_fallback):
        raise InvalidInputError(errors)

    _backend.unassign(assignment_id)
    logging.info(f"Removed project-settings assignment: {assignment_id=}")


def update_to_latest(assignment_id: str, *, allow_fallback: bool) -> ProjectSettings:
    """Point a settings assignment to the latest active version of its settings, keeping scopes and filters.

    :param allow_fallback: whether the caller may change the assignments of the fallback project

    Raises:
        NotFoundError: if there is no such assignment, or its settings no longer exist.
        InvalidInputError: if there is no newer active version, the project is archived or deleted, or
            the latest version is already assigned to the project with the same file name filters.

    """
    project_id = _backend.get_project_id(assignment_id)
    current = next((a for a in find([project_id]) if a.id == assignment_id), None)
    if current is None:
        # `find` leaves out an assignment whose settings no longer exist
        raise NotFoundError(f"Settings of assignment {assignment_id} do not exist")
    latest = settings.latest_active(current.settings.name)
    if latest is None or latest.version <= current.settings.version:
        raise InvalidInputError(
            [
                f"No newer active version of settings '{current.settings.name}' "
                f"than version {current.settings.version}"
            ]
        )

    errors = _project_errors(project_id, allow_fallback=allow_fallback)
    errors += _duplicate_errors(
        project_id,
        latest.id,
        current.raw_file_id_filter,
        current.raw_file_id_exclude_filter,
    )
    if errors:
        raise InvalidInputError(errors)

    updated = _backend.set_settings(assignment_id, latest.id)
    logging.info(
        f"Updated project-settings assignment: {assignment_id=} to {latest.id}"
    )
    return updated
