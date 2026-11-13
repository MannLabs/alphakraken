"""Settings queries."""

import logging
from typing import Any

from shared.db import users
from shared.db._checks import check_status_transition
from shared.db._mongo import settings as _backend
from shared.domain.errors import ValidationError
from shared.domain.inputs import parse
from shared.domain.settings import (
    SETTINGS_STATUS_TRANSITIONS,
    NewSettings,
    Settings,
    SettingsStatus,
)
from shared.runner_checks import check_runner_supports_software_type


def find(
    *,
    name: str | None = None,
    statuses: list[str] | None = None,
    include_deleted: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[Settings]:
    """Return the settings, newest first.

    :param name: only the versions of the settings with this name
    :param statuses: only the settings with one of these statuses; deleted ones also need `include_deleted`
    """
    return _backend.find(
        name=name,
        statuses=statuses,
        include_deleted=include_deleted,
        limit=limit,
        offset=offset,
    )


def count(
    *,
    name: str | None = None,
    statuses: list[str] | None = None,
    include_deleted: bool = False,
) -> int:
    """Return the number of settings that `find` returns without limit and offset."""
    return _backend.count(name=name, statuses=statuses, include_deleted=include_deleted)


def get(unique_name: str) -> Settings:
    """Return the settings with the given unique name, e.g. `fallback_v7`, whatever their status.

    Raises:
        NotFoundError: if there are none, also if `unique_name` is not of that form.

    """
    return _backend.get(unique_name)


def latest_active(name: str) -> Settings | None:
    """Return the latest active version of the settings with the given name, None if there is none."""
    active = find(name=name, statuses=[SettingsStatus.ACTIVE])
    return max(active, key=lambda s: s.version, default=None)


def create(*, archive_previous: bool = False, **fields: Any) -> Settings:
    """Create new settings, as the next version of the settings with the same name.

    :param archive_previous: archive the latest active version of the name afterwards
    :param fields: the fields of `NewSettings`

    Raises:
        NotFoundError: if there is no user with `owner_email`.
        ValidationError: if a field is missing or invalid, the runner is unknown or cannot run
            the software type, or the owner is inactive.

    """
    new = parse(NewSettings, **fields)
    errors = check_runner_supports_software_type(new.runner_name, new.software_type)
    if new.owner_email:
        errors += users.check_active(new.owner_email)
    if errors:
        raise ValidationError(errors)

    existing = find(name=new.name, include_deleted=True)
    version = max((s.version for s in existing), default=0) + 1
    # deliberately ignoring potential race condition
    number = count(include_deleted=True) + 1

    settings = _backend.create(new, number=number, version=version)
    logging.info(f"Created settings: {new.name=} {version=}")

    if archive_previous:
        active = [s for s in existing if s.status == SettingsStatus.ACTIVE]
        if previous := max(active, key=lambda s: s.version, default=None):
            archive(previous.id)
    return settings


def archive(unique_name: str) -> None:
    """Archive settings by setting their status to INACTIVE.

    Raises:
        NotFoundError: if there are no such settings.
        ValidationError: if the settings are not active.

    """
    _check_status_transition(get(unique_name), SettingsStatus.INACTIVE)
    _backend.set_status(unique_name, SettingsStatus.INACTIVE)
    logging.info(f"Archived settings: {unique_name=}")


def delete(unique_name: str) -> None:
    """Soft-delete settings by setting their status to DELETED.

    Raises:
        NotFoundError: if there are no such settings.
        ValidationError: if the settings are not active or still assigned to a project.

    """
    settings = get(unique_name)
    _check_status_transition(settings, SettingsStatus.DELETED)
    project_ids = _backend.assigned_project_ids(unique_name)
    if project_ids:
        raise ValidationError(
            [
                f"Cannot delete settings '{settings.name}' version {settings.version}: "
                f"still assigned to projects {', '.join(project_ids)}"
            ]
        )

    _backend.set_status(unique_name, SettingsStatus.DELETED)
    logging.info(f"Deleted settings: {unique_name=}")


def _check_status_transition(settings: Settings, status: str) -> None:
    check_status_transition(
        SETTINGS_STATUS_TRANSITIONS,
        f"settings '{settings.name}' version {settings.version}",
        settings.status,
        status,
    )
