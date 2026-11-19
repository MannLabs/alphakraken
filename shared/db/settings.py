"""Settings queries."""

import logging

from shared.db import projects, users
from shared.db._mongo import settings as _backend
from shared.domain.errors import InvalidInputError
from shared.domain.inputs import parse
from shared.domain.projects import ProjectStatus
from shared.domain.settings import NewSettings, Settings, SettingsStatus
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


def create(  # noqa: PLR0913
    *,
    name: str,
    software_type: str,
    software: str,
    runner_name: str,
    metrics_type: str,
    description: str | None = None,
    fasta_file_name: str | None = None,
    speclib_file_name: str | None = None,
    config_file_name: str | None = None,
    config_params: str | None = None,
    slurm_cpus_per_task: int | None = None,
    slurm_mem: str | None = None,
    slurm_time: str | None = None,
    num_threads: int | None = None,
    owner_email: str | None = None,
    archive_previous: bool = False,
) -> Settings:
    """Create new settings, as the next version of the settings with the same name, cf. `NewSettings`.

    :param slurm_cpus_per_task: None for the default of the software type, likewise `slurm_mem`,
        `slurm_time` and `num_threads`
    :param archive_previous: archive the latest active version of the name afterwards; if that
        fails, e.g. as it was archived concurrently, the error is raised although the new version
        exists, and a retry creates yet another version

    Raises:
        InvalidInputError: if a field is invalid, the runner is unknown or cannot run the software
            type, or the owner does not exist or is not active.

    """
    new = parse(
        NewSettings,
        name=name,
        software_type=software_type,
        software=software,
        runner_name=runner_name,
        metrics_type=metrics_type,
        description=description,
        fasta_file_name=fasta_file_name,
        speclib_file_name=speclib_file_name,
        config_file_name=config_file_name,
        config_params=config_params,
        slurm_cpus_per_task=slurm_cpus_per_task,
        slurm_mem=slurm_mem,
        slurm_time=slurm_time,
        num_threads=num_threads,
        owner_email=owner_email,
    )
    errors = check_runner_supports_software_type(new.runner_name, new.software_type)
    errors += users.owner_errors(new.owner_email)
    if errors:
        raise InvalidInputError(errors)

    existing = find(name=new.name, include_deleted=True)
    version = max((s.version for s in existing), default=0) + 1
    active = [s for s in existing if s.status == SettingsStatus.ACTIVE]
    previous = max(active, key=lambda s: s.version, default=None)
    # deliberately ignoring potential race condition
    number = count(include_deleted=True) + 1

    settings = _backend.create(new, number=number, version=version)
    logging.info(f"Created settings: {new.name=} {version=}")

    if archive_previous and previous:
        archive(previous.id)
    return settings


def archive(unique_name: str) -> None:
    """Archive settings by setting their status to INACTIVE.

    Raises:
        NotFoundError: if there are no such settings.
        InvalidInputError: if the settings are not active.

    """
    _check_status_change(get(unique_name), SettingsStatus.INACTIVE)
    _backend.set_status(unique_name, SettingsStatus.INACTIVE)
    logging.info(f"Archived settings: {unique_name=}")


def delete(unique_name: str) -> None:
    """Soft-delete settings by setting their status to DELETED.

    Raises:
        NotFoundError: if there are no such settings.
        InvalidInputError: if the settings are not active or still assigned to an active project.

    """
    settings = get(unique_name)
    _check_status_change(settings, SettingsStatus.DELETED)
    # check and write are separate steps; a race between concurrent writes is accepted: an assignment made meanwhile points to deleted settings
    project_ids = _backend.assigned_project_ids(unique_name)
    # archiving and deleting a project are final, so its assignments never apply again
    active_ids = (
        sorted(
            p.id
            for p in projects.find(ids=project_ids, statuses=[ProjectStatus.ACTIVE])
        )
        if project_ids
        else []
    )
    if active_ids:
        raise InvalidInputError(
            [
                f"Cannot delete settings '{settings.name}' version {settings.version}: "
                f"still assigned to projects {', '.join(active_ids)}"
            ]
        )

    _backend.set_status(unique_name, SettingsStatus.DELETED)
    logging.info(f"Deleted settings: {unique_name=}")


def _check_status_change(settings: Settings, new_status: str) -> None:
    """Raise unless the settings are active."""
    # archiving and deleting are final; check and write are separate steps; a race between concurrent writes is accepted
    if settings.status != SettingsStatus.ACTIVE:
        raise InvalidInputError(
            [
                f"Cannot set settings '{settings.name}' version {settings.version} "
                f"from '{settings.status}' to '{new_status}'."
            ]
        )
