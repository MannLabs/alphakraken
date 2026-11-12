"""Settings queries."""

import logging

from shared.db import project_settings
from shared.db._mongo import settings as _backend
from shared.domain.errors import ValidationError
from shared.domain.settings import (
    Settings,
    SettingsStatus,
)


def find(
    *,
    name: str | None = None,
    include_deleted: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[Settings]:
    """Return the settings, newest first.

    :param name: only the versions of the settings with this name
    """
    return _backend.find(
        name=name, include_deleted=include_deleted, limit=limit, offset=offset
    )


def count(*, name: str | None = None, include_deleted: bool = False) -> int:
    """Return the number of settings that `find` returns without limit and offset."""
    return _backend.count(name=name, include_deleted=include_deleted)


def get(unique_name: str) -> Settings:
    """Return the settings with the given unique name, e.g. `fallback_v7`, whatever their status.

    Raises:
        NotFoundError: if there are none, also if `unique_name` is not of that form.

    """
    return _backend.get(unique_name)


def latest_active(name: str) -> Settings | None:
    """Return the latest active version of the settings with the given name, None if there is none."""
    active = [s for s in find(name=name) if s.status == SettingsStatus.ACTIVE]
    return max(active, key=lambda s: s.version, default=None)


def create(  # noqa: PLR0913
    *,
    name: str,
    description: str | None = None,
    fasta_file_name: str | None = None,
    speclib_file_name: str | None = None,
    config_file_name: str | None = None,
    config_params: str | None = None,
    software_type: str,
    software: str,
    runner_name: str,
    metrics_type: str,
    slurm_cpus_per_task: int | None = None,
    slurm_mem: str | None = None,
    slurm_time: str | None = None,
    num_threads: int | None = None,
    owner_email: str | None = None,
) -> Settings:
    """Create new settings, as the next version of the settings with the same name.

    Raises:
        NotFoundError: if there is no user with `owner_email`.
        ValidationError: if a field violates its constraints.

    """
    # moves to shared/services, cf. VALIDATION_DESIGN.md
    versions = [s.version for s in find(name=name, include_deleted=True)]
    version = max(versions, default=0) + 1
    # deliberately ignoring potential race condition
    number = count(include_deleted=True) + 1

    settings = _backend.create(
        number=number,
        name=name,
        version=version,
        description=description,
        fasta_file_name=fasta_file_name,
        speclib_file_name=speclib_file_name,
        config_file_name=config_file_name,
        config_params=config_params,
        software_type=software_type,
        software=software,
        runner_name=runner_name,
        metrics_type=metrics_type,
        slurm_cpus_per_task=slurm_cpus_per_task,
        slurm_mem=slurm_mem,
        slurm_time=slurm_time,
        num_threads=num_threads,
        owner_email=owner_email,
    )
    logging.info(f"Created settings: {name=} {version=}")
    return settings


def archive(unique_name: str) -> None:
    """Archive settings by setting their status to INACTIVE."""
    _backend.set_status(unique_name, SettingsStatus.INACTIVE)
    logging.info(f"Archived settings: {unique_name=}")


def delete(unique_name: str) -> None:
    """Soft-delete settings by setting their status to DELETED.

    Raises:
        NotFoundError: if there are no such settings.
        ValidationError: if the settings are archived or still assigned to a project.

    """
    # moves to shared/services, cf. VALIDATION_DESIGN.md
    settings = get(unique_name)
    label = f"'{settings.name}' version {settings.version}"
    if settings.status == SettingsStatus.INACTIVE:
        raise ValidationError([f"Cannot delete archived settings {label}"])
    project_ids = sorted(
        {
            ps.project_id
            for ps in project_settings.find()
            if ps.settings.id == unique_name
        }
    )
    if project_ids:
        raise ValidationError(
            [
                f"Cannot delete settings {label}: "
                f"still assigned to projects {', '.join(project_ids)}"
            ]
        )

    _backend.set_status(unique_name, SettingsStatus.DELETED)
    logging.info(f"Deleted settings: {unique_name=}")
