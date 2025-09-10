"""Module to handle interactions with the database.

Note: this module must not have any dependencies on the rest of the codebase.
"""

import logging
from datetime import datetime, timedelta

import pytz

from shared.db.engine import connect_db
from shared.db.models import (
    KrakenStatus,
    KrakenStatusEntities,
    Metrics,
    Project,
    ProjectStatus,
    RawFile,
    Settings,
)


def get_raw_files_by_names(raw_file_names: list[str]) -> list[RawFile]:
    """Get raw files from the database with the given original names."""
    logging.info(f"Getting from DB: {raw_file_names=}")
    connect_db()
    return list(RawFile.objects.filter(original_name__in=raw_file_names))


def get_raw_file_by_id(raw_file_id: str) -> RawFile:
    """Get raw file from the database with the given id."""
    logging.info(f"Getting from DB: {raw_file_id=}")
    connect_db()
    return RawFile.objects(id=raw_file_id).first()


def get_raw_files_by_age(
    instrument_id: str,
    *,
    max_age_in_days: int,
    min_age_in_days: int,
) -> list[RawFile]:
    """Get raw file ids older than the given age window in days for a given instrument sorted 'oldest first'."""
    connect_db()
    now = datetime.now(tz=pytz.UTC)

    oldest_created_at = now - timedelta(days=max_age_in_days)
    youngest_created_at = now - timedelta(days=min_age_in_days)
    logging.info(
        f"Getting from DB: {instrument_id=} {oldest_created_at=} {youngest_created_at=}"
    )

    return list(
        RawFile.objects.filter(
            instrument_id=instrument_id,
            created_at__gte=oldest_created_at,
            created_at__lt=youngest_created_at,
        ).order_by("created_at")
    )


def add_raw_file(  # noqa: PLR0913 too many arguments
    file_name: str,
    collision_flag: str | None = None,
    *,
    project_id: str,
    status: str,
    instrument_id: str,
    creation_ts: float,
) -> str:
    """Add a new raw file to the database.

    :param file_name: name of the file
    :param collision_flag: optional flag to indicate a collision
    :param project_id: project id_
    :param status: status of the file
    :param instrument_id: id_ of the acquiring instrument
    :param creation_ts: creation timestamp (unix)
    :return: the raw file id_. This is either equal to the raw_file_name or has a collision flag prefixed
    """
    logging.info(
        f"Adding to DB: {file_name=} {collision_flag=} {project_id=} {status=} {instrument_id=} {creation_ts=}"
    )
    connect_db()

    id_ = file_name if collision_flag is None else f"{collision_flag}{file_name}"

    raw_file = RawFile(
        id=id_,
        collision_flag=collision_flag,
        original_name=file_name,
        project_id=project_id,
        instrument_id=instrument_id,
        status=status,
        created_at=datetime.fromtimestamp(creation_ts, pytz.utc),
    )
    # this will fail if the file already exists
    raw_file.save(force_insert=True)

    return id_


def delete_raw_file(raw_file_id: str) -> None:
    """Remove raw file from the database."""
    logging.info(f"Removing from DB: {raw_file_id=}")
    connect_db()
    RawFile.objects(id=raw_file_id).delete()


# sentinel value to indicate that a parameter should not be updated
_NO_UPDATE = object()


def update_raw_file(  # noqa: PLR0913
    raw_file_id: str,
    *,
    new_status: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    status_details: str | None = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    size: float = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    file_info: dict[str, tuple[float, str]] = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    backup_base_path: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    backup_status: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
) -> None:
    """Update parameters of DB entity of raw file with `raw_file_id`."""
    logging.info(
        f"Updating DB: {raw_file_id=} to {new_status=} {status_details=} {size=} {file_info=} {backup_base_path=} {backup_status=}"
    )
    connect_db()
    raw_file = RawFile.objects.with_id(raw_file_id)
    logging.info(f"Old DB state: {raw_file.status=} {raw_file.status_details=}")

    # prevent overwriting these fields with None if they are not given
    kwargs = {
        "status": new_status,
        "updated_at_": datetime.now(tz=pytz.utc),
        "status_details": status_details,
        "size": size,
        "file_info": file_info,
        "backup_base_path": backup_base_path,
        "backup_status": backup_status,
    }

    raw_file.update(**{k: v for k, v in kwargs.items() if v != _NO_UPDATE})


def add_metrics_to_raw_file(
    raw_file_id: str, *, metrics_type: str, metrics: dict, settings_version: int
) -> None:
    """Add `metrics` to DB entry of `raw_file_id`."""
    logging.info(f"Adding to DB: {raw_file_id=} <- {metrics=} type={metrics_type}")
    connect_db()
    raw_file = RawFile.objects.get(id=raw_file_id)

    Metrics(
        raw_file=raw_file,
        type=metrics_type,
        settings_version=settings_version,
        **metrics,
    ).save()


def add_project(*, project_id: str, name: str, description: str) -> None:
    """Add a new project to the database."""
    logging.info(f"Adding to DB: {project_id=} {name=} {description=}")
    connect_db()
    project = Project(id=project_id, name=name, description=description)
    # this will fail if the project id already exists
    project.save(force_insert=True)


def get_all_project_ids() -> list[str]:
    """Get all project ids from the database."""
    connect_db()
    return [p.id for p in Project.objects.all()]


def get_settings_for_project(project_id: str) -> Settings:
    """Get a project by its id."""
    logging.info(f"Getting from DB: {project_id=}")
    connect_db()
    project = Project.objects.get(id=project_id)
    return Settings.objects(project=project, status=ProjectStatus.ACTIVE).first()


def add_settings(  # noqa: PLR0913 many arguments in function definition
    *,
    project_id: str,
    name: str,
    fasta_file_name: str,
    speclib_file_name: str,
    config_file_name: str | None,
    config_params: str | None,
    software_type: str,
    software: str,
) -> None:
    """Add new settings to a project."""
    connect_db()
    project = Project.objects.get(id=project_id)

    if (
        current_active_setting := Settings.objects(
            project=project, status=ProjectStatus.ACTIVE
        ).first()
    ) is not None:
        logging.info(
            f"Setting existing setting to INACTIVE: {current_active_setting.id}"
        )
        current_active_setting.status = ProjectStatus.INACTIVE
        current_active_setting.save()

        num_existing_settings = Settings.objects(project=project).all().count()
    else:
        num_existing_settings = 0

    settings = Settings(
        project=project,
        name=name,
        fasta_file_name=fasta_file_name,
        speclib_file_name=speclib_file_name,
        config_file_name=config_file_name,
        config_params=config_params,
        software_type=software_type,
        software=software,
        version=num_existing_settings + 1,
    )
    settings.save(force_insert=True)


def update_kraken_status(
    id_: str,
    *,
    status: str,
    status_details: str,
    free_space_gb: int,
    entity_type: str = KrakenStatusEntities.INSTRUMENT,
) -> None:
    """Update the status of an instrument connected to kraken."""
    logging.info(
        f"Updating DB: {id_=} to {status=} with {status_details=} {free_space_gb=} {entity_type=}"
    )
    connect_db()

    KrakenStatus(
        id=id_,
        status=status,
        updated_at_=datetime.now(tz=pytz.utc),
        free_space_gb=free_space_gb,
        status_details=status_details,
        entity_type=entity_type,
    ).save()
