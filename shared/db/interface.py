"""Module to handle interactions with the database.

Note: this module must not have any dependencies on the rest of the codebase.
"""

import logging
from datetime import datetime

import pytz

from shared.db.engine import connect_db
from shared.db.models import (
    KrakenStatus,
    KrakenStatusValues,
    Metrics,
    Project,
    ProjectStatus,
    RawFile,
    Settings,
)


def get_raw_file_names_from_db(raw_file_names: list[str]) -> list[RawFile]:
    """Get raw files from the database with the given original names."""
    logging.info(f"Getting from DB: {raw_file_names=}")
    connect_db()
    return list(RawFile.objects.filter(original_name__in=raw_file_names))


def add_new_raw_file_to_db(  # noqa: PLR0913 too many arguments
    file_name: str,
    collision_flag: str | None = None,
    *,
    project_id: str,
    status: str,
    instrument_id: str,
    creation_ts: float,
) -> None:
    """Add a new raw file to the database.

    :param file_name: name of the file
    :param collision_flag: optional flag to indicate a collision
    :param project_id: project id
    :param status: status of the file
    :param instrument_id: id of the acquiring instrument
    :param creation_ts: creation timestamp (unix)
    :return:
    """
    logging.info(
        f"Adding to DB: {file_name=} {collision_flag=} {project_id=} {status=} {instrument_id=} {creation_ts=}"
    )
    connect_db()
    raw_file = RawFile(
        name=collision_flag + file_name,
        collision_flag=collision_flag,
        original_name=file_name,
        project_id=project_id,
        instrument_id=instrument_id,
        status=status,
        created_at=datetime.fromtimestamp(creation_ts, pytz.utc),
    )
    # this will fail if the file already exists
    raw_file.save(force_insert=True)


def update_raw_file(
    raw_file_name: str,
    *,
    new_status: str,
    status_details: str | None = None,
    size: float | None = None,
) -> None:
    """Set `status` and `size` of DB entity of `raw_file_name` to `new_status`."""
    logging.info(
        f"Updating DB: {raw_file_name=} to {new_status=} with {status_details=} and {size=}"
    )
    connect_db()
    raw_file = RawFile.objects.with_id(raw_file_name)
    logging.info(f"Old DB state: {raw_file.status=} {raw_file.status_details=}")

    # prevent overwriting the size with None if it is not given
    optional_args = {"size": size} if size is not None else {}

    raw_file.update(
        status=new_status,
        updated_at_=datetime.now(tz=pytz.utc),
        status_details=status_details,
        **optional_args,
    )


def add_metrics_to_raw_file(raw_file_name: str, metrics: dict) -> None:
    """Add `metrics` to DB entry of `raw_file_name`."""
    logging.info(f"Adding to DB: {raw_file_name=} <- {metrics=}")
    connect_db()
    raw_file = RawFile.objects.get(name=raw_file_name)
    Metrics(raw_file=raw_file, **metrics).save()


def add_new_project_to_db(*, project_id: str, name: str, description: str) -> None:
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


def add_new_settings_to_db(  # noqa: PLR0913 Too many arguments in function definition
    *,
    project_id: str,
    name: str,
    fasta_file_name: str,
    speclib_file_name: str,
    config_file_name: str,
    software: str,
) -> None:
    """Add new settings to a project."""
    connect_db()
    project = Project.objects.get(id=project_id)

    # TODO: get rid of this limitation: on adding a new setting for a project, set the status of the ACTIVE one to INACTIVE
    if Settings.objects(project=project).first() is not None:
        raise ValueError("Currently, only one settings per project is allowed.")

    settings = Settings(
        project=project,
        name=name,
        fasta_file_name=fasta_file_name,
        speclib_file_name=speclib_file_name,
        config_file_name=config_file_name,
        software=software,
    )
    settings.save(force_insert=True)


def update_kraken_status(
    instrument_id: str, *, status: str, status_details: str
) -> None:
    """Update the status of a instrument connected to kraken."""
    logging.info(f"Updating DB: {instrument_id=} to {status=} with {status_details=}")
    connect_db()
    optional_args = (
        {"last_error_occurred_at": datetime.now(tz=pytz.utc)}
        if status == KrakenStatusValues.ERROR
        else {}
    )

    KrakenStatus(
        instrument_id=instrument_id,
        status=status,
        updated_at_=datetime.now(tz=pytz.utc),
        status_details=status_details,
        **optional_args,
    ).save()
