"""Module to handle interactions with the database.

Note: this module must not have any dependencies on the rest of the codebase.
"""

import logging
from datetime import datetime

import pytz

from shared.db.engine import connect_db
from shared.db.models import Metrics, Project, ProjectStatus, RawFile, Settings


def get_raw_file_names_from_db(raw_file_names: list[str]) -> list[str]:
    """Get raw files from the database with the given names."""
    logging.info(f"Getting from DB: {raw_file_names=}")
    connect_db()
    return [r.name for r in RawFile.objects.filter(name__in=raw_file_names)]


def add_new_raw_file_to_db(
    file_name: str, *, status: str, instrument_id: str, size: float, creation_ts: float
) -> None:
    """Add a new raw file to the database.

    :param file_name: name of the file
    :param status: status of the file
    :param instrument_id: id of the acquiring instrument
    :param size: file size in bytes
    :param creation_ts: creation timestamp (unix)
    :return:
    """
    logging.info(f"Adding to DB: {file_name=} {instrument_id=} {size=} {creation_ts=}")
    connect_db()
    raw_file = RawFile(
        name=file_name,
        status=status,
        size=size,
        instrument_id=instrument_id,
        created_at=datetime.fromtimestamp(creation_ts, pytz.utc),
    )
    # this will fail if the file already exists
    raw_file.save(force_insert=True)


def update_raw_file_status(raw_file_name: str, new_status: str) -> None:
    """Set `status` of DB entity of `raw_file_name` to `new_status`."""
    logging.info(f"Updating DB: {raw_file_name=} to {new_status=}")
    connect_db()
    raw_file = RawFile.objects.with_id(raw_file_name)
    raw_file.update(status=new_status)


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
