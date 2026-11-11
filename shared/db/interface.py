"""Module to handle interactions with the database.

Note: this module must not have any dependencies on the rest of the codebase.
"""

import logging
import re
from datetime import UTC, datetime, timedelta

from shared.db.engine import connect_db
from shared.db.models import (
    RawFile,
)
from shared.domain.raw_files import FileInfoItem, InstrumentFileStatus


def get_raw_files_by_names(
    raw_file_names: list[str], *, case_sensitive: bool = False
) -> list[RawFile]:
    """Get raw files from the database with the given original names, by default ignoring case."""
    logging.info(f"Getting from DB: {raw_file_names=} {case_sensitive=}")
    connect_db()
    names: list[str] | list[re.Pattern[str]] = (
        raw_file_names
        if case_sensitive
        else [
            re.compile(f"^{re.escape(name)}$", re.IGNORECASE) for name in raw_file_names
        ]
    )
    return list(RawFile.objects.filter(original_name__in=names))


def get_raw_file_by_id(raw_file_id: str) -> RawFile:
    """Get raw file from the database with the given id."""
    logging.info(f"Getting from DB: {raw_file_id=}")
    connect_db()
    return RawFile.objects(id=raw_file_id).first()


def get_purge_candidates_by_age(
    instrument_id: str,
    *,
    min_age_in_days: float,
) -> list[RawFile]:
    """Get raw files older than the given age in days for a given instrument that are not purged, renamed or disappeared, sorted 'oldest first'."""
    connect_db()

    youngest_created_at = datetime.now(tz=UTC) - timedelta(hours=min_age_in_days * 24)
    logging.info(f"Getting from DB: {instrument_id=} {youngest_created_at=}")

    return list(
        RawFile.objects.filter(
            instrument_id=instrument_id,
            created_at__lt=youngest_created_at,
            instrument_file_status__nin=[
                InstrumentFileStatus.PURGED,
                InstrumentFileStatus.RENAMED,
                InstrumentFileStatus.DISAPPEARED,
            ],
        ).order_by("created_at")
    )


def add_raw_file(  # noqa: PLR0913 too many arguments
    file_name: str,
    collision_flag: str | None = None,
    *,
    project_id: str | None,
    status: str,
    instrument_file_status: str,
    instrument_id: str,
    creation_ts: float,
) -> str:
    """Add a new raw file to the database.

    :param file_name: name of the file
    :param collision_flag: optional flag to indicate a collision
    :param project_id: project id
    :param status: status of the file
    :param instrument_file_status: status of the physical file on the instrument, see InstrumentFileStatus for possible values
    :param instrument_id: id of the acquiring instrument
    :param creation_ts: creation timestamp (unix)
    :return: the raw file id. This is either equal to the raw_file_name or has a collision flag prefixed
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
        instrument_file_status=instrument_file_status,
        created_at=datetime.fromtimestamp(creation_ts, UTC),
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
    file_info: dict[str, FileInfoItem] = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    backup_status: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    backup_display_base_path: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    s3_upload_path: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    instrument_file_status: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    finalized_at: datetime | None = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    created_at: datetime = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
) -> None:
    """Update parameters of DB entity of raw file with `raw_file_id`."""
    logging.info(
        f"Updating DB: {raw_file_id=} to {new_status=} {status_details=} {size=} {file_info=} {backup_status=} {backup_display_base_path=} {s3_upload_path=} {instrument_file_status=} {finalized_at=} {created_at=}"
    )
    connect_db()
    raw_file = RawFile.objects.with_id(raw_file_id)
    logging.info(f"Old DB state: {raw_file.status=} {raw_file.status_details=}")

    # prevent overwriting these fields with None if they are not given
    kwargs = {
        "status": new_status,
        "updated_at_": datetime.now(tz=UTC),
        "status_details": status_details,
        "size": size,
        "file_info": file_info,
        "backup_status": backup_status,
        "backup_display_base_path": backup_display_base_path,
        "s3_upload_path": s3_upload_path,
        "instrument_file_status": instrument_file_status,
        "finalized_at": finalized_at,
        "created_at": created_at,
    }

    raw_file.update(**{k: v for k, v in kwargs.items() if v != _NO_UPDATE})
