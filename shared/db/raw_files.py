"""Raw file queries."""

import logging
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from typing import Any, cast

from shared.db._mongo import raw_files as _backend
from shared.domain.raw_files import FileInfoItem, RawFile

# "-" prefix for descending: widely understood ORM convention but strictly speaking a _backend detail
DEFAULT_ORDER = "-created_at"

_ORDER_FIELDS = frozenset(field.name for field in fields(RawFile))


@dataclass(frozen=True)
class RawFileFilter:
    """Conditions a raw file must all meet; None means no condition.

    `*_after` and `*_before` are inclusive bounds, `name_contains` and `original_names` are
    case-insensitive.
    """

    ids: list[str] | None = None
    original_names: list[str] | None = None
    instrument_id: str | None = None
    instrument_ids: list[str] | None = None
    project_id: str | None = None
    name_contains: str | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    updated_after: datetime | None = None
    status: str | None = None
    statuses: list[str] | None = None
    statuses_not: list[str] | None = None
    backup_status: str | None = None
    instrument_file_status: str | None = None
    instrument_file_statuses_not: list[str] | None = None


def find(
    where: RawFileFilter,
    *,
    order: str = DEFAULT_ORDER,
    limit: int | None = None,
    offset: int = 0,
    include_file_info: bool = False,
) -> list[RawFile]:
    """Return the raw files matching `where`.

    :param order: a field name of `RawFile`, prefixed with "-" for descending
    :param include_file_info: load `file_info`, which can be large; otherwise it is None
    """
    if order.removeprefix("-") not in _ORDER_FIELDS:
        raise ValueError(f"Unknown order field: {order}")
    return _backend.find(
        where,
        order=order,
        limit=limit,
        offset=offset,
        include_file_info=include_file_info,
    )


def count(where: RawFileFilter) -> int:
    """Return the number of raw files matching `where`."""
    return _backend.count(where)


def latest_per_instrument(
    n: int, *, created_after: datetime
) -> dict[str, list[RawFile]]:
    """Return the `n` newest raw files created at or after `created_after` per instrument, newest first.

    Instruments without such files have no entry. `file_info` is not loaded.
    """
    return _backend.latest_per_instrument(n, created_after=created_after)


def count_by_status(where: RawFileFilter) -> dict[str, int]:
    """Return the number of raw files matching `where` per status."""
    return _backend.count_by_status(where)


def get(raw_file_id: str, *, include_file_info: bool = False) -> RawFile:
    """Return the raw file with the given id.

    :param include_file_info: load `file_info`, which can be large; otherwise it is None

    Raises:
        NotFoundError: if there is none.

    """
    logging.info(f"Getting from DB: {raw_file_id=}")
    return _backend.get(raw_file_id, include_file_info=include_file_info)


def add(  # noqa: PLR0913 too many arguments
    file_name: str,
    collision_flag: str | None = None,
    *,
    project_id: str | None,
    status: str,
    instrument_file_status: str,
    instrument_id: str,
    creation_ts: float,
) -> str:
    """Add a new raw file.

    :param file_name: name of the file
    :param collision_flag: optional flag to indicate a collision
    :param project_id: project id
    :param status: status of the file
    :param instrument_file_status: status of the physical file on the instrument, see InstrumentFileStatus for possible values
    :param instrument_id: id of the acquiring instrument
    :param creation_ts: creation timestamp (unix)
    :return: the raw file id. This is either equal to the raw_file_name or has a collision flag prefixed

    Raises:
        AlreadyExistsError: if a raw file with that id exists.

    """
    logging.info(
        f"Adding to DB: {file_name=} {collision_flag=} {project_id=} {status=} {instrument_id=} {creation_ts=}"
    )
    id_ = file_name if collision_flag is None else f"{collision_flag}{file_name}"

    _backend.add(
        id_,
        collision_flag=collision_flag,
        original_name=file_name,
        project_id=project_id,
        instrument_id=instrument_id,
        status=status,
        instrument_file_status=instrument_file_status,
        created_at=datetime.fromtimestamp(creation_ts, UTC),
    )
    return id_


def delete(raw_file_id: str) -> None:
    """Remove a raw file."""
    logging.info(f"Removing from DB: {raw_file_id=}")
    _backend.delete(raw_file_id)


# sentinel value to indicate that a parameter should not be updated
NO_UPDATE = cast("Any", object())


def update(  # noqa: PLR0913
    raw_file_id: str,
    *,
    new_status: str = NO_UPDATE,
    status_details: str | None = NO_UPDATE,
    size: float = NO_UPDATE,
    file_info: dict[str, FileInfoItem] = NO_UPDATE,
    backup_status: str = NO_UPDATE,
    backup_display_base_path: str = NO_UPDATE,
    s3_upload_path: str = NO_UPDATE,
    instrument_file_status: str = NO_UPDATE,
    finalized_at: datetime | None = NO_UPDATE,
    created_at: datetime = NO_UPDATE,
) -> None:
    """Update the given fields of the raw file with `raw_file_id`; fields left out keep their value.

    Raises:
        NotFoundError: if there is no such raw file.

    """
    logging.info(
        f"Updating DB: {raw_file_id=} to {new_status=} {status_details=} {size=} {file_info=} {backup_status=} {backup_display_base_path=} {s3_upload_path=} {instrument_file_status=} {finalized_at=} {created_at=}"
    )
    # prevent overwriting these fields with None if they are not given
    fields_ = {
        "status": new_status,
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
    _backend.update(
        raw_file_id, {k: v for k, v in fields_.items() if v is not NO_UPDATE}
    )
