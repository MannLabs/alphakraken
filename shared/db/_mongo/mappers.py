"""Mapping of Mongo documents to domain dataclasses."""

from datetime import UTC, datetime
from typing import cast

from shared.db import models
from shared.domain.raw_files import FileInfoItem, RawFile


def as_utc(value: datetime) -> datetime:
    """Return the datetime UTC-aware; pymongo returns naive datetimes that are UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def as_utc_or_none(value: datetime | None) -> datetime | None:
    """Like `as_utc`, for optional datetimes."""
    return None if value is None else as_utc(value)


def to_raw_file(doc: models.RawFile, *, file_info_loaded: bool) -> RawFile:
    """Map a raw file document; `file_info` is None unless it was loaded."""
    return RawFile(
        id=doc.id,
        collision_flag=doc.collision_flag,
        original_name=doc.original_name,
        instrument_id=doc.instrument_id,
        project_id=doc.project_id,
        status=doc.status,
        status_details=doc.status_details,
        size=doc.size,
        backup_status=doc.backup_status,
        backup_display_base_path=doc.backup_display_base_path,
        s3_upload_path=doc.s3_upload_path,
        instrument_file_status=doc.instrument_file_status,
        file_info=_to_file_info(doc.file_info) if file_info_loaded else None,
        created_at=as_utc(doc.created_at),
        finalized_at=as_utc_or_none(doc.finalized_at),
        created_at_=as_utc(doc.created_at_),
        updated_at_=as_utc(doc.updated_at_),
    )


def _to_file_info(file_info: dict[str, list]) -> dict[str, FileInfoItem]:
    """Return the file info with its items as tuples; Mongo stores them as lists."""
    return {path: cast("FileInfoItem", tuple(item)) for path, item in file_info.items()}
