"""Mapping of Mongo documents to domain dataclasses."""

from datetime import UTC, datetime
from typing import cast, overload

from shared.db import models
from shared.domain.alerts import AlertIssue, AlertSuppression
from shared.domain.raw_files import FileInfoItem, RawFile
from shared.domain.status import KrakenStatus
from shared.domain.users import User


@overload
def as_utc(value: datetime) -> datetime: ...
@overload
def as_utc(value: None) -> None: ...
@overload
def as_utc(value: datetime | None) -> datetime | None: ...
def as_utc(value: datetime | None) -> datetime | None:
    """Return the datetime UTC-aware; pymongo returns naive datetimes that are UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
        finalized_at=as_utc(doc.finalized_at),
        created_at_=as_utc(doc.created_at_),
        updated_at_=as_utc(doc.updated_at_),
    )


def _to_file_info(file_info: dict[str, list]) -> dict[str, FileInfoItem]:
    """Return the file info with its items as tuples; Mongo stores them as lists."""
    return {path: cast("FileInfoItem", tuple(item)) for path, item in file_info.items()}


def to_kraken_status(doc: models.KrakenStatus) -> KrakenStatus:
    """Map a kraken status document."""
    return KrakenStatus(
        id=doc.id,
        status=doc.status,
        status_details=doc.status_details,
        free_space_gb=doc.free_space_gb,
        entity_type=doc.entity_type,
        updated_at_=as_utc(doc.updated_at_),
    )


def to_alert_issue(doc: models.AlertIssue) -> AlertIssue:
    """Map an alert issue document."""
    return AlertIssue(
        id=str(doc.pk),
        alert=doc.alert,
        key=doc.key,
        first_seen=as_utc(doc.first_seen),
        last_notified=as_utc(doc.last_notified),
    )


def to_alert_suppression(doc: models.AlertSuppression) -> AlertSuppression:
    """Map an alert suppression document."""
    return AlertSuppression(
        id=str(doc.pk),
        alert=doc.alert,
        instrument_id=doc.instrument_id,
        identifier=doc.identifier,
        details=doc.details,
        comment=doc.comment,
        expires_at=as_utc(doc.expires_at),
        created_by=doc.created_by,
        created_at_=as_utc(doc.created_at_),
    )


def to_user(doc: models.User) -> User:
    """Map a user document."""
    return User(
        email=doc.email,
        initials=doc.initials,
        messenger_id=doc.messenger_id,
        status=doc.status,
        created_at_=as_utc(doc.created_at_),
    )
