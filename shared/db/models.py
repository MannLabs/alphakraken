"""Database models for the MongoDB.

Note: this module must not have any dependencies on the rest of the codebase.
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar

from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    DynamicDocument,
    IntField,
    QuerySet,
    ReferenceField,
    StringField,
)

from shared.keys import MetricsTypes

FileInfoItem = (
    tuple[float | None, str | None] | tuple[float | None, str | None, str | None]
)


class RawFileStatus:
    """Status of raw file.

    Not every status is required for consecutive tasks (e.g. `ACQUISITION_FINISHED`).
    The last task of a DAG should set a status (e.g. `COPYING_FINISHED`).

    Note that you might adapt the webapp when a new status is added, e.g. update TERMINAL_STATUSES, ERROR_STATUSES, highlight_status_cell()
    """

    IGNORED = "ignored"

    QUEUED_FOR_MONITORING = "queued_for_monitoring"
    MONITORING_ACQUISITION = "monitoring_acquisition"
    MONITORING_DONE = "monitoring_done"

    CHECKSUMMING = "checksumming"
    CHECKSUMMING_DONE = "checksumming_done"
    COPYING = "copying"
    COPYING_DONE = "copying_done"
    ACQUISITION_FAILED = "acquisition_failed"

    COMPUTING_MSQC = "computing_msqc"
    QUEUED_FOR_QUANTING = "queued_for_quanting"
    QUANTING = "quanting"
    QUANTING_FAILED = "quanting_failed"

    ERROR = "error"
    DONE = "done"
    DONE_NOT_QUANTED = "done_not_quanted"


class BackupStatus:
    """Status of backup.

    "copying" refers to copying to local backup location, "upload" to uploading to S3.
    """

    COPYING_IN_PROGRESS = "copying_in_progress"
    COPYING_DONE = "copying_done"
    COPYING_FAILED = "copying_failed"

    UPLOAD_IN_PROGRESS = "upload_in_progress"
    UPLOAD_DONE = "upload_done"
    UPLOAD_FAILED = "upload_failed"

    SKIPPED = "skipped"


ERROR_STATUSES = [
    RawFileStatus.ERROR,
    RawFileStatus.QUANTING_FAILED,
    RawFileStatus.ACQUISITION_FAILED,
]
TERMINAL_STATUSES = [
    *ERROR_STATUSES,
    RawFileStatus.DONE,
    RawFileStatus.DONE_NOT_QUANTED,
    RawFileStatus.IGNORED,
]


class InstrumentFileStatus:
    """Status of files on instrument and their lifecycle."""

    NA = "n/a"  # for backwards compatibility
    NEW = "new"  # File exists on instrument, not yet moved
    MOVED = "moved"  # File moved from instrument to instrument backup folder
    PURGED = "purged"  # File removed from instrument backup folder


class RawFile(Document):
    """Schema for a raw file."""

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[RawFile]]

    # Unique identifier of the file. Either the raw file name or, in case of a collision,
    # the raw file name with a unique prefix.
    id = StringField(
        max_length=255, required=True, primary_key=True
    )  # max_length of file names on linux: 255

    # Unique prefix to indicate a collision. If None, no collision occurred.
    collision_flag = StringField(max_length=32, default=None)

    # Original name of the file. In case of collisions, this is not unique. Otherwise, equal to `id`.
    original_name = StringField(max_length=255, required=True)

    instrument_id = StringField(max_length=32)
    project_id = StringField(max_length=32)

    status = StringField(max_length=32)
    status_details = StringField(max_length=512)

    size = IntField(min_value=-1, max_value=int(1000 * 1024**3))  # unit: bytes

    backup_base_path = StringField(
        max_length=128
    )  # absolute path to pool backup location

    backup_status = StringField(max_length=32, default=None)

    # bucket_name or bucket_name/sub_path/ -> file_info paths are always relative to that
    s3_upload_path = StringField(max_length=1024, default=None)

    instrument_file_status = StringField(max_length=16, default=InstrumentFileStatus.NA)

    file_info = DictField()  # mapping of file paths (relative to backup_base_path) to tuples (size: int, hash: str).
    # When read from DB, the tuples are converted to lists.

    created_at = DateTimeField()  # when file was created

    # audit fields
    created_at_ = DateTimeField(default=datetime.now)
    updated_at_ = DateTimeField(default=datetime.now)


def parse_file_info_item(
    item: FileInfoItem | None,
) -> tuple[float | None, str | None]:
    """Parse a file_info item tuple.

    This is to hide the details of the file_info storage format from the rest of the codebase.

    Args:
        item: The file_info item tuple to parse.

    Returns:
        A tuple containing the size and hash.

    Raises:
        ValueError: If the item length is invalid.

    """
    if not item:
        return item  # type: ignore[invalid-return-type]

    if len(item) >= 2:  # noqa: PLR2004
        return item[0], item[1]

    raise ValueError(f"Invalid file_info item length {len(item)} {item}.")


def get_created_at_year_month(raw_file: RawFile) -> str:
    """Get the year and month of the raw file creation date."""
    return raw_file.created_at.strftime("%Y_%m")


class Metrics(DynamicDocument):
    """Schema for metrics calculated for a raw file.

    Inheriting from `DynamicDocument` means that any parameter passed to the model will be added to the DB.
    cf. https://docs.mongoengine.org/guide/defining-documents.html#dynamic-document-schemas
    """

    objects: ClassVar[QuerySet[Metrics]]

    # https://docs.mongoengine.org/guide/defining-documents.html#reference-fields
    raw_file = ReferenceField(RawFile)

    settings_name = StringField(max_length=64, default="n/a")
    settings_version = IntField(min_value=1, default=1)

    # Type of metrics: "alphadia" (default) or "custom"
    type = StringField(max_length=32, default=MetricsTypes.ALPHADIA)

    # audit fields
    created_at_ = DateTimeField(default=datetime.now)


class ProjectStatus:
    """Status of project."""

    ACTIVE = "active"
    INACTIVE = "inactive"


class SettingsStatus:
    """Status of settings."""

    ACTIVE = "active"
    INACTIVE = "inactive"


class Project(Document):
    """Schema for a project."""

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[Project]]

    id = StringField(
        required=True, primary_key=True, min_length=3, max_length=16
    )  # TODO: set min_length to 5
    name = StringField(required=True, max_length=64)
    description = StringField(max_length=512)

    settings = ReferenceField("Settings", required=False)

    status = StringField(max_length=32, default=ProjectStatus.ACTIVE)

    # missing: created by
    created_at_ = DateTimeField(default=datetime.now)


SETTINGS_NAME_REGEX = r"^[a-zA-Z0-9_]+$"


class Settings(Document):
    """Schema for quanting settings."""

    meta: ClassVar = {
        "strict": False,
        "indexes": [{"fields": ["name", "version"], "unique": True}],
        "auto_create_index": False,  # to avoid adding write rights to read-only connections
    }
    objects: ClassVar[QuerySet[Settings]]

    name = StringField(required=True, max_length=64, regex=SETTINGS_NAME_REGEX)
    version = IntField(min_value=1, default=1)
    description = StringField(max_length=512)

    # although only one of (speclib, fasta) is required by alphaDIA,
    # on DB level, we want both filled (empty string is alright)
    fasta_file_name = StringField(required=True, max_length=128)
    speclib_file_name = StringField(required=True, max_length=128)

    config_file_name = StringField(required=False, max_length=128)
    config_params = StringField(required=False, max_length=512)

    software_type = StringField(
        required=True,
        max_length=128,
        default="alphadia",  # TODO: remove, default is just for backwards compatibility
    )
    software = StringField(required=True, max_length=128)

    status = StringField(max_length=64, default=SettingsStatus.ACTIVE)

    created_at_ = DateTimeField(default=datetime.now)


class KrakenStatusValues:
    """Values of kraken health status."""

    OK = "ok"
    ERROR = "error"


class KrakenStatusEntities:
    """Entities for Kraken status updates."""

    INSTRUMENT = "instrument"
    FILE_SYSTEM = "file_system"
    JOB = "job"


class KrakenStatus(Document):
    """Schema for the Kraken status, representing health, status details, free space, entity type, and last update time."""

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[KrakenStatus]]

    id = StringField(max_length=64, required=True, primary_key=True)

    status = StringField(max_length=64, default="n/a")
    status_details = StringField(max_length=256)

    free_space_gb = IntField(min_value=-1, default=-1)
    entity_type = StringField(max_length=16, default=KrakenStatusEntities.INSTRUMENT)

    updated_at_ = DateTimeField(default=datetime.now)
