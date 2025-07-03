"""Database models for the MongoDB.

Note: this module must not have any dependencies on the rest of the codebase.
"""

from datetime import datetime

from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    DynamicDocument,
    IntField,
    ReferenceField,
    StringField,
)

from shared.keys import MetricsTypes


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

    QUEUED_FOR_QUANTING = "queued_for_quanting"
    QUANTING = "quanting"
    QUANTING_FAILED = "quanting_failed"

    ERROR = "error"
    DONE = "done"
    DONE_NOT_QUANTED = "done_not_quanted"


class BackupStatus:
    """Status of backup."""

    IN_PROGRESS = "in_progress"
    DONE = "done"
    SKIPPED = "skipped"
    FAILED = "failed"


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


class RawFile(Document):
    """Schema for a raw file."""

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

    file_info = DictField()  # mapping of relative path on backup to tuples (size: int, hash: str). When read from DB, the tuples are converted to lists.

    created_at = DateTimeField()  # when file was created

    # audit fields
    created_at_ = DateTimeField(default=datetime.now)
    updated_at_ = DateTimeField(default=datetime.now)


def get_created_at_year_month(raw_file: RawFile) -> str:
    """Get the year and month of the raw file creation date."""
    return raw_file.created_at.strftime("%Y_%m")


class Metrics(DynamicDocument):
    """Schema for metrics calculated for a raw file.

    Inheriting from `DynamicDocument` means that any parameter passed to the model will be added to the DB.
    cf. https://docs.mongoengine.org/guide/defining-documents.html#dynamic-document-schemas
    """

    # https://docs.mongoengine.org/guide/defining-documents.html#reference-fields
    raw_file = ReferenceField(RawFile)

    settings_version = IntField(min_value=1, default=1)

    # Type of metrics: "alphadia" (default) or "custom"
    type = StringField(max_length=32, default=MetricsTypes.ALPHADIA)

    # audit fields
    created_at_ = DateTimeField(default=datetime.now)


class ProjectStatus:
    """Status of project."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"


class Project(Document):
    """Schema for a project."""

    id = StringField(required=True, primary_key=True, min_length=3, max_length=16)
    name = StringField(required=True, max_length=64)
    description = StringField(max_length=512)

    status = StringField(max_length=32, default=ProjectStatus.ACTIVE)

    # missing: created by
    created_at_ = DateTimeField(default=datetime.now)


class Settings(Document):
    """Schema for quanting settings."""

    project = ReferenceField(
        Project,
    )

    name = StringField(required=True, max_length=64)
    version = IntField(min_value=1, default=1)
    # TODO: add description = StringField(max_length=512)

    # although only one of (speclib, fasta) is required by alphaDIA,
    # on DB level, we want both filled (empty string is alright)
    fasta_file_name = StringField(required=True, max_length=128)
    speclib_file_name = StringField(required=True, max_length=128)

    config_file_name = StringField(required=True, max_length=128)
    software = StringField(required=True, max_length=128)

    status = StringField(max_length=64, default=ProjectStatus.ACTIVE)

    created_at_ = DateTimeField(default=datetime.now)


class KrakenStatusValues:
    """Values of kraken health status."""

    OK = "ok"
    ERROR = "error"


class KrakenStatus(Document):
    """Schema for a project."""

    instrument_id = StringField(max_length=64, required=True, primary_key=True)

    status = StringField(max_length=64, default="n/a")
    status_details = StringField(max_length=256)

    free_space_gb = IntField(min_value=-1, default=-1)

    updated_at_ = DateTimeField(default=datetime.now)
