"""Database models for the MongoDB.

Note: this module must not have any dependencies on the rest of the codebase.
"""

from datetime import datetime

from mongoengine import (
    DateTimeField,
    Document,
    DynamicDocument,
    IntField,
    ReferenceField,
    StringField,
)


class RawFileStatus:
    """Status of raw file.

    Not every status is required for consecutive tasks (e.g. `ACQUISITION_FINISHED`).
    The last task of a DAG should set a status (e.g. `COPYING_FINISHED`).
    """

    IGNORED = "ignored"

    QUEUED_FOR_MONITORING = "queued_for_monitoring"
    MONITORING_ACQUISITION = "monitoring_acquisition"
    MONITORING_DONE = "monitoring_done"

    COPYING = "copying"
    COPYING_DONE = "copying_done"

    QUEUED_FOR_QUANTING = "queued_for_quanting"
    QUANTING = "quanting"
    QUANTING_FAILED = "quanting_failed"

    ERROR = "error"
    DONE = "done"


class RawFile(Document):
    """Schema for a raw file."""

    # TODO: get rid of "name" -> "id"
    # Unique identifier of the file. Either the raw file name or, in case of a collision,
    # the raw file name with a unique prefix.
    name = StringField(max_length=128, required=True, primary_key=True)

    # Unique prefix to indicate a collision. If None, no collision occurred.
    collision_flag = StringField(max_length=32, default=None)

    # Original name of the file. In case of collisions, this is not unique
    original_name = StringField(max_length=128, required=True)

    status = StringField(max_length=32)
    status_details = StringField(max_length=256)

    size = IntField(min_value=0, max_value=int(1000 * 1024**3))  # unit: bytes
    instrument_id = StringField(max_length=50)

    project_id = StringField(max_length=32)

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

    # audit fields
    created_at_ = DateTimeField(default=datetime.now)


class ProjectStatus:
    """Status of project."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"


class Project(Document):
    """Schema for a project."""

    id = StringField(required=True, primary_key=True, max_length=16)
    name = StringField(required=True, max_length=64)
    description = StringField(max_length=256)

    status = StringField(max_length=32, default=ProjectStatus.ACTIVE)

    # missing: created by
    created_at_ = DateTimeField(default=datetime.now)


class Settings(Document):
    """Schema for quanting settings."""

    project = ReferenceField(
        Project,
    )

    name = StringField(required=True, primary_key=True, max_length=64)

    # although only one of (speclib, fasta) is required by alphaDIA,
    # on DB level, we want both filled (empty string is alright)
    fasta_file_name = StringField(required=True, max_length=128)
    speclib_file_name = StringField(required=True, max_length=128)

    config_file_name = StringField(required=True, max_length=128)
    software = StringField(required=True, max_length=128)

    status = StringField(max_length=64, default=ProjectStatus.ACTIVE)

    created_at_ = DateTimeField(default=datetime.now)


class KrakenStatusValues:
    """Status of kraken."""

    OK = "ok"
    ERROR = "error"


class KrakenStatus(Document):
    """Schema for a project."""

    instrument_id = StringField(max_length=64, required=True, primary_key=True)

    status = StringField(max_length=64, default=ProjectStatus.ACTIVE)
    status_details = StringField(max_length=256)

    updated_at_ = DateTimeField(default=datetime.now)
    last_error_occurred_at = DateTimeField(default=datetime.fromtimestamp(0))  # noqa: DTZ006
