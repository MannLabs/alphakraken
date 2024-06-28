"""Database models for the MongoDB.

Note: this module must not have any dependencies on the rest of the codebase.
"""

from datetime import datetime

from mongoengine import (
    DateTimeField,
    Document,
    DynamicDocument,
    FloatField,
    ReferenceField,
    StringField,
)


class RawFileStatus:
    """Status of raw file."""

    IGNORED = "ignored"
    NEW = "new"  # queued_for_monitoring? (only if there is a queue here)
    # have a distinction between processing and copying as network drives caused issues in the past.
    ACQUISITION_STARTED = "acquisition_started"
    ACQUISITION_FINISHED = "acquisition_finished"
    COPYING = "copying"
    COPYING_FINISHED = "copying_finished"
    # # queued(_for_processing)
    PROCESSING = "processing"  # quanting
    PROCESSED = "processed"  # quanted
    ERROR = "error"


class RawFile(Document):
    """Schema for a raw file."""

    name = StringField(required=True, primary_key=True)
    status = StringField(max_length=32)
    status_details = StringField(max_length=256)

    size = FloatField(
        min_value=-1.0, max_value=1000.0 * 1024**3, default=-1.0
    )  # unit: bytes
    instrument_id = StringField(max_length=50)

    project_id = StringField(max_length=32)

    created_at = DateTimeField()  # when file was created

    # audit fields
    created_at_ = DateTimeField(default=datetime.now)
    updated_at_ = DateTimeField(default=datetime.now)


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
