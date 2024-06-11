"""Module to handle the database connection and the schema."""

import logging
import os
from datetime import datetime

import pytz
from mongoengine import (
    ConnectionFailure,
    DateTimeField,
    Document,
    DynamicDocument,
    FloatField,
    ReferenceField,
    StringField,
    connect,
    disconnect,
)

# Note: this module must not have any dependencies on the rest of the codebase

DB_NAME = "krakendb"

DOCKER_DB_HOST = "mongodb-service"
DB_HOST = os.environ.get(
    "MONGO_HOST", DOCKER_DB_HOST
)  # if mongodb does not run in Docker: use localhost

# nonsensical default values are used by tests only
DB_PORT = int(os.environ.get("MONGO_PORT", 12345))
USER = os.environ.get("MONGO_USER", "user")
PASSWORD = os.environ.get("MONGO_PASSWORD", "user")


class RawFileStatus:
    """Status of raw file."""

    NEW = "new"
    # have a distinction between processing and copying as network drives caused issues in the past.
    COPYING = "copying"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"


def connect_db() -> None:
    """Connect to the database."""
    try:
        disconnect()
        logging.info(f"Connecting to db: {DB_HOST=} {DB_NAME=} {DB_PORT=} {USER=}")

        connect(
            DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
            username=USER,
            password=PASSWORD,
            authentication_source=DB_NAME,
        )
    except ConnectionFailure:
        pass
        # A different connection with alias `default` was already registered.


class RawFile(Document):
    """Schema for a raw file."""

    name = StringField(required=True, primary_key=True)
    status = StringField(max_length=50)

    size = FloatField(min_value=0.0, max_value=1000.0 * 1024**3)  # unit: bytes
    instrument_id = StringField(max_length=50)

    created_at = DateTimeField()
    db_entry_created_at = DateTimeField(default=datetime.now)


class Metrics(DynamicDocument):
    """Schema for metrics calculated for a raw file.

    Inheriting from `DynamicDocument` means that any parameter passed to the model will be added to the DB.
    """

    # https://docs.mongoengine.org/guide/defining-documents.html#dynamic-document-schemas
    db_entry_created_at = DateTimeField(default=datetime.now)

    # https://docs.mongoengine.org/guide/defining-documents.html#reference-fields
    raw_file = ReferenceField(RawFile)


def add_metrics_to_raw_file(raw_file_name: str, metrics: dict) -> None:
    """Add `metrics` to DB entry of `raw_file_name`."""
    connect_db()
    raw_file = RawFile.objects.get(name=raw_file_name)
    Metrics(raw_file=raw_file, **metrics).save()


def get_raw_file_names_from_db(raw_file_names: list[str]) -> list[str]:
    """Get raw files from the database with the given names."""
    connect_db()
    return [r.name for r in RawFile.objects.filter(name__in=raw_file_names)]


def add_new_raw_file_to_db(
    file_name: str, *, instrument_id: str, size: float, creation_ts: float
) -> None:
    """Add a new raw file to the database.

    :param file_name: name of the file
    :param instrument_id: id of the acquiring instrument
    :param size: file size in bytes
    :param creation_ts: creation timestamp (unix)
    :return:
    """
    connect_db()
    raw_file = RawFile(
        name=file_name,
        status=RawFileStatus.NEW,
        size=size,
        instrument_id=instrument_id,
        created_at=datetime.fromtimestamp(creation_ts, pytz.utc),
    )
    # this will fail if the file already exists
    raw_file.save(force_insert=True)
