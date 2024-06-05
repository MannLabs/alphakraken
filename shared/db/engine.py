"""Module to handle the database connection and the schema."""

import logging
import os
from datetime import datetime

from mongoengine import (
    ConnectionFailure,
    DateTimeField,
    Document,
    FloatField,
    StringField,
    connect,
    disconnect,
)

from shared.settings import RawFileStatus

DOCKER_DB_HOST = "mongodb-service"
DB_HOST = os.environ.get("MONGO_HOST", DOCKER_DB_HOST)  # localhost

DB_PORT = int(os.environ.get("MONGO_PORT", 27017))

DB_NAME = "krakendb"


USER = os.environ.get("MONGO_USER", "user")
PASSWORD = os.environ.get("MONGO_PASSWORD", "user")


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

    size = FloatField(min_value=0.0, max_value=1000.0)
    instrument_id = StringField(max_length=50)

    created_at = DateTimeField(default=datetime.now)


def get_raw_file_names_from_db(raw_file_names: list[str]) -> list[str]:
    """Get raw files from the database with the given names."""
    connect_db()
    return [r.name for r in RawFile.objects.filter(name__in=raw_file_names)]


def add_new_raw_file_to_db(
    raw_file_name: str, *, instrument_id: str, raw_file_size: float
) -> None:
    """Add a new raw file to the database."""
    connect_db()
    raw_file = RawFile(
        name=raw_file_name,
        status=RawFileStatus.NEW,
        size=raw_file_size,
        instrument_id=instrument_id,
    )
    # this will fail if the file already exists
    raw_file.save(force_insert=True)
