"""Module to handle the database connection and the schema."""

import logging
import os
from datetime import datetime

from mongoengine import (
    ConnectionFailure,
    DateTimeField,
    Document,
    StringField,
    connect,
    disconnect,
)

DOCKER_DB_HOST = "mongodb-service"
DB_HOST = os.environ.get("MONGO_HOST", DOCKER_DB_HOST)

DB_PORT = int(os.environ.get("MONGO_PORT", 27017))

DB_NAME = "krakendb"


USER = os.environ.get("MONGO_USER")
PASSWORD = os.environ.get("MONGO_PASSWORD")


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
    created_at = DateTimeField(default=datetime.now)
