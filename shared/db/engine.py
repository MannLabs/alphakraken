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

DEFAULT_PROTOCOL = "mongodb://"
DEFAULT_HOST = "mongodb-service"

DB_NAME = "krakendb"
DB_PORT = int(os.environ.get("MONGO_PORT", 27017))


USER = os.environ.get("MONGO_USER")
PASSWORD = os.environ.get("MONGO_PASSWORD")


def connect_db(host: str = DEFAULT_HOST, port: int = DB_PORT) -> None:
    """Connect to the database."""
    try:
        disconnect()
        logging.info(f"Connecting to db: {host=} {DB_NAME=} {port=} {USER=}")

        connect(
            DB_NAME,
            host=host,
            port=port,
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
