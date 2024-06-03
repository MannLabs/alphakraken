"""Module to handle the database connection and the schema."""

import logging

from mongoengine import ConnectionFailure, Document, StringField, connect, disconnect

DEFAULT_HOST = "mongodb://mongodb"

DEFAULT_DB_NAME = "krakendb"

DEFAULT_DB_PORT = 27017


def connect_db() -> None:
    """Connect to the database."""
    try:
        disconnect()
        # TODO: take this from environment
        host = f"{DEFAULT_HOST}:{DEFAULT_DB_PORT}/{DEFAULT_DB_NAME}"
        logging.info("Connecting to database host {host}")
        connect(host=host)
    except ConnectionFailure:
        pass
        # A different connection with alias `default` was already registered.


class RawFile(Document):
    """Schema for a raw file."""

    name = StringField(required=True, primary_key=True)
    status = StringField(max_length=50)
