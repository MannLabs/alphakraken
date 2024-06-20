"""Module to handle the database connection.

Note: this module must not have any dependencies on the rest of the codebase.
"""

import logging
import os

from mongoengine import (
    ConnectionFailure,
    connect,
)

from shared.keys import EnvVars

DB_NAME: str = "krakendb"

# nonsensical default values are used by tests only
DB_HOST = os.environ.get(EnvVars.MONGO_HOST, "some_host")
DB_PORT = int(os.environ.get(EnvVars.MONGO_PORT, 12345))
DB_USER = os.environ.get(EnvVars.MONGO_USER, "pika")
DB_PASSWORD = os.environ.get(EnvVars.MONGO_PASSWORD, "chu")

logging.info(f"DB connection: {DB_HOST=} {DB_PORT=} {DB_USER=}")


def connect_db() -> None:
    """Connect to the database."""
    try:
        # seems like this is not necessary:
        # disconnect()
        # TODO: think about putting DB connection to an Airflow connection
        logging.info(f"Connecting to db: {DB_HOST=} {DB_NAME=} {DB_PORT=} {DB_USER=}")

        connect(
            DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
            username=DB_USER,
            password=DB_PASSWORD,
            authentication_source=DB_NAME,
        )
    except ConnectionFailure:
        pass
        # A different connection with alias `default` was already registered.
