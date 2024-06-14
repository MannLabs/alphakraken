"""Module to handle the database connection.

Note: this module must not have any dependencies on the rest of the codebase.
"""

import logging
import os

from mongoengine import (
    ConnectionFailure,
    connect,
    disconnect,
)

DB_NAME: str = "krakendb"

DOCKER_DB_HOST = "mongodb-service"
DB_HOST = os.environ.get(
    "MONGO_HOST", DOCKER_DB_HOST
)  # if mongodb does not run in Docker: use localhost

# nonsensical default values are used by tests only
DB_PORT = int(os.environ.get("MONGO_PORT", 12345))
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
