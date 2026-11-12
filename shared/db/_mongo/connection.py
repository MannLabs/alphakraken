"""Connection handling of the Mongo backend."""

import logging
import os
from functools import cache

from mongoengine import (
    ConnectionFailure,
    connect,
)
from mongoengine.connection import get_db

from shared.db._mongo.errors import translate_errors
from shared.keys import EnvVars

DB_NAME: str = "krakendb"

# nonsensical default values are used by tests only
DB_HOST = os.environ.get(EnvVars.MONGO_HOST, "some_host")
DB_PORT = int(os.environ.get(EnvVars.MONGO_PORT, 12345))
DB_USER = os.environ.get(EnvVars.MONGO_USER, "pika")
DB_PASSWORD = os.environ.get(EnvVars.MONGO_PASSWORD, "chu")


def connect_db(
    *, connection_data: dict | None = None, raise_on_error: bool = False
) -> None:
    """Connect to the database."""
    if connection_data is None:
        connection_data = {}

    try:
        # TODO: think about putting DB connection to an Airflow connection
        db_name = connection_data.get("DB_NAME", DB_NAME)
        host = connection_data.get("DB_HOST", DB_HOST)
        port = connection_data.get("DB_PORT", DB_PORT)
        username = connection_data.get("DB_USER", DB_USER)
        password = connection_data.get("DB_PASSWORD", DB_PASSWORD)

        logging.debug(f"Connecting to db: {db_name=} {host=} {port=} {username=}")

        connect(
            db_name,
            host=host,
            port=port,
            username=username,
            password=password,
            authentication_source=db_name,
        )
    except ConnectionFailure as e:
        if raise_on_error:
            raise e  # noqa: TRY201
        logging.info(f"DB connection: {DB_HOST=} {DB_PORT=} {DB_USER=}")
        logging.warning(f"Failed to connect to db: {e}")
        # A different connection with alias `default` was already registered.


@cache
def ensure_connected() -> None:
    """Register the connection once per process.

    The call is cached only when it succeeds, so a failed registration is retried on the
    next call. Reconnecting after a dropped connection is the pymongo client's job.
    """
    connect_db(raise_on_error=True)


@translate_errors
def ping() -> None:
    """Check that the database is reachable, cf. `shared.db.ping`."""
    ensure_connected()
    get_db().command("ping")
