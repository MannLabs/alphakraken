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


class UserTypes:
    """Types of user credentials for database connection, see init-mongo.sh for their privileges."""

    READONLY: str = "readonly"
    WEBAPP: str = "webapp"
    READWRITE: str = "readwrite"


def connect_db(
    user_type: str = UserTypes.READONLY, *, raise_on_error: bool = False
) -> None:
    """Connect to the database.

    :param user_type: Type of user credentials to use for the connection.
        Options are "readonly", "webapp", or "default".
    :param raise_on_error: If True, raise an exception on connection failure.
        If False, log the error and continue.
    """
    # Select user credentials based on user_type

    user_env, password_env = {
        "readonly": (EnvVars.MONGO_USER_READ, EnvVars.MONGO_PASSWORD_READ),
        "webapp": (EnvVars.MONGO_USER_WEBAPP, EnvVars.MONGO_PASSWORD_WEBAPP),
        "default": (EnvVars.MONGO_USER, EnvVars.MONGO_PASSWORD),
    }[user_type]

    username = os.environ.get(user_env, "pika")
    password = os.environ.get(password_env, "chu")

    try:
        # TODO: think about putting DB connection to an Airflow connection
        logging.info(f"Connecting to db: {DB_HOST=} {DB_NAME=} {DB_PORT=} {username=}")

        connect(
            DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
            username=username,
            password=password,
            authentication_source=DB_NAME,
        )
    except ConnectionFailure as e:
        if raise_on_error:
            raise e  # noqa: TRY201
        logging.info(f"DB connection: {DB_HOST=} {DB_PORT=} {username=}")
        logging.warning(f"Failed to connect to db: {e}")
        # A different connection with alias `default` was already registered.
