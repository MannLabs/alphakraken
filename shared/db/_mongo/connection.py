"""Connection handling of the Mongo backend."""

from functools import cache

from mongoengine.connection import get_db

from shared.db._mongo.errors import translate_errors
from shared.db.engine import connect_db


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
