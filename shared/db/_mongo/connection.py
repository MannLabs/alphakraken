"""Connection handling of the Mongo backend."""

from functools import cache

from shared.db.engine import connect_db


@cache
def ensure_connected() -> None:
    """Register the connection once per process.

    The call is cached only when it succeeds, so a failed registration is retried on the
    next call. Reconnecting after a dropped connection is the pymongo client's job.
    """
    connect_db(raise_on_error=True)
