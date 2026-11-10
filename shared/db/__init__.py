"""Database access, independent of the storage provider."""

from shared.db._mongo import connection as _connection


def ping() -> None:
    """Check that the database is reachable.

    Raises:
        DatabaseUnavailableError: if it is not.

    """
    _connection.ping()
