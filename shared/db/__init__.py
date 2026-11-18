"""The one API for reading and writing the database, independent of the storage provider.

More than persistence: the write functions enforce the rules of the data, i.e. input validation,
status changes, the protection of the fallback project and duplicate detection. Some rules read
alphakraken.yaml (known scopes, runners), so every process that writes needs that file.
"""

# The write functions take explicit keyword parameters, so an unknown key is a TypeError, not a
# ValidationError. A REST route or MCP tool calling them must:
# 1. declare the same parameters and pass them one by one, never a request body as `**payload`,
# 2. reject unknown keys: FastAPI and FastMCP likely drop them silently by default, so a
#    misspelled optional key would fall back to its default (e.g. `scope` -> all instruments);
#    REST: a body model with `extra="forbid"` or the domain input type; MCP: check the received
#    keys if FastMCP cannot forbid extra arguments,
# 3. have a test sending a misspelled key that expects a validation error.

from shared.db._mongo import connection as _connection


def ping() -> None:
    """Check that the database is reachable.

    Raises:
        DatabaseUnavailableError: if it is not.

    """
    _connection.ping()
