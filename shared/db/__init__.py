"""The one API for reading and writing the database, independent of the storage provider.

More than persistence: the write functions of users, projects, settings, their assignments and
alert suppressions enforce the rules of the data, i.e. input validation, status changes, the
protection of the fallback project and duplicate detection. The other write functions (raw files,
metrics, status, alert issues and leases, Slurm jobs) are for the pipeline and the monitoring only:
they check little more than the field limits, so a client must not call them. Some rules read
alphakraken.yaml (known scopes, runners): it is loaded when `shared.db.settings` or
`shared.db.project_settings` is imported, so every process using them needs that file, also
for reading.
"""

# The write functions take explicit keyword parameters, so an unknown key is a TypeError, not a
# InvalidInputError. A REST route or MCP tool calling them must:
# 1. declare the same parameters and pass them one by one, never a request body as `**payload`,
# 2. reject unknown keys: FastAPI and FastMCP likely drop them silently by default, so a
#    misspelled optional key would fall back to its default (e.g. `scope` -> all instruments);
#    REST: a body model with `extra="forbid"` or the domain input type; MCP: check the received
#    keys if FastMCP cannot forbid extra arguments,
# 3. have a test sending a misspelled key that expects a validation error,
# 4. answer the NotFoundError of a creating call (`projects.add`, `settings.create`,
#    `project_settings.assign`) like a validation error, not with "not found": there it means
#    that the request refers to an owner, project or settings that do not exist.

from shared.db._mongo import connection as _connection


def ping() -> None:
    """Check that the database is reachable.

    Raises:
        DatabaseUnavailableError: if it is not.

    """
    _connection.ping()
