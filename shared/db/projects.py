"""Project queries."""

from shared.db._mongo import projects as _backend
from shared.domain.projects import Project


def find(
    *,
    ids: list[str] | None = None,
    include_deleted: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[Project]:
    """Return the projects, newest first.

    :param ids: only the projects with these ids
    """
    return _backend.find(
        ids=ids, include_deleted=include_deleted, limit=limit, offset=offset
    )


def count(*, ids: list[str] | None = None, include_deleted: bool = False) -> int:
    """Return the number of projects that `find` returns without limit and offset."""
    return _backend.count(ids=ids, include_deleted=include_deleted)
