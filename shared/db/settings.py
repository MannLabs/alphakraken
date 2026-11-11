"""Settings queries."""

from shared.db._mongo import settings as _backend
from shared.domain.settings import Settings


def find(
    *,
    name: str | None = None,
    include_deleted: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[Settings]:
    """Return the settings, newest first.

    :param name: only the versions of the settings with this name
    """
    return _backend.find(
        name=name, include_deleted=include_deleted, limit=limit, offset=offset
    )


def count(*, name: str | None = None, include_deleted: bool = False) -> int:
    """Return the number of settings that `find` returns without limit and offset."""
    return _backend.count(name=name, include_deleted=include_deleted)


def get(unique_name: str) -> Settings:
    """Return the settings with the given unique name, e.g. `fallback_v7`, whatever their status.

    Raises:
        NotFoundError: if there are none, also if `unique_name` is not of that form.

    """
    return _backend.get(unique_name)
