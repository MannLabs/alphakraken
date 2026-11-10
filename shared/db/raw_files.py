"""Raw file queries."""

from dataclasses import dataclass, fields
from datetime import datetime

from shared.db._mongo import raw_files as _backend
from shared.domain.raw_files import RawFile

# "-" prefix for descending: widely understood ORM convention but strictly speaking a _backend detail
DEFAULT_ORDER = "-created_at"

_ORDER_FIELDS = frozenset(field.name for field in fields(RawFile))


@dataclass(frozen=True)
class RawFileFilter:
    """Conditions a raw file must all meet; None means no condition.

    `*_after` and `*_before` are inclusive bounds, `name_contains` is case-insensitive.
    """

    ids: list[str] | None = None
    instrument_id: str | None = None
    instrument_ids: list[str] | None = None
    project_id: str | None = None
    name_contains: str | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    updated_after: datetime | None = None
    status: str | None = None
    statuses: list[str] | None = None
    backup_status: str | None = None
    instrument_file_status: str | None = None


def find(
    where: RawFileFilter,
    *,
    order: str = DEFAULT_ORDER,
    limit: int | None = None,
    offset: int = 0,
    include_file_info: bool = False,
) -> list[RawFile]:
    """Return the raw files matching `where`.

    :param order: a field name of `RawFile`, prefixed with "-" for descending
    :param include_file_info: load `file_info`, which can be large; otherwise it is None
    """
    if order.removeprefix("-") not in _ORDER_FIELDS:
        raise ValueError(f"Unknown order field: {order}")
    return _backend.find(
        where,
        order=order,
        limit=limit,
        offset=offset,
        include_file_info=include_file_info,
    )


def count(where: RawFileFilter) -> int:
    """Return the number of raw files matching `where`."""
    return _backend.count(where)


def latest_per_instrument(
    n: int, *, created_after: datetime
) -> dict[str, list[RawFile]]:
    """Return the `n` newest raw files created at or after `created_after` per instrument, newest first.

    Instruments without such files have no entry. `file_info` is not loaded.
    """
    return _backend.latest_per_instrument(n, created_after=created_after)


def count_by_status(instrument_id: str, *, statuses_not: list[str]) -> dict[str, int]:
    """Return the number of raw files of an instrument per status, leaving out `statuses_not`."""
    return _backend.count_by_status(instrument_id, statuses_not=statuses_not)
