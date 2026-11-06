"""Raw file queries."""

from dataclasses import dataclass, fields
from datetime import datetime

from shared.db._mongo import raw_files as _backend
from shared.domain.raw_files import RawFile

DEFAULT_ORDER = "-created_at"

_ORDER_FIELDS = frozenset(field.name for field in fields(RawFile))


@dataclass(frozen=True)
class RawFileFilter:
    """Conditions a raw file must all meet; None means no condition.

    `*_after`: strictly later than.
    """

    instrument_ids: list[str] | None = None
    created_after: datetime | None = None
    updated_after: datetime | None = None
    statuses: list[str] | None = None
    backup_status: str | None = None


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
