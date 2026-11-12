"""Raw file queries on MongoDB."""

from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_raw_file

if TYPE_CHECKING:
    from datetime import datetime

    from shared.db.raw_files import RawFileFilter
    from shared.domain.raw_files import RawFile

# RawFileFilter field -> mongoengine query key, for every field of the filter
_QUERY_KEYS = {
    "ids": "id__in",
    "instrument_id": "instrument_id",
    "instrument_ids": "instrument_id__in",
    "project_id": "project_id",
    "name_contains": "id__icontains",
    "created_after": "created_at__gte",
    "created_before": "created_at__lte",
    "updated_after": "updated_at___gte",
    "status": "status",
    "statuses": "status__in",
    "statuses_not": "status__nin",
    "backup_status": "backup_status",
    "instrument_file_status": "instrument_file_status",
}


def _conditions(where: RawFileFilter) -> dict[str, Any]:
    """Translate the filter into query conditions, leaving out the unset fields.

    Raises:
        KeyError: if a set field has no query key, rather than silently ignoring it.

    """
    return {
        _QUERY_KEYS[field.name]: value
        for field in fields(where)
        if (value := getattr(where, field.name)) is not None
    }


@translate_errors
def find(
    where: RawFileFilter,
    *,
    order: str,
    limit: int | None,
    offset: int,
    include_file_info: bool,
) -> list[RawFile]:
    """Return the raw files matching `where`, cf. `shared.db.raw_files.find`."""
    ensure_connected()
    query = models.RawFile.objects(**_conditions(where)).order_by(order)
    if not include_file_info:
        query = query.exclude("file_info")
    if offset:
        query = query.skip(offset)
    if limit is not None:
        query = query.limit(limit)
    return [to_raw_file(doc, file_info_loaded=include_file_info) for doc in query]


@translate_errors
def count(where: RawFileFilter) -> int:
    """Return the number of raw files matching `where`, cf. `shared.db.raw_files.count`."""
    ensure_connected()
    return models.RawFile.objects(**_conditions(where)).count()


@translate_errors
def latest_per_instrument(
    n: int, *, created_after: datetime
) -> dict[str, list[RawFile]]:
    """Return the newest raw files per instrument, cf. `shared.db.raw_files.latest_per_instrument`."""
    ensure_connected()
    recent = models.RawFile.objects(created_at__gte=created_after)
    return {
        instrument_id: [
            to_raw_file(doc, file_info_loaded=False)
            for doc in recent.filter(instrument_id=instrument_id)
            .order_by("-created_at")
            .exclude("file_info")
            .limit(n)
        ]
        for instrument_id in recent.distinct("instrument_id")
    }


@translate_errors
def count_by_status(where: RawFileFilter) -> dict[str, int]:
    """Return the number of raw files per status, cf. `shared.db.raw_files.count_by_status`."""
    ensure_connected()
    rows = models.RawFile.objects(**_conditions(where)).aggregate(
        [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    )
    return {row["_id"]: row["count"] for row in rows}
