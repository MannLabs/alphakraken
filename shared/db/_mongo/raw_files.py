"""Raw file queries on MongoDB."""

from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_raw_file

if TYPE_CHECKING:
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
    "backup_status": "backup_status",
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
