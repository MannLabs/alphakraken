"""Raw file queries on MongoDB."""

from __future__ import annotations

import logging
import re
from dataclasses import fields
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import to_raw_file
from shared.domain.errors import NotFoundError

if TYPE_CHECKING:
    from shared.db.raw_files import RawFileFilter
    from shared.domain.raw_files import RawFile

# RawFileFilter field -> mongoengine query key, for every field of the filter
_QUERY_KEYS = {
    "ids": "id__in",
    "original_names": "original_name__in",
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
    "instrument_file_statuses_not": "instrument_file_status__nin",
}


def _exact_ignoring_case(names: list[str]) -> list[re.Pattern[str]]:
    return [re.compile(f"^{re.escape(name)}$", re.IGNORECASE) for name in names]


def _identity(value: Any) -> Any:
    return value


# filter fields whose value is not passed on as it is
_VALUE_TRANSFORMS = {"original_names": _exact_ignoring_case}


def _conditions(where: RawFileFilter) -> dict[str, Any]:
    """Translate the filter into query conditions, leaving out the unset fields.

    Raises:
        KeyError: if a set field has no query key, rather than silently ignoring it.

    """
    return {
        _QUERY_KEYS[field.name]: _VALUE_TRANSFORMS.get(field.name, _identity)(value)
        for field in fields(where)
        if (value := getattr(where, field.name)) is not None
    }


@db_backend_function
def find(
    where: RawFileFilter,
    *,
    order: str,
    limit: int | None,
    offset: int,
    include_file_info: bool,
) -> list[RawFile]:
    """Return the raw files matching `where`, cf. `shared.db.raw_files.find`."""
    query = models.RawFile.objects(**_conditions(where)).order_by(order)
    if not include_file_info:
        query = query.exclude("file_info")
    if offset:
        query = query.skip(offset)
    if limit is not None:
        query = query.limit(limit)
    return [to_raw_file(doc, file_info_loaded=include_file_info) for doc in query]


@db_backend_function
def count(where: RawFileFilter) -> int:
    """Return the number of raw files matching `where`, cf. `shared.db.raw_files.count`."""
    return models.RawFile.objects(**_conditions(where)).count()


@db_backend_function
def latest_per_instrument(
    n: int, *, created_after: datetime
) -> dict[str, list[RawFile]]:
    """Return the newest raw files per instrument, cf. `shared.db.raw_files.latest_per_instrument`."""
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


@db_backend_function
def count_by_status(where: RawFileFilter) -> dict[str, int]:
    """Return the number of raw files per status, cf. `shared.db.raw_files.count_by_status`."""
    rows = models.RawFile.objects(**_conditions(where)).aggregate(
        [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    )
    return {row["_id"]: row["count"] for row in rows}


@db_backend_function
def get(raw_file_id: str, *, include_file_info: bool) -> RawFile:
    """Return the raw file with the given id, cf. `shared.db.raw_files.get`."""
    query = models.RawFile.objects(id=raw_file_id)
    if not include_file_info:
        query = query.exclude("file_info")
    doc = query.first()
    if doc is None:
        raise NotFoundError(f"No raw file {raw_file_id}")
    return to_raw_file(doc, file_info_loaded=include_file_info)


@db_backend_function
def add(raw_file_id: str, **fields_: Any) -> None:
    """Insert a new raw file, cf. `shared.db.raw_files.add`."""
    # this will fail if the file already exists
    models.RawFile(id=raw_file_id, **fields_).save(force_insert=True)


@db_backend_function
def delete(raw_file_id: str) -> None:
    """Remove a raw file, cf. `shared.db.raw_files.delete`."""
    models.RawFile.objects(id=raw_file_id).delete()


@db_backend_function
def update(raw_file_id: str, fields_: dict[str, Any]) -> None:
    """Set the given fields of a raw file, cf. `shared.db.raw_files.update`."""
    doc = models.RawFile.objects.with_id(raw_file_id)
    if doc is None:
        raise NotFoundError(f"No raw file {raw_file_id}")
    logging.info(f"Old DB state: {doc.status=} {doc.status_details=}")

    doc.update(updated_at_=datetime.now(tz=UTC), **fields_)
