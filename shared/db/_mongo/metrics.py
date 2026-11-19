"""Metrics queries on MongoDB."""

import inspect
from datetime import datetime
from typing import Any

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import as_utc
from shared.domain.errors import InvalidInputError
from shared.domain.metrics import Metrics

_METADATA_FIELDS = (
    "raw_file",
    "type",
    "settings_name",
    "settings_version",
    "relative_output_path",
    "output_display_base_path",
    "created_at_",
)
_ABSENT = object()


@db_backend_function
def find(
    *,
    raw_file_ids: list[str] | None,
    created_after: datetime | None,
    fields: list[str] | None,
) -> list[Metrics]:
    """Return the metrics matching the conditions, newest first, cf. `shared.db.metrics`."""
    conditions: dict[str, Any] = {}
    if raw_file_ids is not None:
        conditions["raw_file__in"] = raw_file_ids
    if created_after is not None:
        conditions["created_at___gte"] = created_after

    query = models.Metrics.objects(**conditions).order_by("-created_at_")
    if fields is not None:
        # mongoengine fills fields left out of a projection with their defaults instead of None,
        # so the metadata must always be loaded: a missing `created_at_` would read as "now"
        query = query.only(*_METADATA_FIELDS, *fields)
    return [_to_metrics(doc) for doc in query]


@db_backend_function
def add(  # noqa: PLR0913
    raw_file_id: str,
    *,
    metrics_type: str,
    values: dict[str, Any],
    settings_name: str,
    settings_version: int,
    relative_output_path: str,
    output_display_base_path: str,
) -> None:
    """Add metrics for a raw file, cf. `shared.db.metrics`."""
    if errors := _value_name_errors(values):
        raise InvalidInputError(errors)
    raw_file = models.RawFile.objects.get(id=raw_file_id)

    models.Metrics(
        raw_file=raw_file,
        type=metrics_type,
        settings_name=settings_name,
        settings_version=settings_version,
        relative_output_path=relative_output_path,
        output_display_base_path=output_display_base_path,
        **values,
    ).save(force_insert=True)


def _value_name_errors(values: dict[str, Any]) -> list[str]:
    """Return why names of the metric values cannot be stored."""
    errors = []
    for name in values:
        # mongoengine does not store them
        if name.startswith("_"):
            errors.append(f"values: '{name}' must not start with '_'.")
        # a field, e.g. `type`, or a method, e.g. `save`, of the document would be replaced;
        # `getattr_static`, as `objects` would connect to the database
        elif inspect.getattr_static(models.Metrics, name, _ABSENT) is not _ABSENT:
            errors.append(f"values: '{name}' is a field or method of a metrics entry.")
        # MongoDB reads them as a path or an operator
        elif "." in name or "$" in name:
            errors.append(f"values: '{name}' must not contain '.' or '$'.")
    return errors


def _to_metrics(doc: models.Metrics) -> Metrics:
    data = dict(doc.to_mongo())
    data.pop("_id", None)
    return Metrics(
        raw_file_id=data.pop("raw_file"),
        type=data.pop("type"),
        settings_name=data.pop("settings_name"),
        settings_version=data.pop("settings_version"),
        relative_output_path=data.pop("relative_output_path", None),
        output_display_base_path=data.pop("output_display_base_path", None),
        created_at=as_utc(data.pop("created_at_")),
        values=data,
    )
