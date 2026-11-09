"""Metrics queries on MongoDB."""

from typing import Any

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import as_utc
from shared.domain.metrics import Metrics

# stored key -> domain key, for metrics written by older calculator versions
LEGACY_KEYS = {
    "raw:gradient_length_m": "gradient_length",  # alphadia < 2
    "msqc_evosep_pump_hp_pressure_max": "evosep_pump_hp_pressure_max",  # older alphakraken
}

_METADATA_FIELDS = (
    "raw_file",
    "type",
    "settings_name",
    "settings_version",
    "relative_output_path",
    "output_display_base_path",
    "created_at_",
)


@translate_errors
def latest_by_raw_file(
    raw_file_ids: list[str], *, fields: list[str] | None
) -> dict[str, dict[str, Metrics]]:
    """Return the newest metrics per type of each raw file, cf. `shared.db.metrics`."""
    ensure_connected()
    query = models.Metrics.objects(raw_file__in=raw_file_ids).order_by("-created_at_")
    if fields is not None:
        query = query.only(*_METADATA_FIELDS, *_stored_keys(fields))

    result: dict[str, dict[str, Metrics]] = {}
    for doc in query:
        metrics = _to_metrics(doc)
        result.setdefault(metrics.raw_file_id, {}).setdefault(metrics.type, metrics)
    return result


def _stored_keys(fields: list[str]) -> list[str]:
    """Return the fields plus the legacy keys they were stored under."""
    return [*fields, *(k for k, domain in LEGACY_KEYS.items() if domain in fields)]


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
        values=_harmonized(data),
    )


def _harmonized(values: dict[str, Any]) -> dict[str, Any]:
    """Rename legacy keys; the current key wins if a document holds both."""
    result = {k: v for k, v in values.items() if k not in LEGACY_KEYS}
    for stored, domain in LEGACY_KEYS.items():
        if stored in values:
            result.setdefault(domain, values[stored])
    return result
