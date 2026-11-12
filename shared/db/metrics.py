"""Metrics queries."""

import logging
from datetime import datetime
from typing import Any

from shared.db._mongo import metrics as _backend
from shared.domain.metrics import Metrics


def find(
    raw_file_ids: list[str] | None = None,
    *,
    created_after: datetime | None = None,
    fields: list[str] | None = None,
) -> list[Metrics]:
    """Return all metrics of the given raw files or written at or after `created_after`, newest first.

    A raw file can have several metrics of one type, e.g. from different settings.

    :param fields: the keys of `values` to load, None for all

    Raises:
        ValueError: if neither `raw_file_ids` nor `created_after` is given.

    """
    if raw_file_ids is None and created_after is None:
        raise ValueError("Either raw_file_ids or created_after must be given.")
    return _backend.find(
        raw_file_ids=raw_file_ids, created_after=created_after, fields=fields
    )


def latest_by_raw_file(
    raw_file_ids: list[str] | None = None,
    *,
    created_after: datetime | None = None,
    fields: list[str] | None = None,
) -> dict[str, dict[str, Metrics]]:
    """Return the newest metrics per type of each raw file, keyed by raw file id, then by type.

    Raw files without metrics have no entry. The parameters are those of `find`.
    """
    result: dict[str, dict[str, Metrics]] = {}
    for metrics in find(raw_file_ids, created_after=created_after, fields=fields):
        per_type = result.setdefault(metrics.raw_file_id, {})
        # newest first, so the first one per type is the latest
        if metrics.type not in per_type:
            per_type[metrics.type] = metrics
    return result


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
    """Add metrics of one type for a raw file.

    Raises:
        NotFoundError: if the raw file does not exist.

    """
    logging.info(
        f"Adding to DB: {raw_file_id=} <- {values=} type={metrics_type} {settings_name=} {settings_version=} {relative_output_path=} {output_display_base_path=}"
    )
    _backend.add(
        raw_file_id,
        metrics_type=metrics_type,
        values=values,
        settings_name=settings_name,
        settings_version=settings_version,
        relative_output_path=relative_output_path,
        output_display_base_path=output_display_base_path,
    )
