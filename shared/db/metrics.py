"""Metrics queries."""

from shared.db._mongo import metrics as _backend
from shared.domain.metrics import Metrics


def latest_by_raw_file(
    raw_file_ids: list[str], *, fields: list[str] | None = None
) -> dict[str, dict[str, Metrics]]:
    """Return the newest metrics per type of each raw file, keyed by raw file id, then by type.

    Raw files without metrics have no entry.

    :param fields: the keys of `values` to load, None for all
    """
    return _backend.latest_by_raw_file(raw_file_ids, fields=fields)
