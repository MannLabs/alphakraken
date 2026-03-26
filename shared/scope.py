"""Scope resolution for project-settings assignments."""

# TODO: move

from shared.db.models import ProjectSettings, Settings
from shared.keys import DEFAULT_SCOPE, KNOWN_VENDOR_NAMES

SCOPE_LEVEL_DEFAULT = 0
SCOPE_LEVEL_VENDOR = 1
SCOPE_LEVEL_INSTRUMENT = 2


def resolve_scoped_settings(
    project_settings: list[ProjectSettings],
    instrument_id: str,
    instrument_type: str,
    raw_file_id: str | None = None,
) -> list[Settings]:
    """Filter project-settings by scope, keeping most-specific per software_type.

    Scope levels: "*" (default) < vendor name < instrument ID.
    Per software_type, only the highest-level entry is kept.
    At the same level, entries with a matching raw_file_id_filter beat
    unfiltered entries; among filtered entries the longest match wins.

    If raw_file_id is provided, entries with non-empty raw_file_id_filter
    are only included if the raw_file_id contains the filter string.
    """
    scored_settings: list[tuple[tuple[int, int], Settings]] = []
    for ps in project_settings:
        if instrument_id in (ps.excluded or []):
            continue

        if ps.raw_file_id_filter and raw_file_id is not None:
            if ps.raw_file_id_filter not in raw_file_id:
                continue
            filter_match_len = len(ps.raw_file_id_filter)
        else:
            filter_match_len = 0

        level = _classify_scope(ps.scope, instrument_id, instrument_type)
        if level is not None:
            scored_settings.append(((level, filter_match_len), ps.settings))

    scored_settings.sort(key=lambda x: x[0], reverse=True)

    best_scoring_settings_per_software_type: dict[str, Settings] = {}
    for _, settings in scored_settings:
        if settings.software_type not in best_scoring_settings_per_software_type:
            best_scoring_settings_per_software_type[settings.software_type] = settings
    return list(best_scoring_settings_per_software_type.values())


def _classify_scope(scope: str, instrument_id: str, instrument_type: str) -> int | None:
    """Classify a scope string into a level, or None if it doesn't match."""
    if scope == DEFAULT_SCOPE:
        return SCOPE_LEVEL_DEFAULT

    if scope == instrument_id:
        return SCOPE_LEVEL_INSTRUMENT

    if scope in KNOWN_VENDOR_NAMES and scope == instrument_type:
        return SCOPE_LEVEL_VENDOR

    return None
