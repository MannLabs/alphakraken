"""Scope resolution for project-settings assignments."""

# TODO: move

from collections import defaultdict

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
    Per software_type, only the highest-level entries are kept.
    At the same level, entries with a matching raw_file_id_filter beat
    unfiltered entries. At the same priority, all entries are returned.

    If raw_file_id is provided, entries with non-empty raw_file_id_filter
    are only included if the raw_file_id contains at least one of the strings.
    """
    classified: list[tuple[tuple[int, bool], ProjectSettings]] = []
    for ps in project_settings:
        if instrument_id in (ps.excluded or []):
            continue
        has_matching_filter = False
        if (ps.raw_file_id_filter or []) and raw_file_id is not None:
            if not any(s in raw_file_id for s in ps.raw_file_id_filter):
                continue
            has_matching_filter = True
        level = _classify_scope(ps.scope, instrument_id, instrument_type)
        if level is not None:
            classified.append(((level, has_matching_filter), ps))

    groups: dict[str, list[tuple[tuple[int, bool], Settings]]] = defaultdict(list)
    for priority, ps in classified:
        groups[ps.settings.software_type].append((priority, ps.settings))

    result: list[Settings] = []
    for entries in groups.values():
        max_priority = max(priority for priority, _ in entries)
        result.extend(
            settings for priority, settings in entries if priority == max_priority
        )

    return result


def _classify_scope(scope: str, instrument_id: str, instrument_type: str) -> int | None:
    """Classify a scope string into a level, or None if it doesn't match."""
    if scope == DEFAULT_SCOPE:
        return SCOPE_LEVEL_DEFAULT

    if scope == instrument_id:
        return SCOPE_LEVEL_INSTRUMENT

    if scope in KNOWN_VENDOR_NAMES and scope == instrument_type:
        return SCOPE_LEVEL_VENDOR

    return None
