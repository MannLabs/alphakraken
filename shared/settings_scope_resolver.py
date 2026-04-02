"""Scope resolution for project-settings assignments."""

import logging
from enum import IntEnum

from shared.db.models import ProjectSettings, Settings
from shared.keys import DEFAULT_SCOPE, KNOWN_VENDOR_NAMES


class ScopeLevel(IntEnum):
    """Enumerating the levels of settings scoping."""

    DEFAULT = 0
    VENDOR = 1
    INSTRUMENT = 2


def resolve_scoped_settings(
    project_settings: list[ProjectSettings],
    *,
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

    When raw_file_id is None, entries with a raw_file_id_filter are not excluded, they just lose filter priority.
    This is fine as raw_file_id is only None for the webapp preview.

    Settings with status "inactive" are deliberately not excluded here.
    """
    scored_settings: list[tuple[tuple[ScopeLevel | None, int], Settings]] = []
    for ps in project_settings:
        settings_name = ps.settings.name

        if instrument_id in (ps.excluded or []):
            logging.info(
                f"  [{settings_name}] scope={ps.scope!r} -> SKIPPED (instrument {instrument_id!r} is excluded)"
            )
            continue

        if ps.raw_file_id_filter and raw_file_id is not None:
            if ps.raw_file_id_filter not in raw_file_id:
                logging.info(
                    f"  [{settings_name}] scope={ps.scope!r} -> SKIPPED "
                    f"(raw_file_id_filter={ps.raw_file_id_filter!r} not in {raw_file_id!r})"
                )
                continue
            filter_match_len = len(ps.raw_file_id_filter)
        else:
            filter_match_len = 0

        level = _classify_scope(ps.scope, instrument_id, instrument_type)
        if level is None:
            logging.info(
                f"  [{settings_name}] scope={ps.scope!r} -> SKIPPED (scope does not match)"
            )
            continue

        logging.info(
            f"  [{settings_name}] scope={ps.scope!r} -> SCORED "
            f"level={level.name}, {filter_match_len=}, software_type={ps.settings.software_type!r}"
        )
        scored_settings.append(((level, filter_match_len), ps.settings))

    scored_settings.sort(key=lambda x: x[0], reverse=True)

    best_scoring_settings_per_software_type: dict[str, Settings] = {}
    for (level, filter_match_len), settings in scored_settings:
        if settings.software_type not in best_scoring_settings_per_software_type:
            best_scoring_settings_per_software_type[settings.software_type] = settings
            logging.info(
                f"  Winner for {settings.software_type!r}: {settings.name!r} "
                f"(scope_level={level.name}, {filter_match_len=})"
            )
        else:
            logging.info(
                f"  [{settings.name}] software_type={settings.software_type!r} -> OVERRIDDEN "
                f"by {best_scoring_settings_per_software_type[settings.software_type].name!r}"
            )

    logging.info(f"Resolved {len(best_scoring_settings_per_software_type)} settings")
    return list(best_scoring_settings_per_software_type.values())


def _classify_scope(
    scope: str, instrument_id: str, instrument_type: str
) -> ScopeLevel | None:
    """Classify a scope string into a level, or None if it doesn't match."""
    if scope == DEFAULT_SCOPE:
        return ScopeLevel.DEFAULT

    if scope == instrument_id:
        return ScopeLevel.INSTRUMENT

    if scope in KNOWN_VENDOR_NAMES and scope == instrument_type:
        return ScopeLevel.VENDOR

    return None
