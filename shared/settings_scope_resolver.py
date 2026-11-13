"""Scope resolution for project-settings assignments."""

import logging

from shared.domain.projects import ProjectSettings
from shared.domain.settings import Settings
from shared.keys import DEFAULT_SCOPE, KNOWN_VENDOR_NAMES
from shared.yamlsettings import YAMLSETTINGS, YamlKeys


def known_scopes() -> list[str]:
    """Return the scopes an assignment can have: the default scope, the vendors and the instruments of alphakraken.yaml."""
    return [
        DEFAULT_SCOPE,
        *KNOWN_VENDOR_NAMES,
        *YAMLSETTINGS.get(YamlKeys.INSTRUMENTS, {}),
    ]


def fully_excluded_scopes(scopes: list[str], excluded_scopes: list[str]) -> list[str]:
    """Return the scopes whose instruments in alphakraken.yaml are all excluded."""
    instruments = [
        (instrument_id, config.get(YamlKeys.TYPE))
        for instrument_id, config in YAMLSETTINGS.get(YamlKeys.INSTRUMENTS, {}).items()
    ]
    result = []
    for scope in scopes:
        matching = [i for i in instruments if _scopes_match([scope], *i)]
        if matching and all(_scopes_match(excluded_scopes, *i) for i in matching):
            result.append(scope)
    return result


def resolve_scoped_settings(
    project_settings: list[ProjectSettings],
    *,
    instrument_id: str,
    instrument_type: str,
    raw_file_id: str | None = None,
) -> list[Settings]:
    """Filter project-settings down to those applying to the given instrument and raw file.

    An assignment applies if any of its `scopes` matches the instrument, none of its
    `excluded_scopes` matches, the raw file ID contains any entry of `raw_file_id_filter`
    (empty = all files) and none of `raw_file_id_exclude_filter` (case-insensitive). Exclusion beats inclusion.

    When raw_file_id is None (webapp preview), both file-name filters are ignored.

    Settings with status "inactive" are deliberately not excluded here.
    """
    result: list[Settings] = []
    for ps in project_settings:
        prefix = f"  [{ps.settings.name}] scopes={list(ps.scopes)!r}"

        if not _scopes_match(ps.scopes, instrument_id, instrument_type):
            logging.info(f"{prefix} -> SKIPPED (no scope matches)")
            continue

        if _scopes_match(ps.excluded_scopes, instrument_id, instrument_type):
            logging.info(
                f"{prefix} -> SKIPPED (excluded by {list(ps.excluded_scopes)!r})"
            )
            continue

        if raw_file_id is not None:
            if not _name_included(ps.raw_file_id_filter, raw_file_id):
                logging.info(
                    f"{prefix} -> SKIPPED (raw_file_id_filter={list(ps.raw_file_id_filter)!r} "
                    f"not in {raw_file_id!r})"
                )
                continue

            if _name_excluded(ps.raw_file_id_exclude_filter, raw_file_id):
                logging.info(
                    f"{prefix} -> SKIPPED (raw_file_id_exclude_filter="
                    f"{list(ps.raw_file_id_exclude_filter)!r} in {raw_file_id!r})"
                )
                continue

        logging.info(
            f"{prefix} -> APPLIES (software_type={ps.settings.software_type!r})"
        )
        result.append(ps.settings)

    unique = _unique_by_id(result)
    logging.info(f"Resolved {len(unique)} settings")
    return unique


def _scopes_match(scopes: list[str], instrument_id: str, instrument_type: str) -> bool:
    """Return True if any scope matches: '*' matches all, a vendor name its instruments, else the instrument ID."""
    for scope in scopes or []:
        if scope == DEFAULT_SCOPE:
            return True
        if scope in KNOWN_VENDOR_NAMES:
            if scope == instrument_type:
                return True
        elif scope == instrument_id:
            return True
    return False


def _name_included(filters: list[str], raw_file_id: str) -> bool:
    """Return True if no filter is given or the raw file ID contains any of them."""
    return not filters or any(f in raw_file_id for f in filters)


def _name_excluded(filters: list[str], raw_file_id: str) -> bool:
    """Return True if the raw file ID contains any of the filters, case-insensitive."""
    return any(f.lower() in raw_file_id.lower() for f in filters or [])


def _unique_by_id(settings_list: list[Settings]) -> list[Settings]:
    """Drop repeated settings, keeping first occurrence order."""
    seen: set = set()
    unique: list[Settings] = []
    for s in settings_list:
        if s.id in seen:
            logging.info(f"  [{s.name}] -> DROPPED (already resolved)")
            continue
        seen.add(s.id)
        unique.append(s)
    return unique
