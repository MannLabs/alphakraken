"""Migration: ProjectSettings scope fields become lists, resolution becomes a filter.

Before, exactly one settings per software_type applied to a raw file: the most specific scope
won ("*" < vendor < instrument). Now every matching assignment applies, and restriction is
explicit via `excluded_scopes` and `raw_file_id_exclude_filter`.

For each ProjectSettings document without `scopes`:
    scopes                     <- [scope]
    excluded_scopes            <- excluded (or [])
    raw_file_id_filter         <- [raw_file_id_filter] if non-empty else []
    raw_file_id_exclude_filter <- []
    scope, excluded            -> removed

`--dry-run` additionally prints a co-firing report: per project and instrument, the assignments
that were overridden before and will run in addition after the migration. Read it. Any entry
there doubles the quanting load for that project unless an exclusion is added. Instruments and
their vendors are read from alphakraken.yaml, so run it against the converted yaml.

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.10.0/_migrate_project_settings_scopes.py --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.10.0/_migrate_project_settings_scopes.py
"""

import argparse
import logging
from collections import defaultdict
from typing import Any

from shared.db.engine import connect_db
from shared.db.models import ProjectSettings, Settings
from shared.keys import DEFAULT_SCOPE, KNOWN_VENDOR_NAMES
from shared.yamlsettings import YAMLSETTINGS, YamlKeys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _is_legacy(doc: dict) -> bool:
    return "scopes" not in doc


def _validate(legacy_docs: list[dict]) -> None:
    """Raise if any legacy document has an unexpected shape, before anything is written."""
    for doc in legacy_docs:
        if not isinstance(doc.get("scope"), str):
            raise TypeError(
                f"ProjectSettings {doc['_id']}: `scope` missing or not a str"
            )
        if not isinstance(doc.get("excluded", []), list):
            raise TypeError(f"ProjectSettings {doc['_id']}: `excluded` is not a list")
        if not isinstance(doc.get("raw_file_id_filter", ""), str):
            raise TypeError(
                f"ProjectSettings {doc['_id']}: `raw_file_id_filter` is not a str"
            )


def _to_update(doc: dict) -> dict:
    """Build the update for one legacy document."""
    return {
        "$set": {
            "scopes": [doc["scope"]],
            "excluded_scopes": doc.get("excluded", []),
            "raw_file_id_filter": [f] if (f := doc.get("raw_file_id_filter")) else [],
            "raw_file_id_exclude_filter": [],
        },
        "$unset": {"scope": "", "excluded": ""},
    }


def _migrate_collection(collection: Any, *, dry_run: bool) -> list[dict]:
    """Rewrite the legacy documents of the ProjectSettings collection, returning them."""
    docs = list(collection.find().sort("created_at_", 1))
    legacy_docs = [doc for doc in docs if _is_legacy(doc)]
    _validate(legacy_docs)

    for doc in legacy_docs:
        update = _to_update(doc)
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}ProjectSettings {doc['_id']} "
            f"(project {doc.get('project')!r}): {update['$set']}"
        )
        if not dry_run:
            collection.update_one({"_id": doc["_id"]}, update)

    logger.info(
        f"Migration complete: {len(legacy_docs)} updated, {len(docs) - len(legacy_docs)} skipped."
    )
    return legacy_docs


# --- co-firing report: old override semantics vs. new filter semantics


def _scope_level(scope: str, instrument_id: str, instrument_type: str) -> int | None:
    """Pre-migration `_classify_scope`: None if no match, else the precedence level."""
    if scope == DEFAULT_SCOPE:
        return 0
    if scope == instrument_id:
        return 2
    if scope in KNOWN_VENDOR_NAMES and scope == instrument_type:
        return 1
    return None


def _matching(
    docs: list[dict], instrument_id: str, instrument_type: str
) -> list[tuple[int, dict]]:
    """Legacy docs matching the instrument, with their scope level."""
    result = []
    for doc in docs:
        if instrument_id in doc.get("excluded", []):
            continue
        level = _scope_level(doc["scope"], instrument_id, instrument_type)
        if level is not None:
            result.append((level, doc))
    return result


def _filter_of(doc: dict) -> str:
    return doc.get("raw_file_id_filter") or ""


def _old_winners(
    eligible: list[tuple[int, dict]], settings_by_id: dict[Any, dict]
) -> set[Any]:
    """IDs of the docs the pre-migration resolver kept for one raw file.

    Highest (scope level, filter length) per software_type, first wins on ties.
    """
    ranked = sorted(
        enumerate(eligible),
        key=lambda x: (x[1][0], len(_filter_of(x[1][1])), -x[0]),
        reverse=True,
    )
    winners: dict[str, Any] = {}
    for _, (_, doc) in ranked:
        software_type = settings_by_id[doc["settings"]]["software_type"]
        winners.setdefault(software_type, doc["_id"])
    return set(winners.values())


def _newly_firing(
    matching: list[tuple[int, dict]], settings_by_id: dict[Any, dict]
) -> list[str]:
    """Assignments overridden before but firing now, per class of raw files.

    A raw file matching filter F was eligible for the unfiltered docs and the docs with filter F;
    a file matching no filter only for the unfiltered ones. Filters are assumed not to overlap.
    """
    result = []
    for filter_value in sorted({_filter_of(doc) for _, doc in matching}):
        eligible = [
            (level, doc)
            for level, doc in matching
            if _filter_of(doc) in ("", filter_value)
        ]
        if len(eligible) < 2:  # noqa: PLR2004
            continue
        winners = _old_winners(eligible, settings_by_id)
        suffix = f" [files matching {filter_value!r}]" if filter_value else ""
        result.extend(
            _describe(doc, settings_by_id) + suffix
            for _, doc in eligible
            if doc["_id"] not in winners
        )
    return result


def build_cofiring_report(
    legacy_docs: list[dict],
    settings_by_id: dict[Any, dict],
    instruments: dict[str, str],
) -> dict[tuple[str, str], list[str]]:
    """Per (project, instrument), the assignments that were overridden before and will run now."""
    by_project: dict[str, list[dict]] = defaultdict(list)
    for doc in legacy_docs:
        by_project[doc["project"]].append(doc)

    report: dict[tuple[str, str], list[str]] = {}
    for project_id, docs in sorted(by_project.items()):
        for instrument_id, instrument_type in instruments.items():
            matching = _matching(docs, instrument_id, instrument_type)
            newly_firing = _newly_firing(matching, settings_by_id)
            if newly_firing:
                report[(project_id, instrument_id)] = newly_firing
    return report


def _describe(doc: dict, settings_by_id: dict[Any, dict]) -> str:
    s = settings_by_id[doc["settings"]]
    filter_str = f", filter={f!r}" if (f := _filter_of(doc)) else ""
    return f"{s['name']} v{s['version']} ({s['software_type']}, scope={doc['scope']!r}{filter_str})"


def _log_cofiring_report(report: dict[tuple[str, str], list[str]]) -> None:
    if not report:
        logger.info(
            "Co-firing report: no assignment newly resolves together. Nothing to do."
        )
        return
    logger.warning(
        f"Co-firing report: {len(report)} (project, instrument) pairs get additional jobs per raw file. "
        "Add `excluded_scopes` or `raw_file_id_exclude_filter` where this is not intended:"
    )
    for (project_id, instrument_id), names in report.items():
        logger.warning(f"  {project_id} / {instrument_id}: + {', '.join(names)}")


def migrate(*, dry_run: bool) -> None:
    """Convert all legacy ProjectSettings documents to the list-based scope fields."""
    connect_db()
    legacy_docs = _migrate_collection(
        ProjectSettings._get_collection(),  # noqa: SLF001
        dry_run=dry_run,
    )
    if dry_run:
        settings_by_id = {
            doc["_id"]: doc
            for doc in Settings._get_collection().find(  # noqa: SLF001
                {"_id": {"$in": [doc["settings"] for doc in legacy_docs]}}
            )
        }
        instruments = {
            instrument_id: config.get(YamlKeys.TYPE, "")
            for instrument_id, config in YAMLSETTINGS.get(
                YamlKeys.INSTRUMENTS, {}
            ).items()
        }
        _log_cofiring_report(
            build_cofiring_report(legacy_docs, settings_by_id, instruments)
        )
        logger.info("This was a dry run. No changes were made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert ProjectSettings scope fields to lists."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes and print the co-firing report without writing to the database.",
    )
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
