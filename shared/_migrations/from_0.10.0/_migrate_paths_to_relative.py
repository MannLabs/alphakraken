"""Migration: store location-relative paths in the DB instead of absolute ones.

Absolute paths are composed for display from `display_paths` in alphakraken.yaml, so the DB holds
paths relative to the locations:
    Metrics.output_path       -> Metrics.relative_output_path, the output base path stripped
    RawFile.backup_base_path  -> removed, it derives from `instrument_id` and `created_at`

Metrics documents were written by every runner in its own view, so pass the `output` location of
each runner that ever ran. A document whose output_path starts with none of them is reported and
left untouched: add the missing base path and rerun.

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.10.0/_migrate_paths_to_relative.py --output-base-path /fs/pool-2/output --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.10.0/_migrate_paths_to_relative.py --output-base-path /fs/pool-2/output /other/output
"""

import argparse
import logging
from collections import Counter
from typing import Any

from shared.db.engine import connect_db
from shared.db.models import Metrics, RawFile

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_OLD_METRICS_FIELD = "output_path"
_NEW_METRICS_FIELD = "relative_output_path"
_OLD_RAW_FILE_FIELD = "backup_base_path"


def _strip_base_path(path: str, base_paths: list[str]) -> str | None:
    """Get `path` relative to the first of `base_paths` it lies below, None if there is none."""
    for base_path in base_paths:
        prefix = base_path.rstrip("/") + "/"
        if path.startswith(prefix):
            return path[len(prefix) :]
    return None


def _migrate_metrics(
    collection: Any, base_paths: list[str], *, dry_run: bool
) -> Counter[str]:
    """Replace the absolute output path of each Metrics document by the relative one."""
    counts: Counter[str] = Counter()
    for doc in collection.find({_OLD_METRICS_FIELD: {"$exists": True}}):
        output_path = doc[_OLD_METRICS_FIELD]
        relative_output_path = _strip_base_path(output_path, base_paths)

        if relative_output_path is None:
            logger.warning(
                f"Metrics {doc['_id']}: {output_path!r} is below none of {base_paths}, skipping."
            )
            counts["skipped"] += 1
            continue

        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Metrics {doc['_id']}: "
            f"{output_path!r} -> {relative_output_path!r}"
        )
        if not dry_run:
            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {_NEW_METRICS_FIELD: relative_output_path},
                    "$unset": {_OLD_METRICS_FIELD: ""},
                },
            )
        counts["migrated"] += 1

    logger.info(f"Metrics: {counts['migrated']} migrated, {counts['skipped']} skipped.")
    return counts


def _migrate_raw_files(collection: Any, *, dry_run: bool) -> int:
    """Remove the backup base path from all RawFile documents, returning their count."""
    query = {_OLD_RAW_FILE_FIELD: {"$exists": True}}
    count = collection.count_documents(query)

    if not dry_run:
        collection.update_many(query, {"$unset": {_OLD_RAW_FILE_FIELD: ""}})

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}RawFile: `{_OLD_RAW_FILE_FIELD}` removed from {count} documents."
    )
    return count


def migrate(base_paths: list[str], *, dry_run: bool) -> None:
    """Migrate the Metrics and RawFile collections to relative paths."""
    connect_db()
    _migrate_metrics(Metrics._get_collection(), base_paths, dry_run=dry_run)  # noqa: SLF001
    _migrate_raw_files(RawFile._get_collection(), dry_run=dry_run)  # noqa: SLF001
    if dry_run:
        logger.info("This was a dry run. No changes were made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Store location-relative paths in Metrics and RawFile documents."
    )
    parser.add_argument(
        "--output-base-path",
        nargs="+",
        required=True,
        help="The `output` location of each runner that wrote Metrics documents.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(args.output_base_path, dry_run=args.dry_run)
