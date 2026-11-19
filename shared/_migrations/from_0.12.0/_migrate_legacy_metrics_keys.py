"""Migration: Rename the metric keys written by older calculator versions to their current names.

- every key starting with `msqc_` loses that prefix,
- `quanting_time_elapsed` becomes `time_elapsed`,
- `raw:gradient_length_m` becomes `gradient_length`.

If the current name already exists in the same document, the legacy key is dropped when both
hold the same value, and otherwise left alone and reported.

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_migrate_legacy_metrics_keys.py --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_migrate_legacy_metrics_keys.py
"""

import argparse
import logging
from collections import Counter
from typing import Any

from shared.db._mongo.connection import connect_db
from shared.db._mongo.models import Metrics

MSQC_PREFIX = "msqc_"
LEGACY_KEYS = {
    "quanting_time_elapsed": "time_elapsed",
    "raw:gradient_length_m": "gradient_length",
}

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _get_changes(metrics_doc: dict[str, Any]) -> tuple[dict[str, str], list[str]]:
    """Return the legacy keys to rename to their current names, and those to drop as the current name holds the same value."""
    renames: dict[str, str] = {}
    drops: list[str] = []
    for key, value in metrics_doc.items():
        if key in LEGACY_KEYS:
            new_key = LEGACY_KEYS[key]
        elif key.startswith(MSQC_PREFIX):
            new_key = key[len(MSQC_PREFIX) :]
        else:
            continue

        # the key that holds or will hold the current name after the renames
        existing = new_key if new_key in metrics_doc else None
        existing = existing or next(
            (k for k, new in renames.items() if new == new_key), None
        )
        if existing is None:
            renames[key] = new_key
        elif metrics_doc[existing] == value:
            drops.append(key)
        else:
            logger.warning(
                f"Metrics {metrics_doc['_id']}: not renaming '{key}', '{new_key}' exists with another value."
            )
    return renames, drops


def _update_operators(renames: dict[str, str], drops: list[str]) -> dict[str, Any]:
    """Return the update operators for the renames and drops, leaving out empty ones, which MongoDB rejects."""
    operators: dict[str, Any] = {}
    if renames:
        operators["$rename"] = renames
    if drops:
        operators["$unset"] = dict.fromkeys(drops, "")
    return operators


def migrate(*, dry_run: bool) -> None:
    """Rename the legacy metric keys of all Metrics documents."""
    connect_db()

    collection = Metrics._get_collection()  # noqa: SLF001

    n_documents = 0
    renamed_keys: Counter[str] = Counter()
    dropped_keys: Counter[str] = Counter()
    for metrics_doc in collection.find():
        renames, drops = _get_changes(metrics_doc)
        if not renames and not drops:
            continue

        n_documents += 1
        renamed_keys.update(renames)
        dropped_keys.update(drops)
        if not dry_run:
            collection.update_one(
                {"_id": metrics_doc["_id"]}, _update_operators(renames, drops)
            )

    for key, count in sorted(renamed_keys.items()):
        logger.info(f"  renamed '{key}': {count} documents")
    for key, count in sorted(dropped_keys.items()):
        logger.info(
            f"  dropped '{key}', same value as its current name: {count} documents"
        )
    if dry_run:
        logger.info(f"Dry run complete. {n_documents} documents would be updated.")
    else:
        logger.info(f"Updated {n_documents} documents.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rename legacy metric keys of Metrics documents."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
