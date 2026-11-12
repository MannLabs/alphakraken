"""Migration: Rename the metric keys written by older calculator versions to their current names.

- every key starting with `msqc_` loses that prefix,
- `quanting_time_elapsed` becomes `time_elapsed`,
- `raw:gradient_length_m` becomes `gradient_length`.

A key whose current name already exists in the same document is left alone and reported.

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_migrate_legacy_metrics_keys.py --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_migrate_legacy_metrics_keys.py
"""

import argparse
import logging
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


def _get_renames(metrics_doc: dict[str, Any]) -> dict[str, str]:
    """Return the legacy keys of the document with their current names, without those that would collide."""
    renames = {}
    for key in metrics_doc:
        if key in LEGACY_KEYS:
            new_key = LEGACY_KEYS[key]
        elif key.startswith(MSQC_PREFIX):
            new_key = key[len(MSQC_PREFIX) :]
        else:
            continue

        if new_key in metrics_doc or new_key in renames.values():
            logger.warning(
                f"Metrics {metrics_doc['_id']}: not renaming '{key}', '{new_key}' exists."
            )
            continue
        renames[key] = new_key
    return renames


def migrate(*, dry_run: bool) -> None:
    """Rename the legacy metric keys of all Metrics documents."""
    connect_db()

    collection = Metrics._get_collection()  # noqa: SLF001

    n_documents = 0
    renamed_keys: dict[str, int] = {}
    for metrics_doc in collection.find():
        renames = _get_renames(metrics_doc)
        if not renames:
            continue

        n_documents += 1
        for key in renames:
            renamed_keys[key] = renamed_keys.get(key, 0) + 1
        if not dry_run:
            collection.update_one({"_id": metrics_doc["_id"]}, {"$rename": renames})

    for key, count in sorted(renamed_keys.items()):
        logger.info(f"  '{key}': {count} documents")
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
