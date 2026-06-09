"""Migration: Backfill FALLBACK_PROJECT_ID for RawFile documents with null project_id.

Sets project_id to "_FALLBACK" for all RawFile documents where project_id is None.

# Usage (export DB credentials as env vars first):
    python -m shared._migrations._backfill_fallback_project_id --dry-run
    python -m shared._migrations._backfill_fallback_project_id
"""

import argparse
import logging

from shared.db.engine import connect_db
from shared.db.models import RawFile
from shared.keys import FALLBACK_PROJECT_ID

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def migrate(*, dry_run: bool) -> None:
    """Set project_id to FALLBACK_PROJECT_ID for all RawFile documents where it is None."""
    connect_db()

    collection = RawFile._get_collection()  # noqa: SLF001
    query = {"project_id": None}

    count = collection.count_documents(query)
    logger.info(f"Found {count} RawFile documents with project_id=None.")

    if count == 0:
        logger.info("Nothing to migrate.")
        return

    if dry_run:
        logger.info("This was a dry run. No changes were made.")
        return

    result = collection.update_many(
        query, {"$set": {"project_id": FALLBACK_PROJECT_ID}}
    )
    logger.info(f"Updated {result.modified_count} documents.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill FALLBACK_PROJECT_ID for RawFile documents with null project_id."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
