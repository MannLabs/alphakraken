"""Migration: Backfill `output_display_base_path` for Metrics documents without one.

Sets it to the given `display_paths.output` for all Metrics documents. Only correct if the output
file system has not changed since these were written.

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_backfill_output_display_base_path.py --output-display-base-path /fs/pool-2/output --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_backfill_output_display_base_path.py --output-display-base-path /fs/pool-2/output
"""

import argparse
import logging

from shared.db.engine import connect_db
from shared.db.models import Metrics

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def migrate(output_display_base_path: str, *, dry_run: bool) -> None:
    """Set output_display_base_path for all Metrics documents where it is None."""
    connect_db()

    collection = Metrics._get_collection()  # noqa: SLF001
    query = {"output_display_base_path": None}

    count = collection.count_documents(query)
    logger.info(f"Found {count} Metrics documents with output_display_base_path=None.")

    if count == 0:
        logger.info("Nothing to migrate.")
        return

    if dry_run:
        logger.info("This was a dry run. No changes were made.")
        return

    result = collection.update_many(
        query, {"$set": {"output_display_base_path": output_display_base_path}}
    )
    logger.info(f"Updated {result.modified_count} documents.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill `output_display_base_path` for Metrics documents without one."
    )
    parser.add_argument(
        "--output-display-base-path",
        required=True,
        help="Current `display_paths.output` from alphakraken.yaml.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(args.output_display_base_path, dry_run=args.dry_run)
