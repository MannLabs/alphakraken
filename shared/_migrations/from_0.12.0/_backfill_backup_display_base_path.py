"""Migration: Backfill `backup_display_base_path` for RawFile documents without one.

Sets it to the given `display_paths.backup` for all copied RawFile documents (i.e. backup_status is
not `skipped`). Only correct if the backup file system has not changed since these were copied.

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_backfill_backup_display_base_path.py --backup-display-base-path /fs/pool-1/backup --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.12.0/_backfill_backup_display_base_path.py --backup-display-base-path /fs/pool-1/backup
"""

import argparse
import logging

from shared.db.engine import connect_db
from shared.db.models import BackupStatus, RawFile

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def migrate(backup_display_base_path: str, *, dry_run: bool) -> None:
    """Set backup_display_base_path for all copied RawFile documents where it is None."""
    connect_db()

    collection = RawFile._get_collection()  # noqa: SLF001
    query = {
        "backup_display_base_path": None,
        "backup_status": {"$ne": BackupStatus.SKIPPED},
    }

    count = collection.count_documents(query)
    logger.info(f"Found {count} RawFile documents with backup_display_base_path=None.")

    if count == 0:
        logger.info("Nothing to migrate.")
        return

    if dry_run:
        logger.info("This was a dry run. No changes were made.")
        return

    result = collection.update_many(
        query, {"$set": {"backup_display_base_path": backup_display_base_path}}
    )
    logger.info(f"Updated {result.modified_count} documents.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill `backup_display_base_path` for RawFile documents without one."
    )
    parser.add_argument(
        "--backup-display-base-path",
        required=True,
        help="Current `display_paths.backup` from alphakraken.yaml.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(args.backup_display_base_path, dry_run=args.dry_run)
