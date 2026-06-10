"""Migration: Backfill output_path for Metrics documents.

Constructs the output_path from a given base path and the raw file / metrics metadata,
using the same logic as get_output_folder_rel_path.

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.8.0/_backfill_metrics_output_path.py --output-base-path /data/output --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.8.0/_backfill_metrics_output_path.py --output-base-path /data/output
"""

import argparse
import csv
import logging
from pathlib import Path

from mongoengine import DoesNotExist

from shared.db.engine import connect_db
from shared.db.models import Metrics, RawFile, get_created_at_year_month

OUTPUT_FOLDER_PREFIX = "out_"
LOG_FILE_NAME = "backfill_metrics_output_path.csv"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _get_output_path(
    raw_file: RawFile,
    metrics_type: str,  # noqa: ARG001
    output_base_path: Path,
) -> str:
    """Construct the output path for a metrics document."""
    optional_sub_folder = (
        get_created_at_year_month(raw_file) if not raw_file.has_project else ""
    )
    project_id = (
        raw_file.project_id
        if raw_file.project_id
        else (
            "_FALLBACK_BRUKER" if raw_file.original_name.endwith(".d") else "_FALLBACK"
        )
    )
    return str(
        output_base_path
        / project_id
        / optional_sub_folder
        / f"{OUTPUT_FOLDER_PREFIX}{raw_file.id}"
        # / metrics_type
    )


def migrate(*, output_base_path: Path, dry_run: bool) -> None:
    """Backfill output_path for all Metrics documents where it is not set."""
    connect_db()

    collection = Metrics._get_collection()  # noqa: SLF001
    query = {"$or": [{"output_path": None}, {"output_path": {"$exists": False}}]}

    count = collection.count_documents(query)
    logger.info(f"Found {count} Metrics documents without output_path.")

    if count == 0:
        logger.info("Nothing to migrate.")
        return

    updated = 0
    errors = 0
    log_path = Path(LOG_FILE_NAME)
    with log_path.open("w", newline="") as log_file:
        log_writer = csv.writer(log_file, lineterminator="\n")
        log_writer.writerow(["raw_file_id", "output_path"])

        for metrics_doc in collection.find(query):
            raw_file_ref = metrics_doc.get("raw_file")
            metrics_type = metrics_doc.get("type", "alphadia")

            if raw_file_ref is None:
                logger.warning(
                    f"Metrics {metrics_doc['_id']}: no raw_file reference, skipping."
                )
                errors += 1
                continue

            try:
                raw_file = RawFile.objects.get(id=raw_file_ref)
            except DoesNotExist:
                logger.warning(
                    f"Metrics {metrics_doc['_id']}: RawFile {raw_file_ref} not found, skipping."
                )
                errors += 1
                continue

            output_path = _get_output_path(raw_file, metrics_type, output_base_path)
            log_writer.writerow([raw_file.id, output_path])

            if dry_run:
                logger.info(
                    f"  Would set output_path={output_path} on Metrics {metrics_doc['_id']}"
                )
            else:
                collection.update_one(
                    {"_id": metrics_doc["_id"]},
                    {"$set": {"output_path": output_path}},
                )
                updated += 1

    logger.info(f"Wrote log to {log_path.resolve()}")

    if dry_run:
        logger.info(
            f"Dry run complete. {count - errors} would be updated, {errors} skipped."
        )
    else:
        logger.info(f"Updated {updated} documents, {errors} skipped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill output_path for Metrics documents."
    )
    parser.add_argument(
        "--output-base-path",
        type=Path,
        required=True,
        help="Base path for quanting output directories.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(output_base_path=args.output_base_path, dry_run=args.dry_run)
