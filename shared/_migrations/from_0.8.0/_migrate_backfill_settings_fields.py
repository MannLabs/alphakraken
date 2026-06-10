"""Migration: Backfill new required fields on legacy Settings documents.

The new Settings schema adds fields that quanting jobs read directly
(`processor_impl.py`): `metrics_type` (required, no default -> reads as None on
legacy docs), `slurm_cpus_per_task`, `slurm_mem`, `slurm_time`, `num_threads`,
`number`, and `job_engine`. Without a backfill, legacy Settings linked via
ProjectSettings produce jobs with metrics_type=None and empty slurm parameters.

For each Settings document missing a field, this sets:
    metrics_type        <- software_type
    slurm_cpus_per_task ) from SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS
    slurm_mem           ) keyed by software_type, falling back to the CUSTOM
    slurm_time          ) profile for unknown software types
    num_threads         )
    job_engine          <- "slurm"
    number              <- next free sequential integer

Run this BEFORE _migrate_project_settings_to_mn (order does not strictly matter,
but legacy settings must be backfilled before they are used by new jobs).

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_0.8.0/_migrate_backfill_settings_fields.py --dry-run
    PYTHONPATH=. python shared/_migrations/from_0.8.0/_migrate_backfill_settings_fields.py
"""

import argparse
import logging

from shared.db.engine import connect_db
from shared.db.models import Settings
from shared.keys import (
    SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS,
    JobEngines,
    SoftwareTypes,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# software types without a dedicated profile fall back to the CUSTOM resources
_FALLBACK_RESOURCE_PARAMS = SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS[
    SoftwareTypes.CUSTOM
]


def migrate(*, dry_run: bool) -> None:  # noqa: C901
    """Backfill missing required fields on legacy Settings documents."""
    connect_db()

    collection = Settings._get_collection()  # noqa: SLF001

    existing_numbers = {
        doc["number"] for doc in collection.find({"number": {"$gt": 0}}, {"number": 1})
    }
    next_number = (max(existing_numbers) + 1) if existing_numbers else 1

    migrated = 0
    skipped = 0

    for doc in collection.find().sort("created_at_", 1):
        software_type = doc.get("software_type", SoftwareTypes.ALPHADIA)
        resource_params = SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS.get(
            software_type, _FALLBACK_RESOURCE_PARAMS
        )

        updates: dict = {}
        if doc.get("metrics_type") is None:
            updates["metrics_type"] = software_type
        if doc.get("slurm_cpus_per_task") is None:
            updates["slurm_cpus_per_task"] = resource_params.slurm_cpus_per_task
        if not doc.get("slurm_mem"):
            updates["slurm_mem"] = resource_params.slurm_mem
        if not doc.get("slurm_time"):
            updates["slurm_time"] = resource_params.slurm_time
        if doc.get("num_threads") is None:
            updates["num_threads"] = resource_params.num_threads
        if doc.get("job_engine") is None:
            updates["job_engine"] = JobEngines.SLURM
        if doc.get("number") is None or doc["number"] < 1:
            updates["number"] = next_number
            next_number += 1

        if not updates:
            skipped += 1
            continue

        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Backfilling settings "
            f"{doc['_id']} ({doc.get('name')!r} v{doc.get('version')}, "
            f"software_type={software_type!r}) <- {updates}"
        )

        if not dry_run:
            collection.update_one({"_id": doc["_id"]}, {"$set": updates})

        migrated += 1

    logger.info(f"Backfill complete: {migrated} updated, {skipped} already complete.")
    if dry_run:
        logger.info("This was a dry run. No changes were made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill new required fields on legacy Settings documents."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
