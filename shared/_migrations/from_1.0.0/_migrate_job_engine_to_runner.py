"""Migration: rename `Settings.job_engine` to `Settings.runner_name`.

Quanting jobs run on named runners declared under `runners:` in alphakraken.yaml, and a settings
entry references one by name. The in-repo yamls name each runner after its engine, so the default
mapping is the identity; edit `_ENGINE_TO_RUNNER` if a deployment names them differently.

Precondition: alphakraken.yaml must declare a runner for every target name this script prints at
the end, otherwise the migrated settings fail at `prepare_job`.

For each Settings document with `job_engine` and without `runner_name`:
    runner_name <- _ENGINE_TO_RUNNER[job_engine]
    job_engine  -> removed

# Usage (export DB credentials as env vars first):
    PYTHONPATH=. python shared/_migrations/from_1.0.0/_migrate_job_engine_to_runner.py --dry-run
    PYTHONPATH=. python shared/_migrations/from_1.0.0/_migrate_job_engine_to_runner.py
"""

import argparse
import logging
from collections import Counter
from typing import Any

from shared.db.engine import connect_db
from shared.db.models import Settings
from shared.keys import JobEngines

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# edit before running if the yaml names its runners differently than their engines
_ENGINE_TO_RUNNER: dict[str, str] = {
    engine: engine for engine in JobEngines.get_values()
}


def _migrate_collection(collection: Any, *, dry_run: bool) -> Counter[str]:
    """Rewrite the legacy documents of the Settings collection, returning the target names with counts."""
    target_names: Counter[str] = Counter()
    skipped = 0

    for doc in collection.find().sort("created_at_", 1):
        if "runner_name" in doc or "job_engine" not in doc:
            skipped += 1
            continue

        runner_name = _ENGINE_TO_RUNNER[doc["job_engine"]]
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Settings {doc['_id']} "
            f"({doc.get('name')!r} v{doc.get('version')}): "
            f"job_engine={doc['job_engine']!r} -> runner_name={runner_name!r}"
        )

        if not dry_run:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"runner_name": runner_name}, "$unset": {"job_engine": ""}},
            )

        target_names[runner_name] += 1

    logger.info(
        f"Migration complete: {sum(target_names.values())} updated, {skipped} skipped."
    )
    logger.info(
        f"Target runner names, each must be declared in alphakraken.yaml: {dict(target_names)}"
    )
    return target_names


def migrate(*, dry_run: bool) -> None:
    """Rename `job_engine` to `runner_name` on all legacy Settings documents."""
    connect_db()
    _migrate_collection(Settings._get_collection(), dry_run=dry_run)  # noqa: SLF001
    if dry_run:
        logger.info("This was a dry run. No changes were made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rename Settings.job_engine to Settings.runner_name."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
