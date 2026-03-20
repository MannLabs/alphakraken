"""Migration: Convert Project.settings (1:1) to ProjectSettings (M:N).

For each Project with a non-null `settings` field, creates a ProjectSettings
entry linking the project to the same settings with scope="*".
Then removes the `settings` field from all Project documents.

# Migration part 1: (export DB credentials as env vars first):
    python -m shared._migrations._migrate_project_settings_to_mn --dry-run

# Migration part 2:
start mongosh and execute:

use krakendb
db.updateRole("webappRole", {
      privileges: [
          {
              resource: { db: "krakendb", collection: "project" },
              actions: ["find", "insert", "update", "remove"]
          },
          {
              resource: { db: "krakendb", collection: "settings" },
              actions: ["find", "insert", "update", "remove"]
          },
          {
              resource: { db: "krakendb", collection: "project_settings" },
              actions: ["find", "insert", "update", "remove"]
          },
      ]
  });
"""

import argparse
import logging

from shared.db.engine import connect_db
from shared.db.models import Project, ProjectSettings, Settings
from shared.keys import DEFAULT_SCOPE

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def migrate(*, dry_run: bool) -> None:
    """Migrate Project.settings references to ProjectSettings documents."""
    connect_db()

    project_collection = Project._get_collection()  # noqa: SLF001
    migrated = 0
    skipped = 0

    for raw_doc in project_collection.find():
        project_id = raw_doc["_id"]
        settings_ref = raw_doc.get("settings")

        if settings_ref is None:
            skipped += 1
            continue

        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Migrating project {project_id} -> settings {settings_ref}"
        )

        if not dry_run:
            project = Project.objects.get(id=project_id)
            settings = Settings.objects.get(id=settings_ref)
            ProjectSettings(
                project=project,
                settings=settings,
                scope=DEFAULT_SCOPE,
            ).save()

        migrated += 1

    if not dry_run:
        result = project_collection.update_many({}, {"$unset": {"settings": ""}})
        logger.info(
            f"Removed 'settings' field from {result.modified_count} project documents."
        )

    logger.info(
        f"Migration complete: {migrated} migrated, {skipped} skipped (no settings)."
    )
    if dry_run:
        logger.info("This was a dry run. No changes were made.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate Project.settings to ProjectSettings M:N model."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
