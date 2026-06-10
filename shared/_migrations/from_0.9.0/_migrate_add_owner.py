"""Migration: Add a required `owner` field to Project and Settings documents.

Seeds a placeholder User and assigns it as owner to all Project and Settings
documents that don't have an owner yet.

# Migration part 1 (export DB credentials as env vars first):
    PYTHONPATH=. python shared._migrations/from_0.9.0/_migrate_add_owner --dry-run
    PYTHONPATH=. python shared._migrations/from_0.9.0/_migrate_add_owner

# Migration part 2:
start mongosh and execute (grant the webapp role write access to the new `user`
collection):

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
          {
              resource: { db: "krakendb", collection: "user" },
              actions: ["find", "insert", "update", "remove"]
          },
      ]
  });
"""

import argparse
import logging

from shared.db.engine import connect_db
from shared.db.models import Project, Settings, User

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PLACEHOLDER_EMAIL = "kraken@alphapept.org"
PLACEHOLDER_INITIALS = "NA"


def migrate(*, dry_run: bool) -> None:
    """Seed a placeholder user and backfill owner on Project/Settings documents."""
    connect_db()

    project_collection = Project._get_collection()  # noqa: SLF001
    settings_collection = Settings._get_collection()  # noqa: SLF001

    num_projects = project_collection.count_documents({"owner": {"$exists": False}})
    num_settings = settings_collection.count_documents({"owner": {"$exists": False}})

    logger.info(f"Found {num_projects} Project documents without 'owner' field.")
    logger.info(f"Found {num_settings} Settings documents without 'owner' field.")

    placeholder_exists = User.objects(email=PLACEHOLDER_EMAIL).first() is not None
    if placeholder_exists:
        logger.info(f"Placeholder user '{PLACEHOLDER_EMAIL}' already exists.")
    else:
        logger.info(f"Will create placeholder user '{PLACEHOLDER_EMAIL}'.")

    if dry_run:
        logger.info("This was a dry run. No changes were made.")
        return

    if not placeholder_exists:
        User(email=PLACEHOLDER_EMAIL, initials=PLACEHOLDER_INITIALS).save(
            force_insert=True
        )
        logger.info(f"Created placeholder user '{PLACEHOLDER_EMAIL}'.")

    placeholder = User.objects.get(email=PLACEHOLDER_EMAIL)

    project_result = Project.objects(owner__exists=False).update(owner=placeholder)
    settings_result = Settings.objects(owner__exists=False).update(owner=placeholder)

    logger.info(f"Updated {project_result} Project documents.")
    logger.info(f"Updated {settings_result} Settings documents.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add owner field to Project and Settings documents."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview changes without writing to the database.",
    )
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
