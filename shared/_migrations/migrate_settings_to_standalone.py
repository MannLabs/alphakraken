"""Migration: Convert Settings from project-owned to standalone.

This migration is IDEMPOTENT - safe to run multiple times.
It handles database changes only (file moves are manual/separate).

Migration steps:
1. Verify/create unique index on (name, version)
2. For each Settings with 'project' field (not yet migrated):
   a. Generate name as "{project_id}_v{version}"
   b. Remove 'project' field
   c. Mark as migrated (set migrated_at_ timestamp)
3. For each Project with settings reference:
   a. Remove Project.settings field
4. Convert INACTIVE status to ARCHIVED

Usage:
    python -m shared.migrations.migrate_settings_to_standalone --dry-run
    python -m shared.migrations.migrate_settings_to_standalone --execute
    python -m shared.migrations.migrate_settings_to_standalone --verify
"""

# ruff: noqa
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from mongoengine import get_db
from mongoengine.errors import DoesNotExist, NotUniqueError

from shared.db.engine import connect_db
from shared.db.models import Project, ProjectStatus, Settings, SettingsStatus

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class MigrationResult:
    """Container for migration results."""

    def __init__(self) -> None:
        self.settings_migrated = 0
        self.settings_skipped = 0
        self.projects_cleared = 0
        self.projects_skipped = 0
        self.status_converted = 0
        self.errors: list[str] = []
        self.migrated_settings: list[
            tuple[str, str, int]
        ] = []  # (project_id, settings_name, settings_version)

    def add_error(self, error: str) -> None:
        """Add an error message."""
        self.errors.append(error)
        logger.error(error)

    def print_summary(self, dry_run: bool) -> None:
        """Print migration summary."""
        mode = "DRY RUN" if dry_run else "EXECUTION"
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Migration Summary ({mode})")
        logger.info(f"{'=' * 60}")
        logger.info(f"Settings migrated: {self.settings_migrated}")
        logger.info(f"Settings skipped (already migrated): {self.settings_skipped}")
        logger.info(f"Projects cleared: {self.projects_cleared}")
        logger.info(f"Projects skipped (no settings field): {self.projects_skipped}")
        logger.info(f"Status converted (INACTIVE → ARCHIVED): {self.status_converted}")

        if self.migrated_settings:
            logger.info("\nMigrated Settings (generated names):")
            for project_id, settings_name, version in self.migrated_settings:
                logger.info(f"  {settings_name} v{version} (from project {project_id})")

        if self.errors:
            logger.error(f"\nErrors encountered: {len(self.errors)}")
            for error in self.errors:
                logger.error(f"  - {error}")
        else:
            logger.info("\nNo errors encountered.")

        logger.info(f"{'=' * 60}\n")


def ensure_unique_index() -> None:
    """Create the unique index on (name, version) after migration.

    This should only be called after all Settings have been migrated to the new naming format.
    Creating the index before migration would fail due to duplicate (name, version) pairs.
    """
    logger.info("Checking for unique index on (name, version)...")
    connect_db()

    existing_indexes = Settings._get_collection().index_information()

    index_exists = any(
        idx_info.get("key") == [("name", 1), ("version", 1)]
        and idx_info.get("unique", False)
        for idx_info in existing_indexes.values()
    )

    if index_exists:
        logger.info("✓ Unique index on (name, version) already exists")
    else:
        logger.info("Creating unique index on (name, version)...")
        Settings._get_collection().create_index(
            [("name", 1), ("version", 1)], unique=True
        )
        logger.info("✓ Unique index created")


def migrate_settings(dry_run: bool) -> MigrationResult:
    """Migrate Settings from project-owned to standalone."""
    result = MigrationResult()
    connect_db()

    logger.info("\n" + "=" * 60)
    logger.info("Phase 0: Preparing database (dropping old unique index if exists)")
    logger.info("=" * 60)

    db = get_db()
    collection = db["settings"]
    existing_indexes = collection.index_information()

    index_exists = any(
        idx_info.get("key") == [("name", 1), ("version", 1)]
        and idx_info.get("unique", False)
        for idx_info in existing_indexes.values()
    )

    if index_exists:
        logger.info("Found existing unique index on (name, version)")
        if not dry_run:
            logger.info("Dropping index to allow migration...")
            collection.drop_index([("name", 1), ("version", 1)])
            logger.info("✓ Index dropped")
        else:
            logger.info("Would drop index (dry-run mode)")
    else:
        logger.info("No existing unique index found")

    logger.info("\n" + "=" * 60)
    logger.info("Phase 1: Migrating Settings documents")
    logger.info("=" * 60)

    settings_to_migrate = []
    for settings in Settings.objects:
        raw_doc = Settings._get_collection().find_one({"_id": settings.id})

        if raw_doc is None:
            result.add_error(f"Settings {settings.id} not found in raw collection")
            continue

        has_project_field = "project" in raw_doc
        has_migrated_field = "migrated_at_" in raw_doc

        if has_migrated_field:
            logger.debug(f"Skipping Settings {settings.id} (already migrated)")
            result.settings_skipped += 1
            continue

        if not has_project_field:
            logger.debug(
                f"Skipping Settings {settings.id} (no project field, assuming migrated)"
            )
            result.settings_skipped += 1
            continue

        project_ref = raw_doc["project"]

        if project_ref is None:
            result.add_error(
                f"Settings {settings.id} has project field but it's None - manual intervention required"
            )
            continue

        settings_to_migrate.append((settings, project_ref))

    logger.info(f"Found {len(settings_to_migrate)} Settings to migrate")

    for settings, project_ref in settings_to_migrate:
        try:
            project = Project.objects.get(id=project_ref)
        except DoesNotExist:
            result.add_error(
                f"Settings {settings.id} references non-existent Project {project_ref}"
            )
            continue

        project_id = project.id
        existing_name = settings.name
        existing_version = settings.version

        new_name = f"{project_id}_{existing_name}_{existing_version}"

        logger.info(
            f"Migrating Settings {settings.id}: name={new_name} for project={project_id}"
        )

        if not dry_run:
            try:
                collection = Settings._get_collection()
                collection.update_one(
                    {"_id": settings.id},
                    {
                        "$set": {
                            "name": new_name,
                            "migrated_at_": datetime.now(),
                        },
                        "$unset": {"project": ""},
                    },
                )

                settings.reload()
                logger.info(f"✓ Migrated Settings {settings.id} → {new_name}")

            except NotUniqueError:
                result.add_error(
                    f"Settings {settings.id}: name conflict for '{new_name}' - manual resolution required"
                )
                continue
            except Exception as e:
                result.add_error(f"Settings {settings.id}: failed to migrate - {e}")
                continue

        result.settings_migrated += 1
        result.migrated_settings.append((project_id, new_name, settings.version))

    logger.info("\n" + "=" * 60)
    logger.info("Phase 2: Removing settings references from Projects")
    logger.info("=" * 60)

    projects_with_settings = []
    for project in Project.objects:
        raw_doc = Project._get_collection().find_one({"_id": project.id})

        if raw_doc is None:
            result.add_error(f"Project {project.id} not found in raw collection")
            continue

        has_settings_field = "settings" in raw_doc

        if not has_settings_field:
            logger.debug(f"Skipping Project {project.id} (no settings field)")
            result.projects_skipped += 1
            continue

        projects_with_settings.append(project.id)

    logger.info(
        f"Found {len(projects_with_settings)} Projects with settings field to clear"
    )

    for project_id in projects_with_settings:
        logger.info(f"Removing settings from Project {project_id}")

        if not dry_run:
            try:
                collection = Project._get_collection()
                collection.update_one(
                    {"_id": project_id},
                    {"$unset": {"settings": ""}},
                )
                logger.info(f"✓ Cleared settings from Project {project_id}")

            except Exception as e:
                result.add_error(
                    f"Project {project_id}: failed to clear settings - {e}"
                )
                continue

        result.projects_cleared += 1

    logger.info("\n" + "=" * 60)
    logger.info("Phase 4: Creating unique index on (name, version)")
    logger.info("=" * 60)

    if not dry_run:
        ensure_unique_index()
    else:
        logger.info("Skipping index creation in dry-run mode")

    return result


def verify_migration() -> None:
    """Verify migration integrity."""
    logger.info("\n" + "=" * 60)
    logger.info("Verifying Migration")
    logger.info("=" * 60)

    connect_db()
    errors = []

    logger.info("Checking for Settings with 'project' field...")
    settings_with_project = []
    for settings in Settings.objects:
        raw_doc = Settings._get_collection().find_one({"_id": settings.id})
        if raw_doc and "project" in raw_doc:
            settings_with_project.append(str(settings.id))

    if settings_with_project:
        errors.append(
            f"Found {len(settings_with_project)} Settings with 'project' field: {settings_with_project}"
        )
    else:
        logger.info("✓ No Settings have 'project' field")

    logger.info("Checking for Settings with INACTIVE status...")
    inactive_settings = Settings.objects(status=ProjectStatus.INACTIVE)
    if inactive_settings.count() > 0:
        errors.append(
            f"Found {inactive_settings.count()} Settings with INACTIVE status (should be ARCHIVED)"
        )
    else:
        logger.info("✓ No Settings with INACTIVE status")

    logger.info("Checking unique index on (name, version)...")
    existing_indexes = Settings._get_collection().index_information()
    index_exists = any(
        idx_info.get("key") == [("name", 1), ("version", 1)]
        and idx_info.get("unique", False)
        for idx_info in existing_indexes.values()
    )

    if index_exists:
        logger.info("✓ Unique index on (name, version) exists")
    else:
        errors.append("Unique index on (name, version) is missing")

    logger.info("Checking for duplicate (name, version) pairs...")
    pipeline = [
        {
            "$group": {
                "_id": {"name": "$name", "version": "$version"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]
    duplicates = list(Settings._get_collection().aggregate(pipeline))
    if duplicates:
        errors.append(f"Found duplicate (name, version) pairs: {duplicates}")
    else:
        logger.info("✓ No duplicate (name, version) pairs")

    logger.info("Checking for Projects with 'settings' field...")
    projects_with_settings_field = []
    for project in Project.objects:
        raw_doc = Project._get_collection().find_one({"_id": project.id})
        if raw_doc and "settings" in raw_doc:
            projects_with_settings_field.append(str(project.id))

    if projects_with_settings_field:
        errors.append(
            f"Found {len(projects_with_settings_field)} Projects with 'settings' field: {projects_with_settings_field}"
        )
    else:
        logger.info("✓ No Projects have 'settings' field")

    total_settings = Settings.objects.count()
    total_projects = Project.objects.count()

    logger.info("\nStatistics:")
    logger.info(f"  Total Settings: {total_settings}")
    logger.info(f"  Total Projects: {total_projects}")

    logger.info(f"\n{'=' * 60}")
    if errors:
        logger.error("Verification FAILED:")
        for error in errors:
            logger.error(f"  - {error}")
    else:
        logger.info("✓ Verification PASSED - Migration complete and valid")
    logger.info(f"{'=' * 60}\n")


def main() -> None:
    """Run migration script."""
    parser = argparse.ArgumentParser(
        description="Migrate Settings from project-owned to standalone"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying database",
    )
    group.add_argument(
        "--execute",
        action="store_true",
        help="Execute migration (modify database)",
    )
    group.add_argument(
        "--verify",
        action="store_true",
        help="Verify migration integrity",
    )

    args = parser.parse_args()

    if args.verify:
        verify_migration()
        return

    logger.info(f"\n{'=' * 60}")
    logger.info("Settings Migration to Standalone")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'EXECUTE'}")
    logger.info(f"{'=' * 60}\n")

    if args.execute:
        logger.warning("⚠️  EXECUTING MIGRATION - DATABASE WILL BE MODIFIED")
        response = input("Type 'yes' to continue: ")
        if response.lower() != "yes":
            logger.info("Migration cancelled")
            return

    result = migrate_settings(dry_run=args.dry_run)
    result.print_summary(dry_run=args.dry_run)

    if not args.dry_run and not result.errors:
        logger.info("Running verification...")
        verify_migration()


if __name__ == "__main__":
    main()
