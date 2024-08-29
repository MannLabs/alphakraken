"""Remove path from file_info in RawFile objects.

IMPORTANT: read notes in __init__.py!

To run the update:
 - Start bash in a airflow worker container
 - cd to this file's directory & start python
 - from _remove_relative_path_from_file_info import *
 - update_file_info_to_relative_paths(dry_run=True)
 - check logs for errors, then re-run with dry_run=False
"""
# ruff: noqa: T201, FBT001, FBT002

from shared.db.engine import connect_db
from shared.db.models import RawFile


def update_file_info_to_relative_paths(dry_run: bool = True) -> None:
    """Update all RawFile documents in the database to use relative paths in file_info.

    This method removes the backup_base_path from the beginning of each key in file_info.
    """
    connect_db()

    raw_files = RawFile.objects(backup_base_path__exists=False)
    backup_base_path = "/fs/pool/pool-backup"  # deliberately hardcoded
    skipped = 0
    for raw_file in raw_files:
        updated_file_info = {}

        if raw_file.file_info is None or len(raw_file.file_info) == 0:
            skipped += 1
            continue

        for file_path, file_data in raw_file.file_info.items():
            # Convert the file_path to a Path object and make it relative to base_path
            relative_path = file_path.replace(f"{backup_base_path}/", "")
            updated_file_info[relative_path] = file_data

        print(
            f"{dry_run=} Updating {raw_file.id}: {backup_base_path=}; {raw_file.file_info} -> {updated_file_info}."
        )
        if not dry_run:
            raw_file.update(
                set__file_info=updated_file_info, set__backup_base_path=backup_base_path
            )

    print(f"{dry_run=} Updated {len(raw_files)} RawFile documents. {skipped=}")
