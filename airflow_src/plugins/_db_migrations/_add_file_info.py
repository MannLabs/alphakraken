"""Add missing file_info to RawFile objects.

IMPORTANT: read notes in __init__.py!

To run the update:
- Start bash in an airflow worker container
- cd to this file's directory & start python
- from _add_file_info import *
- add_file_info("test2", dry_run=True)
- check logs for errors, then re-run with dry_run=False

"""
# ruff: noqa: T201, FBT001, FBT002

import sys

from shared.db.engine import connect_db
from shared.db.models import RawFile

sys.path.insert(0, "/opt/airflow/plugins")
from plugins.common.settings import get_internal_backup_path
from plugins.file_handling import _get_file_hash, get_file_size
from plugins.raw_file_wrapper_factory import RawFileWrapperFactory


def add_file_info(instrument_id: str, dry_run: bool = True) -> None:
    """Update all RawFile documents in the database that don't have file_info set.

    This method uses RawDataWrapper to get associated files, calculates their size and hash,
    and adds this information to the RawFile document.
    """
    connect_db()

    # Get all RawFile documents without file_info
    raw_files = RawFile.objects(instrument_id=instrument_id)

    skipped = 0
    for raw_file in raw_files:
        # Create the appropriate RawFileCopyWrapper
        copy_wrapper = RawFileWrapperFactory.create_copy_wrapper(
            instrument_id=raw_file.instrument_id, raw_file=raw_file
        )
        if raw_file.file_info is not None and len(raw_file.file_info) > 0:
            skipped += 1
            continue

        file_info = {}
        backup_base_path = get_internal_backup_path()
        # Get all files associated with this raw file
        for dst_path in copy_wrapper.get_files_to_copy().values():
            # Calculate size and hash
            size = get_file_size(dst_path)
            file_hash = _get_file_hash(dst_path)

            # Use the relative path as the key
            relative_path = dst_path.relative_to(backup_base_path)
            file_info[str(relative_path)] = (size, file_hash)

        true_backup_base_path = "/fs/pool/pool-backup"
        print(
            f"{dry_run=} Updating {raw_file.id}: {true_backup_base_path}; {file_info}"
        )

        if not dry_run:
            raw_file.update(file_info=file_info, backup_base_path=true_backup_base_path)

    print(f"Processed {len(raw_files)} RawFile documents. {skipped=}")
