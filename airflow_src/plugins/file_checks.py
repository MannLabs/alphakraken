"""Checks to make sure a file is the desired one."""

import logging
from pathlib import Path

from common.paths import get_internal_backup_path
from file_handling import get_file_hash, get_file_size

from shared.db.models import parse_file_info_item


class FileRemovalError(Exception):
    """Custom exception for file check and removal errors."""


def check_file(
    file_path_to_remove: Path,
    file_path_pool_backup: Path,
    file_info_in_db: dict[str, tuple[float, str]],
    *,
    hash_check: bool = True,
) -> None:
    """Check that the file to remove is present in the pool backup and has the same size and hash as in the DB.

    Here, "file" means every single file that is part of a raw file.

    We first compare sizes, then hashes, to prevent unnecessary network traffic.
    Calculating hashes is the costly part: the file needs to be transferred over the network.
    Unfortunately, the check only for file size is not sufficient to unambiguously identify a file.

    :param file_path_to_remove: absolute path to file to remove
    :param file_path_pool_backup: absolute path to location of file in pool backup
    :param file_info_in_db: dict with file info from DB
    :param hash_check: whether to check the hash of the file

    :raises: FileRemovalError if one of the checks fails or if file is not present on the pool backup.
    :raises: KeyError if one of the files does not exist in the DB.
    """
    # Check 1: the single file to delete is present on the pool-backup
    # Check 1b: present in the s3 bucket
    if not file_path_pool_backup.exists():
        raise FileRemovalError(f"File {file_path_pool_backup} does not exist.")

    logging.debug(f"Comparing {file_path_to_remove=} to {file_path_pool_backup=} ..")

    # map e.g. '/opt/airflow/mounts/backup/test1/2024_08/test_file_SA_P123_2.raw' => 'test1/2024_08/test_file_SA_P123_2.raw'

    # old file_info key format: 'instrument1/2024_07/file.raw'
    rel_file_path = file_path_pool_backup.relative_to(get_internal_backup_path())
    if str(rel_file_path) not in file_info_in_db:
        # TODO: this is a hack to support the new format. Remove it once the older DB entries have been migrated.
        # new file_info key format: 'file.raw'
        # => strip off instrument1/2025_07 from instrument1/2025_07/file.raw
        rel_file_path = Path(*rel_file_path.parts[2:])

    size_in_db, hash_in_db = parse_file_info_item(file_info_in_db[str(rel_file_path)])

    logging.debug(f"Comparing {file_path_to_remove=} to DB ({rel_file_path}) ..")

    # Check 2: compare the single file to delete with the DB (hash)
    # this checks that the fingerprints of the file to remove match those in the db (prevents deleting the wrong file)
    # Check 2b: compare the single file to delete with the DB (hash)
    size_to_remove = get_file_size(file_path_to_remove, verbose=False)
    hash_to_remove = None
    if size_to_remove != size_in_db or (
        hash_check
        and (hash_to_remove := get_file_hash(file_path_to_remove)) != hash_in_db
    ):
        raise FileRemovalError(
            f"File {rel_file_path} mismatch with instrument backup: {size_to_remove=} vs {size_in_db=}, {hash_to_remove=} vs {hash_in_db=}"
        )

    # Check 3: compare the single file to delete with the pool backup (hash)
    # this essentially re-checks the fingerprints that have been calculated right after file copying, would fail if pool backup was corrupted
    # Check 3: compare the single file to delete with s3 backup (etag)
    size_on_pool_backup = get_file_size(file_path_pool_backup, verbose=False)
    hash_on_pool_backup = None
    if size_on_pool_backup != size_in_db or (
        hash_check
        and (hash_on_pool_backup := get_file_hash(file_path_pool_backup)) != hash_in_db
    ):
        raise FileRemovalError(
            f"File {rel_file_path} mismatch with pool backup: {size_on_pool_backup=} vs {size_in_db=}, {hash_on_pool_backup=} vs {hash_in_db=}"
        )
