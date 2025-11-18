"""Checks to make sure a file is the desired one."""

import logging
from pathlib import Path

from common.paths import (
    get_internal_backup_path,
)
from file_handling import get_file_hash, get_file_size
from raw_file_wrapper_factory import CopyPathProvider

from shared.db.models import RawFile, parse_file_info_item


class FileRemovalError(Exception):
    """Custom exception for file check and removal errors."""


class FileIdentifier:
    """Class to identify and verify files before removal."""

    def __init__(self, raw_file: RawFile) -> None:
        """Initialize the FileIdentifier with a RawFile instance."""
        self._raw_file = raw_file

        self._internal_backup_path = CopyPathProvider(
            instrument_id=raw_file.instrument_id, raw_file=raw_file
        ).get_target_folder_path()

    def check_file(
        self,
        abs_file_path_to_check: Path,
        rel_file_path: Path,
        *,
        hash_check: bool = True,
    ) -> None:
        """Check that the file to remove is present in the pool backup and has the same size and hash as in the DB.

        Here, "file" means every single file that is part of a raw file.

        We first compare sizes, then hashes, to prevent unnecessary network traffic.
        Calculating hashes is the costly part: the file needs to be transferred over the network.
        Unfortunately, the check only for file size is not sufficient to unambiguously identify a file.

        :param abs_file_path_to_check: absolute path to file to remove
        :param rel_file_path: relative path to file (<-> file_info)
        :param hash_check: whether to check the hash of the file

        :raises: FileRemovalError if one of the checks fails or if file is not present on the pool backup.
        :raises: KeyError if one of the files does not exist in the DB.
        """
        # Check 1: the single file to delete is present on the pool-backup
        # Check 1b: the single file to delete is present on present in the s3 bucket
        if not (self._internal_backup_path / rel_file_path).exists():
            raise FileRemovalError(f"File {rel_file_path} does not exist.")

        logging.debug(f"Comparing {abs_file_path_to_check=} to {rel_file_path=} ..")

        # map e.g. '/opt/airflow/mounts/backup/test1/2024_08/test_file_SA_P123_2.raw' => 'test1/2024_08/test_file_SA_P123_2.raw'

        # old file_info key format: 'instrument1/2024_07/file.raw'
        rel_file_path_ = (self._internal_backup_path / rel_file_path).relative_to(
            get_internal_backup_path()
        )
        if str(rel_file_path_) not in self._raw_file.file_info:
            # TODO: this is a hack to support the new format. Remove it once the older DB entries have been migrated.
            # new file_info key format: 'file.raw'
            # => strip off instrument1/2025_07 from instrument1/2025_07/file.raw
            rel_file_path_ = Path(*rel_file_path_.parts[2:])

        size_in_db, hash_in_db = parse_file_info_item(
            self._raw_file.file_info[str(rel_file_path_)]
        )

        logging.debug(f"Comparing {abs_file_path_to_check=} to DB ({rel_file_path}) ..")

        # Check 2: compare the single file to delete with the DB (hash)
        # this checks that the fingerprints of the file to remove match those in the db (prevents deleting the wrong file)
        # Check 2b: compare the single file to delete with the DB (hash)
        size_to_remove = get_file_size(abs_file_path_to_check, verbose=False)
        hash_to_remove = None
        if size_to_remove != size_in_db or (
            hash_check
            and (hash_to_remove := get_file_hash(abs_file_path_to_check)) != hash_in_db
        ):
            raise FileRemovalError(
                f"File {rel_file_path} mismatch with instrument backup: {size_to_remove=} vs {size_in_db=}, {hash_to_remove=} vs {hash_in_db=}"
            )

        # Check 3: compare the single file to delete with the pool backup (hash)
        # this essentially re-checks the fingerprints that have been calculated right after file copying, would fail if pool backup was corrupted
        # Check 3b: compare the single file to delete with s3 backup (etag)
        size_on_pool_backup = get_file_size(rel_file_path, verbose=False)
        hash_on_pool_backup = None
        if size_on_pool_backup != size_in_db or (
            hash_check
            and (hash_on_pool_backup := get_file_hash(rel_file_path)) != hash_in_db
        ):
            raise FileRemovalError(
                f"File {rel_file_path} mismatch with pool backup: {size_on_pool_backup=} vs {size_in_db=}, {hash_on_pool_backup=} vs {hash_in_db=}"
            )
