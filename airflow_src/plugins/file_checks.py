"""Checks to make sure a file is the desired one."""

import logging
from pathlib import Path

from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
from common.paths import (
    get_internal_backup_path,
)
from file_handling import ETAG_SEPARATOR, get_file_hash, get_file_size
from plugins.s3.client import S3_FILE_NOT_FOUND_ETAG, get_etag
from plugins.s3.s3_utils import parse_s3_upload_path
from raw_file_wrapper_factory import CopyPathProvider

from shared.db.models import BackupStatus, RawFile, parse_file_info_item


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
    ) -> bool:
        """Check that the file to remove is present in the pool backup and has the same size and hash as in the DB.

        Here, "file" means every single file that is part of a raw file.

        We first compare sizes, then hashes, and exit early on fail to prevent unnecessary network traffic.
        Calculating hashes is the costly part: the file needs to be transferred over the network.
        Unfortunately, the check only for file size is not sufficient to unambiguously identify a file.

        :param abs_file_path_to_check: absolute path to file to remove
        :param rel_file_path: relative path to file (<-> file_info)
        :param hash_check: whether to check the hash of the file

        :returns: False if one of the checks fails or if file is not present on the pool backup, True otherwise

        :raises: KeyError if one of the files does not exist in the DB.
        """
        if not self._check_reference_exists(rel_file_path):
            return False

        size_in_db, hash_in_db = self._get_hashes(rel_file_path)

        logging.debug(f"Comparing {abs_file_path_to_check=} to {rel_file_path} ..")

        if not self._check_against_db(
            abs_file_path_to_check,
            size_in_db,
            hash_in_db,
            hash_check=hash_check,
        ):
            return False

        if not self._check_against_reference(  # noqa: SIM103
            rel_file_path, size_in_db, hash_in_db, hash_check=hash_check
        ):
            return False

        return True

    def _check_reference_exists(self, rel_file_path: Path) -> bool:
        """Check that the file to remove is present in the pool backup."""
        if not (self._internal_backup_path / rel_file_path).exists():
            logging.warning(f"File {rel_file_path} does not exist.")
            return False
        return True

    def _get_hashes(self, rel_file_path: Path) -> tuple[float, str]:
        """Get size and hash from DB for given relative file path."""
        # TODO: this is a hack to support the old format. Remove it once the older DB entries have been migrated.
        # old file_info key format: 'instrument1/2024_07/file.raw', new: 'file.raw'
        if str(rel_file_path) not in self._raw_file.file_info:
            # map e.g. '/opt/airflow/mounts/backup/test1/2024_08/test_file_SA_P123_2.raw' => 'test1/2024_08/test_file_SA_P123_2.raw'
            rel_file_path = (self._internal_backup_path / rel_file_path).relative_to(
                get_internal_backup_path()
            )

        size_in_db, hash_in_db = parse_file_info_item(
            self._raw_file.file_info[str(rel_file_path)]
        )
        if size_in_db is None or hash_in_db is None:
            raise KeyError(
                f"File {rel_file_path} has no size or hash information in DB."
            )

        return size_in_db, hash_in_db

    @staticmethod
    def _check_against_db(
        abs_file_path_to_check: Path,
        size_in_db: float,
        hash_in_db: str,
        *,
        hash_check: bool,
    ) -> bool:
        """Check that the file to remove matches the size and hash in the DB."""
        size_to_remove = get_file_size(abs_file_path_to_check, verbose=False)
        hash_to_remove = None
        if size_to_remove != size_in_db or (
            hash_check
            and (hash_to_remove := get_file_hash(abs_file_path_to_check)) != hash_in_db
        ):
            logging.warning(
                f"File mismatch with DB: {size_to_remove=} vs {size_in_db=}, {hash_to_remove=} vs {hash_in_db=}"
            )
            return False
        return True

    def _check_against_reference(
        self,
        rel_file_path: Path,
        size_in_db: float,
        hash_in_db: str,
        *,
        hash_check: bool,
    ) -> bool:
        """Check that the file to remove matches the size and hash on the reference (pool backup)."""
        # this essentially re-checks the checksums that have been calculated right before file copying, would fail if pool backup was corrupted

        reference_file_path = self._internal_backup_path / rel_file_path

        size_on_pool_backup = get_file_size(reference_file_path, verbose=False)
        hash_on_pool_backup = None
        if size_on_pool_backup != size_in_db or (
            hash_check
            and (hash_on_pool_backup := get_file_hash(reference_file_path))
            != hash_in_db
        ):
            logging.warning(
                f"File mismatch with reference (pool backup): {size_on_pool_backup=} vs {size_in_db=}, {hash_on_pool_backup=} vs {hash_in_db=}"
            )
            return False
        return True


class S3FileIdentifier(FileIdentifier):
    """File identifier that verifies files against S3 backup instead of local pool backup."""

    def __init__(self, raw_file: RawFile, s3_client: BaseAwsConnection) -> None:
        """Initialize the S3FileIdentifier with a RawFile instance and S3 client."""
        super().__init__(raw_file)
        self._s3_client = s3_client
        self._cached_head_responses: dict[str, tuple[str | object, int]] = {}

        if raw_file.s3_upload_path:
            self._bucket_name, self._key_prefix = parse_s3_upload_path(
                raw_file.s3_upload_path
            )
        else:
            self._bucket_name, self._key_prefix = "", ""

    def _get_hashes(self, rel_file_path: Path) -> tuple[float, str, str]:
        """Get size, md5, and etag from DB for given relative file path."""
        size_in_db, md5_in_db = super()._get_hashes(rel_file_path)

        file_info_key = str(rel_file_path)
        if file_info_key not in self._raw_file.file_info:
            file_info_key = str(
                (self._internal_backup_path / rel_file_path).relative_to(
                    get_internal_backup_path()
                )
            )

        file_info = self._raw_file.file_info[file_info_key]
        if len(file_info) < 3:  # noqa: PLR2004
            raise KeyError(
                f"File {rel_file_path} has no ETag information in DB (file_info has {len(file_info)} elements)."
            )

        etag_in_db = file_info[2].split(ETAG_SEPARATOR)[0]
        return size_in_db, md5_in_db, etag_in_db

    def check_file(
        self,
        abs_file_path_to_check: Path,
        rel_file_path: Path,
        *,
        hash_check: bool = True,
    ) -> bool:
        """Check that the file is present on S3 and matches the DB records."""
        if not self._check_reference_exists(rel_file_path):
            return False

        size_in_db, md5_in_db, etag_in_db = self._get_hashes(rel_file_path)

        logging.debug(f"Comparing {abs_file_path_to_check=} to {rel_file_path} ..")

        if not self._check_against_db(
            abs_file_path_to_check,
            size_in_db,
            md5_in_db,
            hash_check=hash_check,
        ):
            return False

        if not self._check_against_reference(  # noqa: SIM103
            rel_file_path, size_in_db, etag_in_db, hash_check=hash_check
        ):
            return False

        return True

    def _check_reference_exists(self, rel_file_path: Path) -> bool:
        """Check that the file exists on S3."""
        if not self._raw_file.s3_upload_path:
            logging.warning(f"File {rel_file_path}: s3_upload_path not set, skipping.")
            return False

        if self._raw_file.backup_status != BackupStatus.UPLOAD_DONE:
            logging.warning(
                f"File {rel_file_path}: backup_status is {self._raw_file.backup_status}, not UPLOAD_DONE, skipping."
            )
            return False

        s3_key = self._key_prefix + str(rel_file_path)

        try:
            etag, content_length = get_etag(self._bucket_name, s3_key, self._s3_client)
        except ValueError:
            logging.warning(
                f"S3 error checking {rel_file_path} in {self._bucket_name}/{s3_key}, skipping."
            )
            return False

        if etag is S3_FILE_NOT_FOUND_ETAG:
            logging.warning(
                f"File {rel_file_path} not found on S3 at {self._bucket_name}/{s3_key}."
            )
            return False

        self._cached_head_responses[str(rel_file_path)] = (etag, content_length)
        return True

    def _check_against_reference(
        self,
        rel_file_path: Path,
        size_in_db: float,
        hash_in_db: str,
        *,
        hash_check: bool,  # noqa: ARG002
    ) -> bool:
        """Check that the file matches the S3 backup (etag and size)."""
        s3_etag, s3_content_length = self._cached_head_responses[str(rel_file_path)]

        if s3_content_length != int(size_in_db):
            logging.warning(
                f"File mismatch with S3: {s3_content_length=} vs {size_in_db=}"
            )
            return False

        if s3_etag != hash_in_db:
            logging.warning(f"File mismatch with S3: {s3_etag=} vs {hash_in_db=}")
            return False

        return True


def create_file_identifier(
    raw_file: RawFile,
    *,
    verify_against_s3: bool,
    s3_client: BaseAwsConnection | None = None,
) -> FileIdentifier:
    """Create the appropriate file identifier based on the verification mode."""
    if verify_against_s3:
        if s3_client is None:
            raise ValueError("verify_against_s3=True requires an s3_client")
        return S3FileIdentifier(raw_file, s3_client)
    return FileIdentifier(raw_file)
