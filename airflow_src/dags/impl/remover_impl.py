"""Implementation of tasks for file_remover."""

import logging
import traceback
from collections import defaultdict
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.constants import (
    BYTES_TO_GB,
    DEFAULT_MAX_FILE_AGE_TO_REMOVE_D,
    DEFAULT_MIN_FILE_AGE_TO_REMOVE_D,
)
from common.keys import AirflowVars, Tasks, XComKeys
from common.paths import (
    get_internal_backup_path,
    get_internal_instrument_data_path,
)
from common.utils import get_airflow_variable, get_env_variable, get_xcom, put_xcom
from common.yaml_wrapper import get_instrument_ids
from file_handling import get_disk_usage, get_file_hash, get_file_size
from raw_file_wrapper_factory import RawFileWrapperFactory, RemovePathProvider

from shared.db.interface import get_raw_file_by_id, get_raw_files_by_age
from shared.db.models import RawFile
from shared.keys import EnvVars


class FileRemovalError(Exception):
    """Custom exception for file check and removal errors."""


def get_raw_files_to_remove(ti: TaskInstance, **kwargs) -> None:
    """Get files to remove from the instrument backup folder."""
    del kwargs  # unused

    min_file_age = int(
        get_airflow_variable(
            AirflowVars.MIN_FILE_AGE_TO_REMOVE_IN_DAYS, DEFAULT_MIN_FILE_AGE_TO_REMOVE_D
        )
    )

    min_free_space_gb = int(get_airflow_variable(AirflowVars.MIN_FREE_SPACE_GB, "-1"))

    raw_file_ids_to_remove = {}
    instruments_with_errors = []
    if min_free_space_gb <= 0:
        logging.warning(f"Skipping: {AirflowVars.MIN_FREE_SPACE_GB} not set.")
    else:
        for instrument_id in get_instrument_ids():
            try:
                raw_file_ids_to_remove[instrument_id] = _decide_on_raw_files_to_remove(
                    instrument_id,
                    min_free_gb=min_free_space_gb,
                    min_file_age=min_file_age,
                )
                logging.info(
                    f"Removing for {instrument_id} {len(raw_file_ids_to_remove[instrument_id])} files: {raw_file_ids_to_remove[instrument_id]}"
                )
            except Exception:  # noqa: PERF203
                # catch all errors to avoid one instrument blocking all others
                logging.exception(f"Error for {instrument_id}.")
                instruments_with_errors.append(instrument_id)

    put_xcom(
        ti,
        XComKeys.FILES_TO_REMOVE,
        raw_file_ids_to_remove,
    )

    put_xcom(
        ti,
        XComKeys.INSTRUMENTS_WITH_ERRORS,
        instruments_with_errors,
    )


def _decide_on_raw_files_to_remove(
    instrument_id: str,
    *,
    min_free_gb: int,
    min_file_age: int,
) -> list[str]:
    """Get from the DB the raw files to remove from the instrument backup folder and select as many as needed.

    :param instrument_id: ID of the instrument
    :param min_free_gb: minimum free space in GB to keep on the instrument
    :param min_file_age: minimum age of files to remove in days

    :return: dict with instrument_id as key and list of raw file ids to remove as value

    For the given instrument, get as many files (oldest first) as are required to free up the desired disk space.
    """
    raw_file_ids_to_remove = []

    instrument_path = get_internal_instrument_data_path(instrument_id)

    if not instrument_path.exists():
        logging.warning(f"Skipping {instrument_id}: path does not exist.")
        return raw_file_ids_to_remove

    total_gb, used_gb, free_gb = get_disk_usage(instrument_path)
    logging.info(f"{instrument_id=} {total_gb=} {used_gb=} {free_gb=}")

    min_space_to_free_gb = min_free_gb - free_gb
    if min_space_to_free_gb <= 0:
        logging.info(
            f"Skipping {instrument_id}: free space {free_gb} GB is already above minimum ({min_free_gb} GB)."
        )
        return raw_file_ids_to_remove

    raw_files = get_raw_files_by_age(
        instrument_id,
        min_age_in_days=min_file_age,
        max_age_in_days=DEFAULT_MAX_FILE_AGE_TO_REMOVE_D,
    )

    sum_size_gb = 0
    for raw_file in raw_files:
        logging.info(
            f"Checking {raw_file.id=} created_at={raw_file.created_at.strftime('%Y-%m-%d %H-%M-%S:%f')}"
        )

        try:
            total_size, num_files = _get_total_size(raw_file)
        except FileRemovalError as e:
            logging.warning(f"Skipping {raw_file.id}: {e}")
            continue

        if not num_files:
            logging.info(f"Skipping {raw_file.id}: no files found to remove")
            continue

        logging.info(f"Adding {raw_file.id=} {total_size=} bytes {sum_size_gb=}")
        raw_file_ids_to_remove.append(raw_file.id)
        sum_size_gb += total_size * BYTES_TO_GB
        if sum_size_gb >= min_space_to_free_gb:
            logging.info("Got enough files.")
            break
    else:
        logging.warning(f"Not enough files to remove, got only {sum_size_gb=}")

    return raw_file_ids_to_remove


def _get_total_size(raw_file: RawFile) -> tuple[float, int]:
    """Return total size of all files associated with a raw_file that are actually on the disk.

    This calculates the total size of all files associated with a raw file that are actually on the disk.
    This is important, as there could be cases where some files for a raw file have been already removed from the disk,
    which would overestimate the total size gain if this data was removed.
    """
    remove_wrapper = RawFileWrapperFactory.create_write_wrapper(
        raw_file, path_provider=RemovePathProvider
    )

    files_to_remove = remove_wrapper.get_files_to_remove()

    total_size_bytes = 0.0
    num_files = 0

    for (
        file_path_to_remove,
        file_path_pool_backup,
    ) in files_to_remove.items():
        if not file_path_to_remove.exists():
            continue  # file was already removed

        _check_file(
            file_path_to_remove,
            file_path_pool_backup,
            raw_file.file_info,
            hash_check=False,  # we only want to check the sizes here
        )

        total_size_bytes += get_file_size(file_path_to_remove, verbose=False)
        num_files += 1

    return total_size_bytes, num_files


def _safe_remove_files(raw_file_id: str) -> None:
    """Delete raw data from instrument Backup folder.

    :param raw_file_id: ID of the raw file to delete

    :raises: FileRemovalError if one of the checks or deletions fails

    To avoid mistakes in deletion, this method works very defensively:
    - takes as ground truth the contents of the instrument Backup folder (i.e. the files that will be actually deleted)
        to account for files that have been added to it manually
    - for each single file to delete
        - compares it to the corresponding file in the DB (to verify that it is actually the file that should be deleted)
        - compares it to the corresponding file in the pool backup folder (to verify that the backup was successful)
    - only if all checks pass for all files associated with a raw file, those files are deleted.
    """
    raw_file = get_raw_file_by_id(raw_file_id)

    remove_wrapper = RawFileWrapperFactory.create_write_wrapper(
        raw_file, path_provider=RemovePathProvider
    )

    file_paths_to_remove: list[Path] = []
    for (
        file_path_to_remove,
        file_path_pool_backup,
    ) in remove_wrapper.get_files_to_remove().items():
        if not file_path_to_remove.exists():
            logging.info(
                f"File {file_path_to_remove} does not exist. Presuming it was already removed."
            )
            continue

        _check_file(
            file_path_to_remove,
            file_path_pool_backup,
            raw_file.file_info,
        )

        logging.info(f"Marking file {file_path_to_remove} for removal .. ")

        file_paths_to_remove.append(file_path_to_remove)

    base_raw_file_path_to_remove = remove_wrapper.get_folder_to_remove()

    if file_paths_to_remove:
        if base_raw_file_path_to_remove is not None:  # Bruker case
            _change_folder_permissions(base_raw_file_path_to_remove)

        _remove_files(file_paths_to_remove)

    if base_raw_file_path_to_remove is not None:  # Bruker case
        _remove_folder(base_raw_file_path_to_remove)


def _change_folder_permissions(base_raw_file_path_to_remove: Path) -> None:
    """Make all subfolders in base_raw_file_path_to_remove writeable.

    For reasons known only to Bruker, the .m subfolder carrying the methods is not writeable by default (permissions dr-xr-xr-x)
    """
    for sub_path in base_raw_file_path_to_remove.rglob("*"):
        if sub_path.is_dir():
            logging.info(f"Making {sub_path} writeable..")
            sub_path.chmod(0o775)


def _remove_files(file_paths_to_remove: list[Path]) -> None:
    """Remove files.

    :param file_paths_to_remove: list of absolute file paths to remove

    :raises: FileRemovalError if removing a file fails.
    """
    if get_env_variable(EnvVars.ENV_NAME) != "production":
        logging.warning(
            f"NOT removing files {file_paths_to_remove}: not in production."
        )
        return

    logging.info(f"removing files {file_paths_to_remove}")
    try:
        for file_path_to_remove in file_paths_to_remove:
            logging.info(f"Removing file {file_path_to_remove} ..")
            file_path_to_remove.unlink()
    except Exception as e:
        raise FileRemovalError(f"Error removing {file_path_to_remove}: {e}") from e


def _remove_folder(folder_path_to_remove: Path) -> None:
    """Remove folder.

    :param folder_path_to_remove: absolute path to raw data folder to remove

    :raises: FileRemovalError if removing a file fails.
    """
    if folder_path_to_remove.exists() and folder_path_to_remove.is_dir():
        if get_env_variable(EnvVars.ENV_NAME) != "production":
            logging.warning(
                f"NOT removing folder {folder_path_to_remove}: not in production."
            )
            return

        try:
            _delete_empty_directory(folder_path_to_remove)
        except Exception as e:
            raise FileRemovalError(
                f"Error removing {folder_path_to_remove}: {e}"
            ) from e


def _check_file(
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
    """
    # Check 1: the single file to delete is present on the pool-backup
    if not file_path_pool_backup.exists():
        raise FileRemovalError(f"File {file_path_pool_backup} does not exist.")

    logging.debug(f"Comparing {file_path_to_remove=} to {file_path_pool_backup=} ..")

    # /opt/airflow/mounts/backup/test1/2024_08/test_file_SA_P123_2.raw => test1/2024_08/test_file_SA_P123_2.raw
    rel_file_path = str(file_path_pool_backup.relative_to(get_internal_backup_path()))
    size_in_db, hash_in_db = file_info_in_db.get(rel_file_path)

    logging.debug(f"Comparing {file_path_to_remove=} to DB ({rel_file_path}) ..")

    # Check 2: compare the single file to delete with the DB (hash)
    # this checks that the fingerprints of the file to remove match those in the db (prevents deleting the wrong file)
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
    size_on_pool_backup = get_file_size(file_path_pool_backup, verbose=False)
    hash_on_pool_backup = None
    if size_on_pool_backup != size_in_db or (
        hash_check
        and (hash_on_pool_backup := get_file_hash(file_path_pool_backup)) != hash_in_db
    ):
        raise FileRemovalError(
            f"File {rel_file_path} mismatch with pool backup: {size_on_pool_backup=} vs {size_in_db=}, {hash_on_pool_backup=} vs {hash_in_db=}"
        )


def _delete_empty_directory(directory_path: Path) -> None:
    """Recursively delete directory if it is empty or only contains empty directories."""
    logging.info(f"Deleting directory {directory_path} ..")
    for sub_path in directory_path.glob("*"):
        if sub_path.is_dir():
            _delete_empty_directory(sub_path)
    directory_path.rmdir()  # rmdir only removes empty directories


def remove_raw_files(ti: TaskInstance, **kwargs) -> None:
    """Remove files/folders from the instrument backup folder."""
    del kwargs  # unused

    raw_file_ids_to_remove = get_xcom(
        ti, XComKeys.FILES_TO_REMOVE
    )  # pytype: disable=attribute-error

    errors = defaultdict(list)
    for (
        instrument_id,
        raw_file_ids,
    ) in raw_file_ids_to_remove.items():  # pytype: disable=attribute-error
        logging.info(
            f"Removing for {instrument_id} {len(raw_file_ids)} files: {raw_file_ids}"
        )
        for raw_file_id in raw_file_ids:
            error = None
            try:
                _safe_remove_files(raw_file_id)
            except FileRemovalError as e:
                error = f"Error: {e}"
            except Exception as e:  # noqa: BLE001
                error = f"Unknown error: {e} {traceback.format_exc()}"
            finally:
                if error:
                    msg = f"Error for {raw_file_id}: {error}"
                    errors[instrument_id].append(msg)
                    logging.error(error)

    if errors:
        for instrument_id, error_list in errors.items():
            errors_pretty = "\n  - ".join(error_list)
            logging.error(
                f"Errors removing files for {instrument_id}:\n{errors_pretty}\n\n"
            )

        raise AirflowFailException("Errors removing files.")

    # Fail in case raw file selection failed in the upstream task to make these errors transparent in Airflow UI:
    if instruments_with_errors := get_xcom(ti, XComKeys.INSTRUMENTS_WITH_ERRORS):
        raise AirflowFailException(
            f"Error in previous task {Tasks.GET_RAW_FILES_TO_REMOVE} for {instruments_with_errors=}. Check the logs there."
        )
