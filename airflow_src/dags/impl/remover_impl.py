"""Implementation of tasks for file_remover."""

import logging
import shutil
import traceback
from collections import defaultdict
from pathlib import Path

from airflow.models import TaskInstance
from common.keys import AirflowVars, XComKeys
from common.settings import (
    BYTES_TO_GB,
    DEFAULT_MAX_FILE_AGE_TO_REMOVE_D,
    DEFAULT_MIN_FILE_AGE_TO_REMOVE_D,
    INSTRUMENTS,
    get_internal_backup_path,
    get_internal_instrument_data_path,
)
from common.utils import get_airflow_variable, get_env_variable, get_xcom, put_xcom
from file_handling import get_file_size
from raw_file_wrapper_factory import RawFileWrapperFactory, RemovePathProvider

from shared.db.interface import get_raw_file_by_id, get_raw_files_by_age
from shared.keys import EnvVars


class FileRemovalError(Exception):
    """Custom exception for file check and removal errors."""


def get_raw_files_to_remove(ti: TaskInstance, **kwargs) -> None:
    """Get files to remove from the instrument backup folder."""
    del kwargs  # unused

    min_free_gb = int(get_airflow_variable(AirflowVars.MIN_FREE_SPACE_GB, "-1"))

    min_file_age = int(
        get_airflow_variable(
            AirflowVars.MIN_FILE_AGE_TO_REMOVE_IN_DAYS, DEFAULT_MIN_FILE_AGE_TO_REMOVE_D
        )
    )
    if min_free_gb > 0:
        raw_file_ids_to_remove = _decide_on_raw_files_to_remove(
            min_free_gb, min_file_age, INSTRUMENTS.keys()
        )
    else:
        logging.warning(f"Skipping: {AirflowVars.MIN_FREE_SPACE_GB} not set.")
        raw_file_ids_to_remove = {}

    for instrument_id, files_to_remove in raw_file_ids_to_remove.items():
        logging.info(
            f"Removing for {instrument_id} {len(files_to_remove)} files: {files_to_remove}"
        )

    put_xcom(
        ti,
        XComKeys.FILES_TO_REMOVE,
        raw_file_ids_to_remove,
    )


def _decide_on_raw_files_to_remove(
    min_free_gb: int, min_file_age: int, instrument_ids: list[str]
) -> dict[str, list[str]]:
    """Get raw files to remove from the instrument backup folder from the DB.

    :param min_free_gb: minimum free space in GB to keep on the instrument
    :param min_file_age: minimum age of files to remove in days

    :return: dict with instrument_id as key and list of raw file ids to remove as value

    For each instrument, get as many files (oldest first) as are required to free up the desired disk space.
    """
    raw_file_ids_to_remove = defaultdict(list)

    for instrument_id in instrument_ids:
        instrument_path = get_internal_instrument_data_path(instrument_id)

        if not instrument_path.exists():
            logging.warning(f"Skipping {instrument_id}: path does not exist.")
            continue

        total_bytes, used_bytes, free_bytes = shutil.disk_usage(instrument_path)
        total_gb, used_gb, free_gb = (
            total_bytes * BYTES_TO_GB,
            used_bytes * BYTES_TO_GB,
            free_bytes * BYTES_TO_GB,
        )
        # TODO: security check: <= 30 % ?

        logging.info(f"{instrument_id=} {total_gb=} {used_gb=} {free_gb=}")

        min_space_to_free_gb = min_free_gb - free_gb
        if min_space_to_free_gb <= 0:
            logging.info(
                f"Skipping {instrument_id}: free space {free_gb} GB is already above minimum ({min_free_gb} GB)."
            )
            continue

        raw_files = get_raw_files_by_age(
            instrument_id,
            min_age_in_days=min_file_age,
            max_age_in_days=DEFAULT_MAX_FILE_AGE_TO_REMOVE_D,
        )
        logging.info(
            f"{instrument_id}: found {len(raw_files)} files as candidates for removal: "
            f"{[(r.id, r.size, r.created_at) for r in raw_files]}"
        )

        sum_size_gb = 0
        for raw_file in raw_files:
            if raw_file.size is None:
                logging.warning(f"Skipping {raw_file.id}: size is None.")
                continue
            sum_size_gb += raw_file.size * BYTES_TO_GB
            raw_file_ids_to_remove[instrument_id].append(raw_file.id)

            logging.info(f"Adding {raw_file.id=} {raw_file.size=} {sum_size_gb=}")
            if sum_size_gb >= min_space_to_free_gb:
                break
        else:
            logging.warning(f"Not enough files to remove, got only {sum_size_gb=}")

    return raw_file_ids_to_remove


def _safe_remove_files(raw_file_id: str) -> None:
    """Delete raw data from instrument Backup folder.

    :param raw_file_id: ID of the raw file to delete

    :raises: FileRemovalError if one of the checks or deletions fails

    To avoid mistakes in deletion, this method works very defensively:
    - takes as ground truth the contents of the instrument Backup folder (i.e. the files that will be actually deleted)
        to account for files that have been added to it manually
    - for each single file to delete
        - compares* it to the corresponding file in the pool backup folder (to verify that the backup was successful)
        - compares* it to the corresponding file in the DB (to verify that it is actually the file that should be deleted)
    - only if all checks pass for all files associated with a raw file, the file is deleted.

    *Note: To avoid extra network traffic, a size comparison instead of hash comparison is used for these checks.
        Given that the instrument Backup folder contains only files that have passed a hash check against the
        pool backup, this should be sufficient.
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

    if file_paths_to_remove:
        _remove_files(file_paths_to_remove)

    if (
        base_raw_file_path_to_remove := remove_wrapper.get_folder_to_remove()
    ) is not None:
        _remove_folder(base_raw_file_path_to_remove)


def _remove_files(
    file_paths_to_remove: list[Path],
) -> None:
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
            f"Removing file {file_path_to_remove} .."
            file_path_to_remove.unlink()
    except Exception as e:
        raise FileRemovalError(f"Error removing {file_path_to_remove}: {e}") from e


def _remove_folder(folder_path_to_remove: Path | None) -> None:
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
) -> None:
    """Check that the file to remove is present in the pool backup and has the same size as in the DB.

    Here, "file" means every single file that is part of a raw file.

    :param file_path_to_remove: absolute path to file to remove
    :param file_path_pool_backup: absolute path to location of file in pool backup
    :param file_info_in_db: dict with file info from DB

    :raises: FileCheckError if one of the checks fails.
    """
    logging.info(
        f"Comparing {file_path_to_remove=} to {file_path_pool_backup=} with {file_info_in_db=}"
    )
    size_on_instrument = get_file_size(file_path_to_remove, verbose=False)

    # Check 1: the single file to delete is present on the pool-backup
    logging.info(f"Comparing {file_path_to_remove=} to {file_path_pool_backup=} ..")
    if size_on_instrument != (
        size_in_pool_backup := get_file_size(file_path_pool_backup, verbose=False)
    ):
        raise FileRemovalError(
            f"File {file_path_to_remove} mismatch with pool backup: {size_in_pool_backup=} vs {size_on_instrument=}"
        )

    # Check 2: single file to delete is the one we have in the DB

    # /opt/airflow/mounts/backup/test1/2024_08/test_file_SA_P1_2.raw => test1/2024_08/test_file_SA_P1_2.raw
    rel_file_path = str(file_path_pool_backup.relative_to(get_internal_backup_path()))
    logging.info(f"Comparing {file_path_to_remove=} to DB ({rel_file_path}) ..")
    if size_on_instrument != (
        size_in_db := file_info_in_db.get(rel_file_path)[
            0
        ]  # first element of tuple (size, hash)
    ):
        raise FileRemovalError(
            f"File {rel_file_path} mismatch with instrument backup: {size_on_instrument=} vs {size_in_db=}"
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

    raw_file_ids_to_remove = get_xcom(ti, XComKeys.FILES_TO_REMOVE)

    errors = defaultdict(list)
    for instrument_id, raw_file_ids in raw_file_ids_to_remove.items():
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

        raise ValueError("Errors removing files.")
