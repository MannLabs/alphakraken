"""Implementation of tasks for file_remover."""

import logging
import traceback
from pathlib import Path

from airflow.models import TaskInstance
from common.keys import XComKeys
from common.settings import (
    INSTRUMENT_BACKUP_FOLDER_NAME,
    MIN_FILE_AGE_TO_REMOVE_D,
    get_internal_backup_path,
    get_internal_instrument_data_path,
)
from common.utils import get_env_variable, get_xcom, put_xcom
from file_handling import get_file_size
from raw_file_wrapper_factory import RawFileWrapperFactory, RemovePathProvider

from shared.db.interface import get_raw_file_by_id, get_raw_file_ids_older_than
from shared.keys import EnvVars


class FileRemovalError(Exception):
    """Custom exception for file check and removal errors."""


def get_raw_files_to_remove(ti: TaskInstance, **kwargs) -> None:
    """Get files to remove from the instrument backup folder."""
    del kwargs  # unused

    put_xcom(
        ti,
        XComKeys.FILES_TO_REMOVE,
        get_raw_file_ids_older_than(MIN_FILE_AGE_TO_REMOVE_D),
    )


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

    instrument_id = raw_file.instrument_id

    file_wrapper = RawFileWrapperFactory.create_copy_wrapper(
        instrument_id, raw_file, path_provider=RemovePathProvider
    )

    file_paths_to_remove: list[Path] = []
    for (
        file_path_to_remove,
        file_path_pool_backup,
    ) in file_wrapper.get_files_to_copy().items():
        _check_file(
            file_path_to_remove,
            file_path_pool_backup,
            raw_file.file_info,
        )

        logging.info(f"Marking file {file_path_to_remove} for removal .. ")

        file_paths_to_remove.append(file_path_to_remove)

    instrument_backup_path = (
        get_internal_instrument_data_path(instrument_id) / INSTRUMENT_BACKUP_FOLDER_NAME
    )
    base_file_path_to_remove = (
        instrument_backup_path / raw_file_id
    )  # TODO: get from wrapper
    _remove_files(file_paths_to_remove, base_file_path_to_remove)


def _remove_files(
    file_paths_to_remove: list[Path], base_file_path_to_remove: Path
) -> None:
    """Remove files.

    :param file_paths_to_remove: list of absolute paths to remove
    :param base_file_path_to_remove: absolute path to base file to remove (redundant to `file_paths_to_remove`
    unless it's a directory)

    :raises: FileRemovalError if removing a file fails.
    """
    if get_env_variable(EnvVars.ENV_NAME) != "production":
        logging.warning(
            f"NOT removing files {base_file_path_to_remove}, {file_paths_to_remove}: not in production."
        )
        return

    logging.info(f"removing files {base_file_path_to_remove}, {file_paths_to_remove}")
    try:
        for file_path_to_remove in file_paths_to_remove:
            f"Removing file {file_path_to_remove} .."
            file_path_to_remove.unlink()
    except Exception as e:
        raise FileRemovalError(f"Error removing {file_path_to_remove}: {e}") from e

    # special handling if `raw_file_id` is a directory
    if base_file_path_to_remove.exists() and base_file_path_to_remove.is_dir():
        try:
            _delete_empty_directory(base_file_path_to_remove)
        except Exception as e:
            raise FileRemovalError(
                f"Error removing {base_file_path_to_remove}: {e}"
            ) from e


def _check_file(
    file_path_to_remove: Path,
    file_path_pool_backup: Path,
    file_info_in_db: dict[str, tuple[float, str]],
) -> None:
    """Check that the file to remove is present in the pool backup and has the same size as in the DB.

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

    raw_file_ids = get_xcom(ti, XComKeys.FILES_TO_REMOVE)
    logging.info(f"Removing {len(raw_file_ids)} raw files: {raw_file_ids}")

    errors = []
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
                errors.append(error)
                logging.error(f"Error removing raw file {raw_file_id}:")
                logging.error(error)

    if errors:
        raise ValueError("Errors removing files.")
