"""Utility functions for handling files."""

import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

import pytz
from airflow.exceptions import AirflowFailException
from common.settings import get_internal_instrument_data_path


def get_file_creation_timestamp(raw_file_name: str, instrument_id: str) -> float:
    """Get the creation timestamp (unix epoch) of a raw file.

    Note that the results of this method will be compared for one file across different file systems,
    so make sure the results are file system independent.
    """
    raw_file_path = get_internal_instrument_data_path(instrument_id) / raw_file_name
    file_creation_ts = raw_file_path.stat().st_ctime
    logging.info(
        f"File {raw_file_name} has {file_creation_ts=} {datetime.fromtimestamp(file_creation_ts, tz=pytz.UTC)}"
    )
    return file_creation_ts


def get_file_size(file_path: Path) -> float:
    """Get the size (in bytes) of a file.

    Note that the results of this method will be compared for one file across different file systems,
    so make sure the results are file system independent.
    """
    file_size_bytes = file_path.stat().st_size
    logging.info(f"File {file_path} has {file_size_bytes=}")
    return file_size_bytes


def _get_file_hash(file_path: Path, chunk_size: int = 8192) -> str:
    """Get the hash of a file."""
    with file_path.open("rb") as f:
        file_hash = hashlib.md5()  # noqa: S324
        while chunk := f.read(chunk_size):
            file_hash.update(chunk)
    logging.info(f"Hash of {file_path} is {file_hash.hexdigest()}")
    return file_hash.hexdigest()


def _file_already_exists(dst_path: Path, src_hash: str) -> bool:
    """Check if a file already exists in the backup location and has the same hash."""
    if dst_path.exists():
        logging.info("File already exists in backup location. Checking hash ..")
        if _get_file_hash(dst_path) == src_hash:
            logging.info("Hashes match.")
            return True
        logging.warning("Hashes do not match.")
    return False


def copy_file(
    src_path: Path,
    dst_path: Path,
) -> None:
    """Copy a raw file to the backup location and check its hashsum."""
    logging.info(f"Calculating hash for {src_path} ..")
    start = datetime.now()  # noqa: DTZ005
    src_hash = _get_file_hash(src_path)
    time_elapsed = (datetime.now() - start).total_seconds()  # noqa: DTZ005
    logging.info(f"Hash calculated. Time elapsed: {time_elapsed/60:.1f} min")
    if _file_already_exists(dst_path, src_hash):
        return
    if not dst_path.parent.exists():
        logging.info(f"Creating parent directories for {dst_path} ..")
        dst_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(f"Copying {src_path} to {dst_path} ..")
    start = datetime.now()  # noqa: DTZ005
    shutil.copy2(src_path, dst_path)
    time_elapsed = (datetime.now() - start).total_seconds()  # noqa: DTZ005
    dst_size = get_file_size(dst_path)
    logging.info(
        f"Copying done. Time elapsed: {time_elapsed/60:.1f} min at {dst_size / max(time_elapsed, 1) / 1024 ** 2:.1f} MB/s"
    )

    logging.info("Verifying hash ..")
    if (hash_dst := _get_file_hash(dst_path)) != (src_hash):
        raise ValueError(f"Hashes do not match ofter copy! {src_hash=} != {hash_dst=}")
    logging.info("Verifying hash done!")


def compare_paths(
    source_path: Path, target_path: Path
) -> tuple[list[str], list[str], list[str]]:
    """Recursively compare items in source_path and target_path.

    :param source_path: Path to the source file or directory.
    :param target_path: Path to the target file or directory.

    Returns a tuple of lists of strings containing the relative paths for:
        - missing_files: files/folders that are missing in target_path
        - different_files: files/folders that have a different hash sum in target_path
        - items_only_in_target: files/folders that are only in target_path
    """
    if source_path.is_dir() and not target_path.is_dir():
        raise AirflowFailException(
            f"Source {source_path} is a directory but target {target_path} is not."
        )

    missing_items = []
    different_items = []

    source_items = (
        list(source_path.rglob("*")) if source_path.is_dir() else [source_path]
    )

    for source_item in source_items:
        if source_path.is_dir():
            source_item_relative_path = source_item.relative_to(source_path)
            target_item_path = target_path / source_item_relative_path
        else:
            source_item_relative_path = source_item.name
            target_item_path = target_path

        if not target_item_path.exists():
            missing_items.append(str(source_item_relative_path))
            continue

        if source_item.is_dir():
            # no hashsum check for directories
            continue

        source_hash = _get_file_hash(source_item)
        target_hash = _get_file_hash(target_item_path)

        if source_hash != target_hash:
            different_items.append(str(source_item_relative_path))

    items_only_in_target = (
        [
            str(p)
            for p in (
                _get_relative_paths(target_path) - _get_relative_paths(source_path)
            )
        ]
        if source_path.is_dir()
        else []
    )

    return missing_items, different_items, items_only_in_target


def _get_relative_paths(dir_path: Path) -> set[Path]:
    """Get relative paths of all files in a directory."""
    return {file_path.relative_to(dir_path) for file_path in dir_path.rglob("*")}
