"""Utility functions for handling files."""

import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

import pytz
from airflow.exceptions import AirflowFailException
from common.constants import BYTES_TO_GB, BYTES_TO_MB
from common.keys import AirflowVars
from common.paths import get_internal_instrument_data_path


class HashMismatchError(Exception):
    """Custom exception for hash mismatch errors."""


def get_file_ctime(path: Path) -> float:
    """Get the creation timestamp (unix epoch, unit: seconds) of a file."""
    return path.stat().st_ctime


def get_file_creation_timestamp(
    raw_file_name: str, instrument_id: str, *, verbose: bool = True
) -> float:
    """Get the creation timestamp (unix epoch) of a raw file.

    Note that the results of this method will be compared for one file across different file systems,
    so make sure the results are file system independent.
    """
    raw_file_path = get_internal_instrument_data_path(instrument_id) / raw_file_name
    file_creation_ts = get_file_ctime(raw_file_path)
    logging.info(
        f"File {raw_file_name} has {file_creation_ts=} {datetime.fromtimestamp(file_creation_ts, tz=pytz.UTC)}"
    ) if verbose else None
    return file_creation_ts


def get_file_size(
    file_path: Path, default: int | None = None, *, verbose: bool = True
) -> float:
    """Get the size (in bytes) of a file.

    Note that the results of this method will be compared for one file across different file systems,
    so make sure the results are file system independent.

    An optional default value can be provided in case the file does not exist.
    """
    if file_path.exists():
        file_size_bytes = file_path.stat().st_size
    else:
        if default is not None:
            logging.info(
                f"File {file_path} not found, returning {default=}"
            ) if verbose else None
            return default
        raise FileNotFoundError(f"File {file_path} not found.")
    file_size_mb = file_size_bytes * BYTES_TO_MB
    logging.info(
        f"File {file_path} has {file_size_bytes=} ({file_size_mb:.2f} MB)"
    ) if verbose else None
    return file_size_bytes


def get_disk_usage(path: Path) -> tuple[float, float, float]:
    """Get the disk space (total, used, free) of a path in GB."""
    total_bytes, used_bytes, free_bytes = shutil.disk_usage(path)
    total_gb, used_gb, free_gb = (
        total_bytes * BYTES_TO_GB,
        used_bytes * BYTES_TO_GB,
        free_bytes * BYTES_TO_GB,
    )
    return total_gb, used_gb, free_gb


def get_file_hash(
    file_path: Path,
    chunk_size_mb: int = 8,
    *,
    verbose: bool = True,
) -> str:
    """Get the hash of a file.

    Wrapper for get_file_hash_with_etag()

    This operation is expensive for large files and/or if transferred over a network.
    :param file_path: Path to the file.
    :param chunk_size_mb: Size of chunks to read the file in Megabytes.
    :param verbose: Whether to log progress information.

    :return: the MD5 hash
    """
    return get_file_hash_with_etag(
        file_path,
        chunk_size_mb,
        calculate_etag=False,
        verbose=verbose,
    )[0]


def get_file_hash_with_etag(
    file_path: Path,
    chunk_size_mb: int,
    *,
    calculate_etag: bool = False,
    verbose: bool = True,
) -> tuple[str, str]:
    """Get the hash of a file with optional etag.

    This operation is expensive for large files and/or if transferred over a network.
    :param file_path: Path to the file.
    :param chunk_size_mb: Size of chunks to read the file in Megabytes.
    :param calculate_etag: Whether to calculate the etag (for multipart uploads).
    :param verbose: Whether to log progress information.

    :return: A tuple containing the MD5 hash and the ETag of the file. The latter is empty if calculate_etag is False
    """
    if verbose:
        start = datetime.now()  # noqa: DTZ005
        file_size = get_file_size(file_path, verbose=False)
        logging.info(f"Calculating hash of {file_path} ({file_size=})..")

    chunk_size_bytes = chunk_size_mb * 1024 * 1024
    md5_hashes = []
    with file_path.open("rb") as f:
        file_hash = hashlib.md5()  # noqa: S324 hashlib-insecure-hash-function
        while chunk := f.read(chunk_size_bytes):
            file_hash.update(chunk)

            if calculate_etag:
                md5_hashes.append(hashlib.md5(chunk).digest())  # noqa: S324

    md5sum = file_hash.hexdigest()
    etag = _md5hashes_to_etag(md5_hashes, chunk_size_mb) if calculate_etag else ""

    if verbose:
        file_size = get_file_size(
            file_path, verbose=False
        )  # deliberately calling a second time in case the file was modified during the hash calculation
        time_elapsed = (datetime.now() - start).total_seconds()  # noqa: DTZ005
        logging.info(
            f".. {md5sum=} {etag=} ({file_size=}) Time elapsed: {time_elapsed / 60:.1f} min"
        )

    return md5sum, etag


# separator between ETag and chunk size
ETAG_SEPARATOR = "__"


def _md5hashes_to_etag(md5_hashes: list[bytes], chunk_size_mb: int) -> str:
    """Convert a list of MD5 hashes to an S3 ETag format.

    The chunk size is appended as a prefix with a '__' separator. This is because the ETag depends
    on the chunk size used during multipart upload, so we need to keep track of the chunk size.
    """
    if len(md5_hashes) == 0:
        # Empty file
        value = hashlib.md5(b"").hexdigest()  # noqa: S324  # this is d41d8cd98f00b204e9800998ecf8427e
    elif len(md5_hashes) == 1:
        # Single part - just MD5 without part count
        value = md5_hashes[0].hex()
    else:
        # Multipart - MD5 of concatenated hashes with part count
        combined_hash = hashlib.md5(b"".join(md5_hashes)).hexdigest()  # noqa: S324
        value = f"{combined_hash}-{len(md5_hashes)}"

    return f"{value}{ETAG_SEPARATOR}{chunk_size_mb!s}"


def _identical_copy_exists(dst_path: Path, src_hash: str) -> bool:
    """Check if a file already exists in `dst_path` and has the same hash.

    :param dst_path: Path to the destination file.
    :param src_hash: Hash of the source file.

    :return: True if an identical copy exists, False otherwise.
    :raises ValueError: If the hash of the existing file does not match the source hash.
    """
    logging.info(f"Checking if file already exists in {dst_path} ..")
    if dst_path.exists():
        logging.info("File already exists in backup location. Checking hash ..")
        if (dst_hash := get_file_hash(dst_path)) == src_hash:
            logging.info(f"Hashes match {src_hash=}.")
            return True
        raise HashMismatchError(f"Hash mismatch: {src_hash=} {dst_hash=}")
    return False


def copy_file(
    src_path: Path,
    dst_path: Path,
    src_hash: str,
) -> tuple[float, str]:
    """Copy a single file from `src_path` to `dst_path` and check its hashsum.

    :param src_path: Path to the source file.
    :param dst_path: Path to the destination file.
    :param src_hash: Hash of the source file.

    :raises AirflowFailException: If the hash of the copied file does not match the source hash

    :return: A tuple containing the size and hash of the copied file.
    """
    if not dst_path.parent.exists():
        logging.info(f"Creating parent directories for {dst_path} ..")
        dst_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(f"Copying {src_path} to {dst_path} ..")
    start = datetime.now()  # noqa: DTZ005
    shutil.copy2(src_path, dst_path)
    time_elapsed = (datetime.now() - start).total_seconds()  # noqa: DTZ005

    dst_size = get_file_size(dst_path)
    logging.info(".. copying done.")
    logging.info(
        f"Time elapsed: {time_elapsed / 60:.1f} min at {dst_size * BYTES_TO_MB / max(time_elapsed, 0.00001):.1f} MB/s"
    )

    logging.info("Verifying hash ..")
    if (dst_hash := get_file_hash(dst_path)) != src_hash:
        src_size = get_file_size(src_path)
        raise AirflowFailException(
            f"Hashes do not match after copy: {src_hash=} {dst_hash=} (sizes: {dst_size=} {src_size=})"
        )
    logging.info(".. verifying done")

    return dst_size, dst_hash


def _decide_if_copy_required(
    src_path: Path, dst_path: Path, src_hash: str, *, overwrite: bool
) -> bool:
    """Decide if a copy operation is required.

    :param src_path: Path to the source file.
    :param dst_path: Path to the destination file.
    :param src_hash: Hash of the source file.
    :param overwrite: Whether to overwrite the file if it already exists with a different hash in the destination.

    :return: a boolean indicating whether a copy is required
    :raises AirflowFailException: If the file already exists with a different hash and `overwrite=False`
    """
    try:
        copy_required = not _identical_copy_exists(dst_path, src_hash)
    except HashMismatchError as e:
        src_size = get_file_size(src_path, verbose=False)
        dst_size = get_file_size(dst_path, verbose=False)
        msg = f"File {dst_path} exists with different hash."
        logging.warning(f"{msg} {e} {src_size=} {dst_size=}")
        if overwrite:
            logging.warning("Will overwrite file.")
            copy_required = True
        else:
            logging.exception(
                f"{msg}\n"
                "This might be due to a previous copy operation being interrupted. \n"
                "To resolve this issue: \n"
                "1. Check and remove the file from backup if necessary, then restart this task, or \n"
                f"2. Set the Airflow Variable {AirflowVars.BACKUP_OVERWRITE_FILE_ID} to the ID of the raw file to force overwrite."
            )

            raise AirflowFailException(msg) from e
    return copy_required


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
    source_path_is_dir = source_path.is_dir()
    if source_path_is_dir and not target_path.is_dir():
        raise AirflowFailException(
            f"Source {source_path} is a directory but target {target_path} is not."
        )

    missing_items = []
    different_items = []

    source_items = list(source_path.rglob("*")) if source_path_is_dir else [source_path]

    for source_item in source_items:
        if source_path_is_dir:
            source_item_relative_path = source_item.relative_to(source_path)
            target_item_path = target_path / source_item_relative_path
        else:
            source_item_relative_path = source_item.name
            target_item_path = target_path

        if not target_item_path.exists():
            missing_items.append(str(source_item_relative_path))
            continue

        if source_item.is_dir():
            # no comparison for directories
            continue

        source_hash = get_file_hash(source_item)
        target_hash = get_file_hash(target_item_path)

        if source_hash != target_hash:
            different_items.append(str(source_item_relative_path))

    items_only_in_target = (
        _get_relative_paths(target_path) - _get_relative_paths(source_path)
        if source_path_is_dir
        else {}
    )

    return missing_items, different_items, [str(p) for p in items_only_in_target]


def _get_relative_paths(dir_path: Path) -> set[Path]:
    """Get relative paths of all files in a directory."""
    return {file_path.relative_to(dir_path) for file_path in dir_path.rglob("*")}


def move_existing_file(file_path: Path, suffix: str = ".alphakraken.bkp") -> Path:
    """Move existing file to a new name with an incrementing number.

    Parameters
    ----------
    file_path : Path
        Path to the file that needs to be backed up
    suffix : str, default '.alphakraken.bkp'
        Suffix to add to the backup file name before the incrementing number

    Returns
    -------
    str
        Path of the moved file if it was moved, path to the original file otherwise

    """
    old_path = file_path
    new_path = old_path

    n = -1
    while new_path.exists():
        n += 1
        new_path = old_path.parent / f"{old_path.stem}{old_path.suffix}.{n}{suffix}"

    if n != -1:
        file_path.rename(new_path)
        logging.warning(f"Moved existing file {old_path} to {new_path}")
        return new_path

    return old_path
