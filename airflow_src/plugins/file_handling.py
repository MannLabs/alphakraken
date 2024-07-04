"""Utility functions for handling files."""

import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

from common.settings import get_internal_instrument_data_path


def get_file_creation_timestamp(raw_file_name: str, instrument_id: str) -> float:
    """Get the creation timestamp (unix epoch) of a raw file."""
    raw_file_path = get_internal_instrument_data_path(instrument_id) / raw_file_name
    file_creation_ts = raw_file_path.stat().st_ctime
    logging.info(f"File {raw_file_name} has {file_creation_ts=}")
    return file_creation_ts


def get_file_size(file_path: Path) -> float:
    """Get the size (in bytes) of a file."""
    file_size_bytes = file_path.stat().st_size
    logging.info(f"File {file_path} has {file_size_bytes=}")
    return file_size_bytes


def _get_file_hash(file_path: Path, chunk_size: int = 8192) -> str:
    """Get the hash of a file."""
    with open(file_path, "rb") as f:  # noqa: PTH123
        file_hash = hashlib.md5()  # noqa: S324
        while chunk := f.read(chunk_size):
            file_hash.update(chunk)
    logging.info(f"Hash of {file_path} is {file_hash.hexdigest()}")
    return file_hash.hexdigest()


def _file_already_exists(dst_path: Path, src_hash: str) -> bool:
    """Check if a file already exists in the backup location and has the same hash."""
    if dst_path.exists():
        logging.info("File already exists in backup location. Checking hash ..")
        if _get_file_hash(dst_path) == (src_hash):
            logging.info("Hashes match.")
            return True
        logging.warning("Hashes do not match.")
    return False


def copy_file(
    src_path: Path,
    dst_path: Path,
) -> None:
    """Copy a raw file to the backup location and check its hashsum."""
    logging.info(f"Copying {src_path} to {dst_path} ..")

    start = datetime.now()  # noqa: DTZ005
    src_hash = _get_file_hash(src_path)
    time_elapsed = (datetime.now() - start).total_seconds()  # noqa: DTZ005
    logging.info(f"Hash calculated! time elapsed: {time_elapsed/60:.1f} min")
    if _file_already_exists(dst_path, src_hash):
        return

    start = datetime.now()  # noqa: DTZ005
    shutil.copy2(src_path, dst_path)
    time_elapsed = (datetime.now() - start).total_seconds()  # noqa: DTZ005
    dst_size = get_file_size(dst_path)
    logging.info(
        f"Copying done! time elapsed: {time_elapsed/60:.1f} min at {dst_size / max(time_elapsed, 1) / 1024 ** 2:.1f} MB/s"
    )

    if (hash_dst := _get_file_hash(dst_path)) != (src_hash):
        raise ValueError(f"Hashes do not match ofter copy! {src_hash=} != {hash_dst=}")
