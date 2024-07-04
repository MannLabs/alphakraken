"""Class wrapping instrument-type specific logic for acquisition monitoring and file copying."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from common.keys import InstrumentTypes
from common.settings import (
    get_instrument_type,
    get_internal_instrument_backup_path,
    get_internal_instrument_data_path,
)

# TODO: file duplicate handling


class RawDataWrapper(ABC):
    """Abstract class wrapping instrument-type specific logic.

    Note: do not call constructors of subclasses directly, always use RawDataWrapper.create().
    """

    @classmethod
    def create(cls, *, instrument_id: str, **kwargs) -> "RawDataWrapper":
        """Create a RawDataWrapper instance based on the instrument type."""
        instrument_type = get_instrument_type(instrument_id)
        if instrument_type == InstrumentTypes.THERMO:
            return ThermoRawDataWrapper(instrument_id, **kwargs)

        raise ValueError(f"Unsupported vendor: {instrument_type}")

    def __init__(self, instrument_id: str, raw_file_name: str):
        """Initialize the RawDataWrapper."""
        # could be .raw file, one of the four .wiff files, or .d folder
        self._raw_file_name = raw_file_name

        self._instrument_path = get_internal_instrument_data_path(instrument_id)
        self._backup_path = get_internal_instrument_backup_path(instrument_id)

        self._id = self._get_id()

        logging.info(
            f"Initialized with {self._instrument_path=} {self._backup_path=} {self._id=}"
        )

    @abstractmethod
    def _file_path_to_watch(self) -> Path:
        """Get the path to the file to watch for changes."""

    @abstractmethod
    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths (both absolute)."""

    def file_path_to_watch(self) -> Path:
        """Get the path to the file to watch for changes."""
        file_path_to_watch = self._file_path_to_watch()
        logging.info(f"{file_path_to_watch=}")
        return file_path_to_watch

    def get_files_to_copy(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths (both absolute)."""
        files_to_copy = self._get_files_to_copy()
        logging.info(f"{files_to_copy=}")
        return files_to_copy

    def _get_id(self) -> str:
        """Get the ID (= name without extension) of the raw file."""
        return Path(self._raw_file_name).stem


class ThermoRawDataWrapper(RawDataWrapper):
    """Class wrapping Thermo-specific logic."""

    def _file_path_to_watch(self) -> Path:
        """Get the path to the raw file."""
        return self._instrument_path / self._raw_file_name

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of the path to the raw file on the instrument to the target on the backup folder."""
        src_path = self._instrument_path / self._raw_file_name
        dst_path = self._backup_path / self._raw_file_name

        return {src_path: dst_path}
