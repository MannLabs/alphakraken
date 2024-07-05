"""Class wrapping instrument-type specific logic for acquisition monitoring and file copying.

A general note on naming:
Within this code base, the term "raw file" refers to the "main" file (or folder) produced by the instrument.
For thermo, it is the ".raw" file, for zeno, it is the ".wiff" file, and for bruker, it is the ".d" folder.
"""

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

    # the extension of the main raw data file (with leading dot!)
    _main_file_extension: str | None = None

    @classmethod
    def create(cls, *, instrument_id: str, **kwargs) -> "RawDataWrapper":
        """Create a RawDataWrapper instance based on the instrument type."""
        instrument_type = get_instrument_type(instrument_id)
        if instrument_type == InstrumentTypes.THERMO:
            return ThermoRawDataWrapper(instrument_id, **kwargs)
        if instrument_type == InstrumentTypes.ZENO:
            return ZenoRawDataWrapper(instrument_id, **kwargs)
        raise ValueError(f"Unsupported vendor: {instrument_type}")

    def __init__(self, instrument_id: str, raw_file_name: str | None):
        """Initialize the RawDataWrapper."""
        # could be .raw file, one of the four .wiff files, or .d folder
        self._raw_file_name = raw_file_name

        if (
            raw_file_name is not None
            and (ext := Path(raw_file_name).suffix) != self._main_file_extension
        ):
            raise ValueError(
                f"Unsupported file extension: {ext}, expected {self._main_file_extension}"
            )

        self._instrument_path = get_internal_instrument_data_path(instrument_id)
        self._backup_path = get_internal_instrument_backup_path(instrument_id)

        logging.info(
            f"Initialized with {self._instrument_path=} {self._backup_path=} {self._raw_file_name=}"
        )

    @property
    def instrument_path(self) -> Path:
        """Get the path to the instrument data directory."""
        return self._instrument_path

    @abstractmethod
    def _file_path_to_watch(self) -> Path:
        """Get the path to the file to watch for changes."""

    @abstractmethod
    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths (both absolute)."""

    # TODO: file_path_to_watch_acquisition
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

    def get_raw_files_on_instrument(self) -> set[str]:
        """Get the current raw file names (only with the relevant extension) in the instrument directory."""
        dir_contents = set(self._instrument_path.glob(f"*{self._main_file_extension}"))

        file_names = {d.name for d in dir_contents}

        logging.info(
            f"Current contents of {self._instrument_path} ({len(file_names)}, extension '.{self._main_file_extension}'): {file_names}"
        )
        return file_names


class ThermoRawDataWrapper(RawDataWrapper):
    """Class wrapping Thermo-specific logic."""

    _main_file_extension = ".raw"

    def _file_path_to_watch(self) -> Path:
        """Get the path to the raw file."""
        return self._instrument_path / self._raw_file_name

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of the path to the raw file on the instrument to the target on the backup folder."""
        src_path = self._instrument_path / self._raw_file_name
        dst_path = self._backup_path / self._raw_file_name

        return {src_path: dst_path}


class ZenoRawDataWrapper(RawDataWrapper):
    """Class wrapping Zeno-specific logic."""

    _main_file_extension = ".wiff"

    def _file_path_to_watch(self) -> Path:
        """See docu of superclass."""
        return self._instrument_path / self._raw_file_name

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """See docu of superclass."""
        files_to_copy = {}
        for file_path in self._instrument_path.rglob(f"{self._raw_file_name}.*"):
            src_path = file_path
            dst_path = self._backup_path / file_path.name

            files_to_copy[src_path] = dst_path

        return files_to_copy
