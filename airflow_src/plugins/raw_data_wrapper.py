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
    COLLISION_FLAG_SEP,
    get_instrument_type,
    get_internal_instrument_backup_path,
    get_internal_instrument_data_path,
)

from shared.db.models import RawFile, get_created_at_year_month


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
        if instrument_type == InstrumentTypes.BRUKER:
            return BrukerRawDataWrapper(instrument_id, **kwargs)
        raise ValueError(f"Unsupported vendor: {instrument_type}")

    def __init__(
        self,
        instrument_id: str,
        raw_file_name: str | None = None,
        raw_file: RawFile | None = None,
    ):
        """Initialize the RawDataWrapper.

        If neither raw_file_name nor raw_file is given, only get_raw_files_on_instrument() will work.
        If raw_file_name is given, all methods up to get_files_to_copy() will work.
        Full functionality if `raw_file` object is given.
        """
        # could be .raw file, one of the four .wiff files, or .d folder
        self._raw_file_name: str | None = None

        self._raw_file_original_name: str | None = None
        self._year_month_subfolder: str | None = None

        if raw_file is not None and raw_file_name is not None:
            raise ValueError("Only one of raw_file and raw_file_name must be given.")

        # this logic could be moved to a factory class
        if raw_file is not None:
            self._raw_file_name = raw_file.name
            self._raw_file_original_name = raw_file.original_name
            self._year_month_subfolder = get_created_at_year_month(raw_file)
            logging.info(f"Got {self._year_month_subfolder=}")
        elif raw_file_name is not None:
            self._raw_file_name = raw_file_name
            # Extracting the collision flag like this is a bit hacky, but it makes the class work also without
            # the raw_file object.
            self._raw_file_original_name = (
                raw_file_name
                if (COLLISION_FLAG_SEP not in raw_file_name)
                else raw_file_name.split(COLLISION_FLAG_SEP, maxsplit=1)[1]
            )

        if (
            self._raw_file_name is not None
            and (ext := Path(self._raw_file_name).suffix) != self._main_file_extension
        ):
            raise ValueError(
                f"Unsupported file extension: {ext}, expected {self._main_file_extension}"
            )

        self._instrument_path = get_internal_instrument_data_path(instrument_id)
        self._backup_path = get_internal_instrument_backup_path(instrument_id)

        logging.info(
            f"Initialized with {self._instrument_path=} {self._backup_path=} {self._raw_file_name=} {self._raw_file_original_name=}"
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
            f"Current contents of {self._instrument_path} ({len(file_names)}, extension '{self._main_file_extension}'): {file_names}"
        )
        return file_names

    def _get_destination_file_path(self, file_path: Path) -> Path:
        """Get destination file path by replacing the original file name with the raw file id in the given path."""
        return Path(
            str(file_path).replace(self._raw_file_original_name, self._raw_file_name)
        )


class ThermoRawDataWrapper(RawDataWrapper):
    """Class wrapping Thermo-specific logic."""

    _main_file_extension = ".raw"

    def _file_path_to_watch(self) -> Path:
        """Get the (absolute) path to the raw file."""
        return self._instrument_path / self._raw_file_original_name

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination path (both absolute) for the raw file."""
        src_path = self._instrument_path / self._raw_file_original_name
        dst_path = self._backup_path / self._year_month_subfolder / self._raw_file_name

        return {src_path: dst_path}


class ZenoRawDataWrapper(RawDataWrapper):
    """Class wrapping Zeno-specific logic."""

    _main_file_extension = ".wiff"

    def _file_path_to_watch(self) -> Path:
        """Get the (absolute) path to the raw file."""
        return self._instrument_path / self._raw_file_original_name

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination paths (both absolute) for the raw file.

        In addition to the raw file (e.g. raw_file.wiff), all other files sharing
        the same stem are considered here (e.g. raw_file.something).
        """
        files_to_copy = {}
        raw_file_stem = Path(self._raw_file_original_name).stem
        for file_path in self._instrument_path.glob(f"{raw_file_stem}.*"):
            src_path = file_path
            dst_path = self._backup_path / self._year_month_subfolder / file_path.name

            files_to_copy[src_path] = self._get_destination_file_path(dst_path)

        return files_to_copy


class BrukerRawDataWrapper(RawDataWrapper):
    """Class wrapping Bruker-specific logic."""

    _main_file_extension = ".d"
    _file_name_to_watch = "analysis.tdf_bin"

    def _file_path_to_watch(self) -> Path:
        """Get the (absolute) path to the main raw data file."""
        return (
            self._instrument_path
            / self._raw_file_original_name
            / self._file_name_to_watch
        )

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination paths (both absolute) for the raw file.

        All files within the raw file directory are returned (including those in subfolders).
        """
        src_base_path = self._instrument_path / self._raw_file_original_name

        files_to_copy = {}

        for src_item in src_base_path.rglob("*"):
            if src_item.is_file():
                src_file_path = src_item
                rel_file_path = src_file_path.relative_to(self._instrument_path)
                dst_file_path = (
                    self._backup_path / self._year_month_subfolder / rel_file_path
                )

                files_to_copy[src_file_path] = self._get_destination_file_path(
                    dst_file_path
                )

        return files_to_copy
