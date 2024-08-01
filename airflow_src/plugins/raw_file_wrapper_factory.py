"""Classes wrapping instrument-type specific logic for acquisition monitoring and file copying.

A general note on naming:
Within this code base, the term "raw file" refers to the "main" file (or folder) produced by the instrument.
For thermo, it is the ".raw" file, for zeno, it is the ".wiff" file, and for bruker, it is the ".d" folder.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

from common.keys import InstrumentTypes
from common.settings import (
    COLLISION_FLAG_SEP,
    get_instrument_type,
    get_internal_instrument_backup_path,
    get_internal_instrument_data_path,
)

from shared.db.models import RawFile, get_created_at_year_month


class RawFileMonitorWrapper(ABC):
    """Abstract base class for wrapping raw files for monitoring acquisitions."""

    _main_file_extension: str

    def __init__(self, instrument_id: str, raw_file_name: str | None = None):
        """Initialize the RawFileMonitorWrapper.

        :param instrument_id: The ID of the instrument
        :param raw_file_name: The name of the raw file. Needs to be set to allow calling file_path_to_monitor_acquisition().
        """
        self._instrument_path = get_internal_instrument_data_path(instrument_id)
        # Extracting the collision flag like this is a bit hacky,
        # but it makes the class work also without the raw_file object.
        self._raw_file_name: str | None = (
            raw_file_name
            if (raw_file_name is None or COLLISION_FLAG_SEP not in raw_file_name)
            else raw_file_name.split(COLLISION_FLAG_SEP, maxsplit=1)[1]
        )

        if (
            self._raw_file_name is not None
            and (ext := Path(self._raw_file_name).suffix) != self._main_file_extension
        ):
            raise ValueError(
                f"Unsupported file extension: {ext}, expected {self._main_file_extension}"
            )

    def get_raw_files_on_instrument(self) -> set[str]:
        """Get the current raw file names (only with the relevant extension) in the instrument directory."""
        dir_contents = set(self._instrument_path.glob(f"*{self._main_file_extension}"))

        file_names = {d.name for d in dir_contents}

        logging.info(
            f"Current contents of {self._instrument_path} ({len(file_names)}, extension '{self._main_file_extension}'): {file_names}"
        )
        return file_names

    def file_path_to_monitor_acquisition(self) -> Path:
        """Get the path to the file to watch for changes."""
        if self._raw_file_name is None:
            raise ValueError("Raw file name not set.")

        file_path_to_monitor_acquisition = self._file_path_to_monitor_acquisition()
        logging.info(f"{file_path_to_monitor_acquisition=}")
        return file_path_to_monitor_acquisition

    @abstractmethod
    def _file_path_to_monitor_acquisition(self) -> Path:
        """Get the path to the file to watch for changes."""

    @property
    def instrument_path(self) -> Path:
        """Get the path to the instrument data directory."""
        return self._instrument_path


class ThermoRawFileMonitorWrapper(RawFileMonitorWrapper):
    """RawFileMonitorWrapper for Thermo instruments."""

    _main_file_extension = ".raw"

    def _file_path_to_monitor_acquisition(self) -> Path:
        """Get the (absolute) path to the raw file to monitor."""
        return self._instrument_path / self._raw_file_name


class ZenoRawFileMonitorWrapper(RawFileMonitorWrapper):
    """RawFileMonitorWrapper for Zeno instruments."""

    _main_file_extension = ".wiff"

    def _file_path_to_monitor_acquisition(self) -> Path:
        """Get the (absolute) path to the raw file to monitor."""
        return self._instrument_path / self._raw_file_name


class BrukerRawFileMonitorWrapper(RawFileMonitorWrapper):
    """RawFileMonitorWrapper for Bruker instruments."""

    _main_file_extension = ".d"
    _file_name_to_watch = "analysis.tdf_bin"

    def _file_path_to_monitor_acquisition(self) -> Path:
        """Get the (absolute) path to the main raw data file to monitor."""
        return self._instrument_path / self._raw_file_name / self._file_name_to_watch


class RawFileCopyWrapper(ABC):
    """Abstract base class for copying raw data files."""

    def __init__(
        self, instrument_id: str, raw_file: RawFile, target_path: Path | None = None
    ):
        """Initialize the RawFileCopyWrapper.

        :param instrument_id: the ID of the instrument
        :param raw_file: a raw file object
        """
        self._raw_file = raw_file

        self._instrument_path = get_internal_instrument_data_path(instrument_id)

        self._target_path = (
            get_internal_instrument_backup_path(instrument_id)
            / get_created_at_year_month(raw_file)
            if target_path is None
            else target_path
        )

        self._acquisition_monitor = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id, raw_file.original_name
        )

    @abstractmethod
    def get_files_to_copy(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths for copying."""

    def get_files_to_move(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths for copying."""
        return self.get_files_to_copy()

    def file_path_to_calculate_size(self) -> Path:
        """Get the path to the file to calculate size."""
        return self._acquisition_monitor.file_path_to_monitor_acquisition()

    def _get_destination_file_path(self, file_path: Path) -> Path:
        """Get destination file path by replacing the original file name with the raw file id in the given path."""
        return Path(
            str(file_path).replace(self._raw_file.original_name, self._raw_file.id)
        )


class ThermoRawFileCopyWrapper(RawFileCopyWrapper):
    """Class wrapping Thermo-specific logic."""

    def get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination path (both absolute) for the raw file."""
        src_path = self._instrument_path / self._raw_file.original_name
        dst_path = self._target_path / self._raw_file.id

        files_to_copy = {src_path: dst_path}

        logging.info(f"{files_to_copy=}")
        return files_to_copy


class ZenoRawFileCopyWrapper(RawFileCopyWrapper):
    """Class wrapping Zeno-specific logic."""

    def get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination paths (both absolute) for the raw file.

        In addition to the raw file (e.g. raw_file.wiff), all other files sharing
        the same stem are considered here (e.g. raw_file.something).
        """
        files_to_copy = {}
        src_file_stem = Path(self._raw_file.original_name).stem
        for file_path in self._instrument_path.glob(f"{src_file_stem}.*"):
            src_path = file_path
            dst_path = self._target_path / file_path.name

            files_to_copy[src_path] = self._get_destination_file_path(dst_path)

        logging.info(f"{files_to_copy=}")
        return files_to_copy


class BrukerRawFileCopyWrapper(RawFileCopyWrapper):
    """Class wrapping Bruker-specific logic."""

    def get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination paths (both absolute) for the raw file.

        All files within the raw file directory are returned (including those in subfolders).
        """
        src_base_path = self._instrument_path / self._raw_file.original_name

        files_to_copy = {}

        for src_item in src_base_path.rglob("*"):
            if src_item.is_file():
                src_file_path = src_item
                rel_file_path = src_file_path.relative_to(self._instrument_path)
                dst_file_path = self._target_path / rel_file_path

                files_to_copy[src_file_path] = self._get_destination_file_path(
                    dst_file_path
                )
        logging.info(f"{files_to_copy=}")
        return files_to_copy

    def get_files_to_move(self) -> dict[Path, Path]:
        """Get a list of items that can be passed to a "move" command."""
        return {
            self._instrument_path / self._raw_file.original_name: self._target_path
            / self._raw_file.id
        }


MONITOR = "monitor"
COPIER = "copier"


class RawFileWrapperFactory:
    """Factory class for creating appropriate handlers based on instrument type."""

    _handlers: dict[str, dict[str, type]] = {  # noqa: RUF012
        InstrumentTypes.THERMO: {
            MONITOR: ThermoRawFileMonitorWrapper,
            COPIER: ThermoRawFileCopyWrapper,
        },
        InstrumentTypes.ZENO: {
            MONITOR: ZenoRawFileMonitorWrapper,
            COPIER: ZenoRawFileCopyWrapper,
        },
        InstrumentTypes.BRUKER: {
            MONITOR: BrukerRawFileMonitorWrapper,
            COPIER: BrukerRawFileCopyWrapper,
        },
    }

    @classmethod
    def _create_handler(
        cls, handler_type: str, instrument_id: str, *args
    ) -> Union["RawFileMonitorWrapper", "RawFileCopyWrapper"]:
        """Create a handler of the specified type for the given instrument.

        :param handler_type: The type of handler to create ('lister', 'monitor', or 'copier')
        :param instrument_id: The ID of the instrument
        :param args: Additional arguments to pass to the handler constructor
        :raises ValueError: If the instrument type or handler type is not supported
        """
        instrument_type = get_instrument_type(instrument_id)
        handler_class = cls._handlers.get(instrument_type, {}).get(handler_type)

        if handler_class is None:
            raise ValueError(
                f"Unsupported vendor or handler type: {instrument_type}, {handler_type}"
            )

        return handler_class(instrument_id, *args)

    @classmethod
    def create_monitor_wrapper(
        cls, instrument_id: str, raw_file_name: str | None = None
    ) -> RawFileMonitorWrapper:
        """Create an RawFileMonitorWrapper for the specified instrument and raw file.

        :param instrument_id: The ID of the instrument
        :param raw_file_name: The name of the raw file to monitor
        :return: An instance of the appropriate RawFileMonitorWrapper subclass
        """
        return cls._create_handler(MONITOR, instrument_id, raw_file_name)

    @classmethod
    def create_copy_wrapper(
        cls, instrument_id: str, raw_file: RawFile, target_path: Path | None = None
    ) -> RawFileCopyWrapper:
        """Create a RawFileCopyWrapper for the specified instrument and raw file.

        :param instrument_id: The ID of the instrument
        :param raw_file: a raw file object
        :param target_path: optional target base path
        """
        return cls._create_handler(COPIER, instrument_id, raw_file, target_path)
