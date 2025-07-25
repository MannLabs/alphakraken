"""Classes wrapping instrument-type specific logic for acquisition monitoring and file copying.

A general note on naming:
Within this code base, the term "raw file" refers to the file (or folder) produced by the instrument.
For Thermo, it is the ".raw" file, for Sciex, it is the ".wiff" file, and for Bruker, it is the ".d" folder.

The term "main file" refers to one physical file that is monitored for changes during acquisition, and used for
decisions that involve size comparisons. For Thermo, it is the ".raw" file itself, for Sciex, it is the ".wiff" file,
for Bruker, it is the "analysis.tdf_bin" file within the ".d" folder.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

from airflow.exceptions import AirflowFailException
from common.keys import InstrumentKeys
from common.paths import (
    get_internal_backup_path_for_instrument,
    get_internal_instrument_data_path,
)
from common.settings import (
    INSTRUMENT_BACKUP_FOLDER_NAME,
    get_instrument_settings,
)

from shared.db.models import RawFile, get_created_at_year_month
from shared.keys import InstrumentTypes

MONITOR = "monitor"
COPIER = "copier"


class RawFileStemEmptyError(Exception):
    """Raise in case the raw file name equals the file extension."""

    def __init__(self, raw_file_name: str):
        """Initialize the error."""
        super().__init__(
            f"Raw file name equals the file extension: {raw_file_name}. "
            f"This case is not handled by AlphaKraken. Please take care of this file manually.\n"
            f"Important: think twice before renaming it on the acquisition folder (cf. Readme!)."
        )


class RawFileMonitorWrapper(ABC):
    """Abstract base class for wrapping raw files for monitoring acquisitions."""

    _raw_file_extension: str
    # The 'main file name' is just required if the instrument produces a folder.
    main_file_name: str | None = None

    def __init__(
        self,
        instrument_id: str,
        raw_file: RawFile | None = None,
        raw_file_original_name: str | None = None,
    ):
        """Initialize the RawFileMonitorWrapper.

        :param instrument_id: The ID of the instrument
        :param raw_file: The raw file object from DB.
        :param raw_file_original_name: The original name of the raw file.

        raw_file and raw_file_original_name are mutually exclusive, one of them
        needs to be set to allow calling main_file_path().
        """
        self._instrument_path = get_internal_instrument_data_path(instrument_id)

        original_name = None
        if raw_file_original_name is not None:
            if raw_file is not None:
                raise ValueError(
                    "Either raw_file or raw_file_original_name should be set, not both."
                )
            original_name = raw_file_original_name
        elif raw_file is not None:
            original_name = raw_file.original_name

        if original_name == self._raw_file_extension:
            # The edge case when the file stem is "" (e.g. filename is ".raw") needs to be handled manually:
            # there could be downstream consequences that are not worth thinking of given this is clearly a mistake
            # and should happen very rarely.
            raise RawFileStemEmptyError(str(original_name))

        if (
            original_name is not None
            and (ext := Path(original_name).suffix) != self._raw_file_extension
        ):
            raise ValueError(
                f"Unsupported file extension: {ext}, expected {self._raw_file_extension}"
            )

        self._raw_file_original_name: str | None = original_name

    def get_raw_files_on_instrument(self) -> set[str]:
        """Get the current raw file names (only with the relevant extension) in the instrument directory."""
        dir_contents = set(self._instrument_path.glob(f"*{self._raw_file_extension}"))

        file_names = {d.name for d in dir_contents}

        logging.info(
            f"Contents ('*{self._raw_file_extension}') of {self._instrument_path} ({len(file_names)}): {file_names}"
        )
        return file_names

    def main_file_path(self) -> Path:
        """Get the (absolute) path to the main file, i.e. the one to monitor."""
        if self._raw_file_original_name is None:
            raise ValueError("Raw file name not set.")

        main_file_path = self._main_file_path()
        logging.info(f"{main_file_path=}")
        return main_file_path

    @abstractmethod
    def _main_file_path(self) -> Path:
        """Get the (absolute) path to the raw file to monitor."""

    @property
    def instrument_path(self) -> Path:
        """Get the path to the instrument data directory."""
        return self._instrument_path

    def get_corrupted_file_name(self) -> str | None:
        """Get the name of the file that the raw file is renamed to by the acquisition software in certain failure cases.

        Defaults to None -> handling of corrupted files turned off.
        """
        return None

    @classmethod
    def is_corrupted_file_name(cls, file_name: str) -> bool:
        """Check if the given file name is a corrupted file (i.e. renamed by acquisition software).

        Defaults to False -> handling of corrupted files turned off.
        """
        del file_name  # unused
        return False


class ThermoRawFileMonitorWrapper(RawFileMonitorWrapper):
    """RawFileMonitorWrapper for Thermo instruments."""

    _raw_file_extension = ".raw"
    _corrupted_file_suffix = f"_CORRUPTED{_raw_file_extension}"

    def _main_file_path(self) -> Path:
        """Get the (absolute) path to the raw file to monitor."""
        return self._instrument_path / self._raw_file_original_name

    def get_corrupted_file_name(self) -> str | None:
        """Get the name of the file that the raw file is renamed to by the acquisition software in certain failure cases.

        Only the Thermo acquisition software does these renamings on certain acquisition failures.
        """
        return f"{Path(self._raw_file_original_name).stem}{self._corrupted_file_suffix}"

    @classmethod
    def is_corrupted_file_name(cls, file_name: str) -> bool:
        """Check if the given file name is a corrupted file (i.e. renamed by acquisition software)."""
        return file_name.endswith(cls._corrupted_file_suffix)


class SciexRawFileMonitorWrapper(RawFileMonitorWrapper):
    """RawFileMonitorWrapper for Sciex instruments."""

    _raw_file_extension = ".wiff"

    def _main_file_path(self) -> Path:
        """Get the (absolute) path to the raw file to monitor."""
        return self._instrument_path / self._raw_file_original_name


class BrukerRawFileMonitorWrapper(RawFileMonitorWrapper):
    """RawFileMonitorWrapper for Bruker instruments."""

    _raw_file_extension = ".d"
    main_file_name = "analysis.tdf_bin"

    def _main_file_path(self) -> Path:
        """Get the (absolute) path to the main raw data file to monitor."""
        return (
            self._instrument_path / self._raw_file_original_name / self.main_file_name
        )


class PathProvider(ABC):
    """Abstract base class for providing source and target paths for raw file operations.

    Depending on the use case (copy to pool backup, move to instrument backup, compare before deletion, the
    source and target paths and file names can differ. This class provides a common interface for all of them.
    """

    def __init__(self, instrument_id: str, raw_file: RawFile):
        """Initialize the PathProvider."""
        self._instrument_id = instrument_id
        self._raw_file = raw_file

    @abstractmethod
    def get_source_folder_path(self) -> Path:
        """Get the source path (=folder where raw file is located) for a raw file operation."""

    @abstractmethod
    def get_target_folder_path(self) -> Path:
        """Get the target path (=folder where raw file is located) for a raw file operation."""

    @abstractmethod
    def get_source_file_name(self) -> str:
        """Get the source file name for a raw file operation."""

    @abstractmethod
    def get_target_file_name(self) -> str:
        """Get the target file name for a raw file operation."""


class CopyPathProvider(PathProvider):
    """PathProvider for copying raw files from the instrument (original name) to the pool backup (raw file id)."""

    def get_source_folder_path(self) -> Path:
        """See docu of superclass."""
        return get_internal_instrument_data_path(self._instrument_id)

    def get_target_folder_path(self) -> Path:
        """See docu of superclass."""
        return get_internal_backup_path_for_instrument(
            self._instrument_id
        ) / get_created_at_year_month(self._raw_file)

    def get_source_file_name(self) -> str:
        """See docu of superclass."""
        return self._raw_file.original_name

    def get_target_file_name(self) -> str:
        """See docu of superclass."""
        return self._raw_file.id


class MovePathProvider(PathProvider):
    """PathProvider for moving raw files from the instrument (original name) to the instrument backup (raw file id)."""

    def get_source_folder_path(self) -> Path:
        """See docu of superclass."""
        return get_internal_instrument_data_path(self._instrument_id)

    def get_target_folder_path(self) -> Path:
        """See docu of superclass."""
        return (
            get_internal_instrument_data_path(self._instrument_id)
            / INSTRUMENT_BACKUP_FOLDER_NAME
        )

    def get_source_file_name(self) -> str:
        """See docu of superclass."""
        return self._raw_file.original_name

    def get_target_file_name(self) -> str:
        """See docu of superclass."""
        return self._raw_file.id


class RemovePathProvider(PathProvider):
    """PathProvider for comparing raw files from instrument backup (raw file id) to the pool backup (raw file id)."""

    def get_source_folder_path(self) -> Path:
        """See docu of superclass."""
        return (
            get_internal_instrument_data_path(self._instrument_id)
            / INSTRUMENT_BACKUP_FOLDER_NAME
        )

    def get_target_folder_path(self) -> Path:
        """See docu of superclass."""
        return get_internal_backup_path_for_instrument(
            self._instrument_id
        ) / get_created_at_year_month(self._raw_file)

    def get_source_file_name(self) -> str:
        """See docu of superclass."""
        return self._raw_file.id

    def get_target_file_name(self) -> str:
        """See docu of superclass."""
        return self._raw_file.id


class RawFileWriteWrapper(ABC):
    """Abstract base class for preparing write operations (copying, moving or removing) of raw data files."""

    def __init__(
        self,
        instrument_id: str,
        *,
        raw_file: RawFile,
        path_provider: type[PathProvider],
    ):
        """Initialize the RawFileWriteWrapper.

        :param instrument_id: the ID of the instrument
        :param raw_file: a raw file object
        :param path_provider: the path provider class for the operation
        """
        self._path_provider_instance = path_provider(instrument_id, raw_file)

        self._source_folder_path = self._path_provider_instance.get_source_folder_path()
        self._target_folder_path = self._path_provider_instance.get_target_folder_path()
        self._source_file_name = self._path_provider_instance.get_source_file_name()
        self._target_file_name = self._path_provider_instance.get_target_file_name()

        self._acquisition_monitor = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id, raw_file
        )

    def _check_path_provider(self, path_provider: type[PathProvider]) -> None:
        """Check if the path provider is the correct one for the operation."""
        if not isinstance(self._path_provider_instance, path_provider):
            raise TypeError(
                f"Wrong path provider for operation: {self._path_provider_instance.__class__}"
            )

    def get_files_to_copy(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths for file-by-file operations (copying or removing).

        This gives a 1:1 mapping between source and destination files (not folders!).

        :return: A dictionary mapping source to destination paths for 'copy' operations.
        :raises ValueError: If the path provider is not the correct one for the operation.
        """
        self._check_path_provider(CopyPathProvider)

        files = self._get_files_to_copy()
        logging.info(f"{files=}")
        return files

    @property
    def target_folder_path(self) -> Path:
        """The target folder path for the raw file operation, e.g. '/opt/airflow/mounts/backup/test2/2025_07'."""
        return self._target_folder_path

    @abstractmethod
    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Actual implementation."""
        # TODO: there's currently an inconsistent behaviour between the different implementations:
        #  some always return the source file path, some return nothing if the source file is not present
        #  (same for _get_files_to_move)

    def get_files_to_move(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths for moving.

        :return: A dictionary mapping source to destination paths for 'move' operations.
        :raises ValueError: If the path provider is not the correct one for the operation.
        """
        self._check_path_provider(MovePathProvider)

        files = self._get_files_to_move()
        logging.info(f"{files=}")
        return files

    @abstractmethod
    def _get_files_to_move(self) -> dict[Path, Path]:
        """Actual implementation."""

    def get_folder_to_remove(self) -> Path | None:
        """Get the absolute path of the folder to remove.

        None if no folder to be removed.
        """
        self._check_path_provider(RemovePathProvider)

        return self._get_folder_to_remove()

    @abstractmethod
    def _get_folder_to_remove(self) -> Path | None:
        """Actual implementation."""

    def get_files_to_remove(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths for comparing before removing the 'keys'.

        :return: A dictionary mapping source to destination paths for comparing before 'remove' operations.
        :raises ValueError: If the path provider is not the correct one for the operation.
        """
        self._check_path_provider(RemovePathProvider)

        files = self._get_files_to_copy()
        logging.info(f"{files=}")
        return files

    def main_file_path(self) -> Path:
        """Get the absolute path to the main file (i.e. the one to monitor)."""
        return self._acquisition_monitor.main_file_path()


class ThermoRawFileWriteWrapper(RawFileWriteWrapper):
    """Class wrapping Thermo-specific logic."""

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination path (both absolute) for the raw file."""
        src_path = self._source_folder_path / self._source_file_name
        dst_path = self._target_folder_path / self._target_file_name

        return {src_path: dst_path}

    def _get_files_to_move(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths for moving."""
        return self._get_files_to_copy()

    def _get_folder_to_remove(self) -> Path | None:
        """Get the folder to remove."""
        return None


class SciexRawFileWriteWrapper(RawFileWriteWrapper):
    """Class wrapping Sciex-specific logic."""

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination paths (both absolute) for the raw file.

        All other files sharing the same stem with the raw file (e.g. `some_file.wiff` -> stem: `some_file`),
        are considered here (e.g. some_file.wiff, some_file.wiff.scan, some_file.wiff2, some_file.timeseries.data).
        """
        files_to_copy = {}

        src_file_stem = Path(self._source_file_name).stem
        dst_file_stem = Path(self._target_file_name).stem

        for src_file_path in self._source_folder_path.glob(f"{src_file_stem}.*"):
            # resorting to string manipulation here, because of double-extensions (e.g. .wiff.scan)
            dst_file_name = Path(
                src_file_path.name.replace(src_file_stem, dst_file_stem)
            )

            files_to_copy[src_file_path] = self._target_folder_path / dst_file_name

        return files_to_copy

    def _get_files_to_move(self) -> dict[Path, Path]:
        """Get a dictionary mapping source file to destination paths for moving."""
        return self._get_files_to_copy()

    def _get_folder_to_remove(self) -> Path | None:
        """Get the folder to remove."""
        return None


class BrukerRawFileWriteWrapper(RawFileWriteWrapper):
    """Class wrapping Bruker-specific logic."""

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Get the mapping of source to destination paths (both absolute) for the raw file.

        All files within the raw file directory are returned (including those in subfolders).
        Note that the code that does the copying must take care of creating the target directory if it does not exist.
        """
        src_base_path = self._source_folder_path / self._source_file_name
        dst_base_path = self._target_folder_path / self._target_file_name

        files_to_copy = {}

        for src_item in src_base_path.rglob("*"):
            if src_item.is_file():
                src_file_path = src_item
                rel_file_path = src_file_path.relative_to(src_base_path)

                files_to_copy[src_file_path] = dst_base_path / rel_file_path

        return files_to_copy

    def _get_files_to_move(self) -> dict[Path, Path]:
        """Get a dictionary mapping of items that can be passed to a "move" command.

        In the case of Bruker, just map the folder name as "move" can handle it.
        """
        return {
            self._source_folder_path / self._source_file_name: self._target_folder_path
            / self._target_file_name
        }

    def _get_folder_to_remove(self) -> Path | None:
        """Get the folder to remove.

        For Bruker instruments, the folder to remove is the source folder of the raw data.
        """
        return self._source_folder_path / self._source_file_name


class RawFileWrapperFactory:
    """Factory class for creating appropriate handlers based on instrument type."""

    _handlers: dict[str, dict[str, type]] = {  # noqa: RUF012
        InstrumentTypes.THERMO: {
            MONITOR: ThermoRawFileMonitorWrapper,
            COPIER: ThermoRawFileWriteWrapper,
        },
        InstrumentTypes.SCIEX: {
            MONITOR: SciexRawFileMonitorWrapper,
            COPIER: SciexRawFileWriteWrapper,
        },
        InstrumentTypes.BRUKER: {
            MONITOR: BrukerRawFileMonitorWrapper,
            COPIER: BrukerRawFileWriteWrapper,
        },
    }

    @classmethod
    def _create_handler(
        cls, handler_type: str, instrument_id: str, **kwargs
    ) -> Union["RawFileMonitorWrapper", "RawFileWriteWrapper"]:
        """Create a handler of the specified type for the given instrument.

        :param handler_type: The type of handler to create ('lister', 'monitor', or 'copier')
        :param instrument_id: The ID of the instrument
        :param args: Additional arguments to pass to the handler constructor
        :raises ValueError: If the instrument type or handler type is not supported
        """
        instrument_type = get_instrument_settings(instrument_id, InstrumentKeys.TYPE)
        handler_class = cls._handlers.get(instrument_type, {}).get(handler_type)

        if handler_class is None:
            raise ValueError(
                f"Unsupported vendor or handler type for {instrument_id}: {instrument_type}, {handler_type}"
            )

        return handler_class(instrument_id, **kwargs)

    @classmethod
    def create_monitor_wrapper(
        cls,
        instrument_id: str,
        raw_file: RawFile | None = None,
        raw_file_original_name: str | None = None,
    ) -> RawFileMonitorWrapper:
        """Create an RawFileMonitorWrapper for the specified instrument and raw file.

        :param instrument_id: The ID of the instrument
        :param raw_file: The RawFile instance of the raw file to monitor
        :return: An instance of the appropriate RawFileMonitorWrapper subclass
        """
        return cls._create_handler(
            MONITOR,
            instrument_id,
            raw_file=raw_file,
            raw_file_original_name=raw_file_original_name,
        )

    @classmethod
    def create_write_wrapper(
        cls,
        raw_file: RawFile,
        path_provider: type[PathProvider],
    ) -> RawFileWriteWrapper:
        """Create a RawFileWriteWrapper for the specified instrument and raw file.

        :param raw_file: a raw file object
        :param path_provider: the path provider class for the operation
        """
        instrument_id = raw_file.instrument_id
        return cls._create_handler(
            COPIER,
            instrument_id,
            raw_file=raw_file,
            path_provider=path_provider,
        )


def get_main_file_size_from_db(raw_file: RawFile) -> int | None:
    """Get the size in bytes of the main file from the `raw_file` object in the database.

    Returns None if the main file is not found in the `raw_file.file_info` dictionary.
    """
    if raw_file.file_info == {}:
        return None

    monitor_wrapper = RawFileWrapperFactory.create_monitor_wrapper(
        instrument_id=raw_file.instrument_id, raw_file=raw_file
    )

    # to enable size lookup in file_info in case of collisions, we need to replace the original name with the raw file id to
    main_file_name = monitor_wrapper.main_file_path().name.replace(
        raw_file.original_name, raw_file.id
    )

    file_sizes = [
        size
        for path, (size, *_hashes) in raw_file.file_info.items()
        # this compares only file names, could be made more robust by comparing relative paths
        if Path(path).name == main_file_name
    ]

    if len(file_sizes) > 1:
        raise AirflowFailException(
            f"Found more than one item for {main_file_name}: got {file_sizes=} from {raw_file.file_info}"
        )

    if len(file_sizes) == 1:
        return file_sizes[0]

    logging.warning(
        f"Main file {main_file_name} not found in raw file {raw_file.file_info}, returning None."
    )

    return None
