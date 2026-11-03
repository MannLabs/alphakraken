"""A custom airflow acquisition monitor.

Wait until acquisition is done.

An acquisition is considered "done" if either:
- exactly one new file has been found and all associated files have been unchanged for a short time since then
- all associated files have been unchanged for a long time (end of an acquisition queue)
- the main file has not appeared for a certain amount of time (relevant for Bruker only)
- the file got renamed by the acquisition software, indicating a failed acquisition
- (only if Airflow  variable 'consider_old_files_acquired' is set) when the file is "old" (default: > 5h) compared to the
  youngest file. This should be used with caution, but can be handy in case a catchup is required.

In the first, second and last case, the acquisition software must additionally have finalized the raw file
(cf. _acquisition_finalized()), unless the files have been unchanged for a very long time.
"""

import logging
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz
from airflow.sdk import BaseSensorOperator
from common.keys import (
    AcquisitionMonitorErrors,
    AirflowVars,
    DagContext,
    DagParams,
    XComKeys,
)
from common.paths import get_internal_instrument_data_path
from common.utils import get_airflow_variable, get_timestamp, put_xcom
from file_handling import get_file_ctime, get_file_size
from raw_file_wrapper_factory import (
    RawFileMonitorWrapper,
    RawFileWrapperFactory,
    SciexRawFileMonitorWrapper,
)

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import RawFile, RawFileStatus

SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M: int = 120

# After the next file appeared, all associated files must be unchanged for at least this long,
# as the acquisition software might still be writing to them.
FILES_SETTLE_TIME_S: int = 5

# If all associated files are unchanged for this long, the acquisition is considered done even without a next file.
# This should only trigger at the end of an acquisition queue, so this value is rather conservative and hard-coded for now.
FILES_UNCHANGED_TIMEOUT_M: int = 60

# If all associated files are unchanged for this long, the acquisition is considered done even if not finalized.
# heuristics: for Zeno ZT scan mode, conversion takes < 24h
FINALIZATION_TIMEOUT_M: int = 24 * 60

# Zeno ZT scan files remain small until conversion completes
ZENO_ZT_SCAN_FILE_THRESHOLD_BYTES: int = 44 + 100  # 44 magic bytes + buffer

# Files older than the youngest file in the directory by this margin are considered acquired, cf. 'consider_old_files_acquired'.
# Picked conservatively.
OLD_FILE_THRESHOLD_H: int = 5


class AcquisitionMonitor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id

        self._raw_file: RawFile | None = None
        self._raw_file_monitor_wrapper: RawFileMonitorWrapper | None = None
        self._initial_dir_content: set | None = None
        self._main_file_path: Path | None = None
        self._corrupted_file_name: str | None = None
        self._file_got_renamed: bool = False

        self._first_poke_timestamp: float | None = None

        self._files_stat: dict[Path, tuple[int, float]] | None = None
        self._files_unchanged_since: float | None = None
        self._next_file_appeared: bool = False

        # to track whether the main file showed up (relevant for Bruker only)
        self._main_file_exists: bool = False

        self._file_is_old: bool = False

    def pre_execute(self, context: dict[str, Any]) -> None:
        """Initialize the monitoring state from the raw file and the instrument directory."""
        raw_file_id = context[DagContext.PARAMS][DagParams.RAW_FILE_ID]
        self._raw_file = get_raw_file_by_id(raw_file_id)

        self._raw_file_monitor_wrapper = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id=self._instrument_id, raw_file=self._raw_file
        )

        self._corrupted_file_name = (
            self._raw_file_monitor_wrapper.get_corrupted_file_name()
        )

        self._initial_dir_content = (
            self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
        )
        logging.info(f"Initial directory content: {self._initial_dir_content}")

        self._main_file_path = self._raw_file_monitor_wrapper.main_file_path()

        self._main_file_exists = self._main_file_path.exists()

        self._first_poke_timestamp = get_timestamp()

        update_raw_file(
            self._raw_file.id, new_status=RawFileStatus.MONITORING_ACQUISITION
        )

        if (
            get_airflow_variable(
                AirflowVars.CONSIDER_OLD_FILES_ACQUIRED, "false"
            ).lower()
            == "true"
        ) and self._main_file_exists:
            youngest_file_age = self._get_youngest_file_age(
                get_internal_instrument_data_path(self._instrument_id),
                self._initial_dir_content,
            )

            self._file_is_old = self._is_older_than_threshold(
                self._main_file_path, youngest_file_age
            )

        logging.info(f"Monitoring {self._main_file_path}")

    def post_execute(self, context: dict[str, Any], result: Any = None) -> None:
        """Update the status of the raw file in the database."""
        del result  # unused

        assert self._raw_file_monitor_wrapper is not None
        assert self._raw_file is not None

        acquisition_monitor_errors = []
        if not self._main_file_exists:
            acquisition_monitor_errors += [
                f"{AcquisitionMonitorErrors.MAIN_FILE_MISSING}: {self._main_file_path.name}"
            ]
        if self._file_got_renamed:
            acquisition_monitor_errors += [
                f"{AcquisitionMonitorErrors.FILE_GOT_RENAMED}: {self._corrupted_file_name}"
            ]

        put_xcom(
            context["ti"],
            XComKeys.ACQUISITION_MONITOR_ERRORS,
            acquisition_monitor_errors,
        )

        update_raw_file(self._raw_file.id, new_status=RawFileStatus.MONITORING_DONE)

    def poke(self, context: dict[str, Any]) -> bool:
        """Return True if acquisition is done."""
        del context  # unused

        assert self._main_file_path is not None
        assert self._raw_file_monitor_wrapper is not None

        now = get_timestamp()

        if self._file_is_old:
            logging.info(
                "File is old compared to the youngest file in the directory. Considering acquisition to be done."
            )
            return self._acquisition_finalized()

        if not self._main_file_exists:
            if self._main_file_path.exists():
                self._main_file_exists = True
            else:
                return self._main_file_missing_for_too_long(now)

        files_unchanged_s = self._update_files_stat(now)

        if files_unchanged_s >= FILES_UNCHANGED_TIMEOUT_M * 60:
            return self._files_unchanged_for_too_long(files_unchanged_s)

        return self._next_file_appeared_and_files_settled(now, files_unchanged_s)

    def _acquisition_finalized(self) -> bool:
        """Return whether the acquisition software has finalized the raw file after writing it.

        heuristics: for Zeno ZT scan mode (Sciex), the .wiff.scan file (= main file) is very small until the conversion is done.
        This is very rough, and has some downsides, one being that on failed acquisitions the .wiff.scan file can remain
        small, which then clogs the pipeline.

        TODO: remove once Sciex improved this
        """
        if not isinstance(self._raw_file_monitor_wrapper, SciexRawFileMonitorWrapper):
            return True

        return (
            get_file_size(self._main_file_path, -1, verbose=False)
            > ZENO_ZT_SCAN_FILE_THRESHOLD_BYTES
        )

    def _files_unchanged_for_too_long(self, files_unchanged_s: float) -> bool:
        """Return true if the acquisition is considered done because the associated files did not change for too long."""
        if self._files_stat and self._get_total_file_size() == 0:
            # If a file is opened in the acquisition software, but not written to yet (because of, e.g. an LC error)
            # the size can remain 0 for a long time. Only once the next acquisition is started, the file is written to
            # and closed. As this could lead to inconsistencies, the acquisition ist not considered to be done in this case.
            logging.warning(
                "Total file size is 0. Presuming file is still open and acquisition is not done."
            )
            return False

        if (
            files_unchanged_s < FINALIZATION_TIMEOUT_M * 60
            and not self._acquisition_finalized()
        ):
            logging.info(
                f"Files unchanged for >= {FILES_UNCHANGED_TIMEOUT_M} min, but acquisition not finalized yet."
            )
            return False

        logging.info(
            f"Files unchanged for >= {FILES_UNCHANGED_TIMEOUT_M} min. Considering acquisition to be done."
        )
        return True

    def _next_file_appeared_and_files_settled(
        self, now: float, files_unchanged_s: float
    ) -> bool:
        """Return true if the acquisition is considered done because the next file appeared and the associated files settled since.

        Also true if the file got renamed by the acquisition software.
        """
        current_dir_content, new_dir_content = self._get_dir_content()

        # Handling the case where the file got renamed by the acquisition software. Not restricted to a
        # single new file, as the exact name match makes false positives from race conditions unlikely.
        if (
            self._corrupted_file_name is not None
            and self._corrupted_file_name in new_dir_content
        ):
            logging.warning(f"File got renamed to {self._corrupted_file_name}.")
            self._file_got_renamed = True
            return True

        # corrupted files of other raw files are not the next acquisition
        new_dir_content = {
            file_name
            for file_name in new_dir_content
            if not self._raw_file_monitor_wrapper.is_corrupted_file_name(file_name)
        }

        if not new_dir_content:
            return False

        logging.info(f"New file(s) found: {new_dir_content}.")

        if len(new_dir_content) > 1 and not self._next_file_appeared:
            logging.warning(
                f"More than one new file found: {new_dir_content}. "
                f"This could be due to a manual intervention on the file system."
            )
            # accepting the new content as 'initial' to be able to fire on the next new file
            self._initial_dir_content = current_dir_content
            return False

        # this is the standard case
        if not self._next_file_appeared:
            self._next_file_appeared = True
            # the files must settle from this moment on, regardless of how long they were unchanged before
            self._files_unchanged_since = now
            return False

        if files_unchanged_s < FILES_SETTLE_TIME_S:
            return False

        logging.info(
            "Files have settled after the next file appeared. Considering previous acquisition to be done."
        )
        return self._acquisition_finalized()

    @staticmethod
    def _get_youngest_file_age(
        instrument_data_path: Path, dir_content: set[str]
    ) -> float:
        """Return the age (unix epoch) of the youngest file in the directory, 0 (= very old) if no file exists."""
        file_ages = []
        for file_name in dir_content:
            try:
                file_age = get_file_ctime(instrument_data_path / file_name)
            except FileNotFoundError:
                # e.g. moved away by the file mover in the meantime
                continue
            file_ages.append(file_age)

        # st_ctime gives epoch timestamp ("age in seconds since 1970"), so the largest value is the youngest file
        return max(file_ages, default=0)

    @staticmethod
    def _is_older_than_threshold(
        file_path_to_check: Path,
        youngest_file_age: float,
        threshold_h: int = OLD_FILE_THRESHOLD_H,
    ) -> bool:
        """Return true if the file age exceeds the youngest file in the directory by the threshold.

        A relative age between files is used to decouple the time settings on the monitored instrument
        from those of the monitoring system.

        :param file_path_to_check: The file to check
        :param youngest_file_age: The age of the youngest file in the directory
        :param threshold_h: The threshold in hours.

        :return: True if the file is older than the youngest file in the directory by the threshold.
        """
        logging.info(
            f"Youngest file in directory: {datetime.fromtimestamp(youngest_file_age, tz=pytz.utc)}"
        )

        age_file_path_to_check_age = get_file_ctime(file_path_to_check)
        age_difference_in_h = (youngest_file_age - age_file_path_to_check_age) / 3600
        logging.info(
            f"Current file: {datetime.fromtimestamp(age_file_path_to_check_age, tz=pytz.utc)} ({age_difference_in_h} h)"
        )

        return age_difference_in_h > threshold_h

    def _main_file_missing_for_too_long(self, now: float) -> bool:
        """Return true if the main file has not appeared for a certain amount of time."""
        time_since_first_check_s = now - self._first_poke_timestamp

        if time_since_first_check_s / 60 >= SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M:
            logging.info(
                f"Main file has not shown up for >= {SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M} min. Assuming failed acquisition."
            )
            return True

        logging.info("Main file does not exist yet.")

        return False

    def _get_dir_content(self) -> tuple[set[str], set[str]]:
        """Return current and new directory content."""
        assert self._raw_file_monitor_wrapper is not None
        assert self._initial_dir_content is not None

        current_dir_content = (
            self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
        )

        new_dir_content = current_dir_content - self._initial_dir_content
        removed_dir_content = self._initial_dir_content - current_dir_content

        if new_dir_content or removed_dir_content:
            logging.info(
                f"Changed directory content. + {new_dir_content} / - {removed_dir_content}"
            )

        return current_dir_content, new_dir_content

    def _update_files_stat(self, now: float) -> float:
        """Refresh the (size, mtime) snapshot of all associated files and return the seconds since it last changed."""
        files_stat = self._get_associated_files_stat()

        if files_stat != self._files_stat:
            logging.info(
                f"Files changed: {len(files_stat)} file(s), total size {sum(size for size, _ in files_stat.values())}"
            )
            self._files_stat = files_stat
            self._files_unchanged_since = now

        return now - self._files_unchanged_since

    def _get_total_file_size(self) -> int:
        """Return the total size of all associated files in the current snapshot."""
        assert self._files_stat is not None

        return sum(size for size, _ in self._files_stat.values())

    def _get_associated_files_stat(self) -> dict[Path, tuple[int, float]]:
        """Return the (size, mtime) of all existing files associated with the raw file."""
        assert self._raw_file_monitor_wrapper is not None
        assert self._raw_file is not None

        # a single stat per entry to reduce load on network share
        associated_paths = self._raw_file_monitor_wrapper.get_associated_paths(
            self._raw_file_monitor_wrapper.instrument_path,
            self._raw_file.original_name,
        )

        files_stat = {}
        for path in associated_paths:
            try:
                st = path.stat()
            except FileNotFoundError:
                # vanished between listing and stat
                continue
            if self._is_regular_file(st):
                files_stat[path] = (st.st_size, st.st_mtime)

        return files_stat

    @staticmethod
    def _is_regular_file(st: os.stat_result) -> bool:
        """Return whether the stat result belongs to a regular file (not a directory, symlink, etc.)."""
        # rglob also yields subfolders, whose size is filesystem-dependent
        return stat.S_ISREG(st.st_mode)
