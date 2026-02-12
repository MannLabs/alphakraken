"""A custom airflow acquisition monitor.

Wait until acquisition is done.

An acquisition is considered "done" if either:
- exactly one new file has been found
- the main file has not appeared for a certain amount of time (relevant for Bruker only)
- the file size has not changed for a certain amount of time
- (only if Airflow  variable 'consider_old_files_acquired' is set) when the file is "old" (default: > 5h) compared to the
  youngest file. This should be used with caution, but can be handy in case a catchup is required.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz
from airflow.sensors.base import BaseSensorOperator
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
from raw_file_wrapper_factory import RawFileMonitorWrapper, RawFileWrapperFactory

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import RawFile, RawFileStatus

# Soft timeout for the second type of check
SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M: int = 120

# For the third type of check, the file size is calculated every SIZE_CHECK_INTERVAL_M minutes,
# if it has not changed between two checks, the acquisition is considered to be done
# This part of the logic should be triggered only at the end of an acquisition queue,
# so this value is rather conservative and hard-coded for now.
# Note that it takes at least 2*SIZE_CHECK_INTERVAL_M minutes to detect that the acquisition is done that way.
SIZE_CHECK_INTERVAL_M: int = 60

# heuristics: for Zeno ZT scan mode, conversion takes < 24h
ZENO_ZT_SIZE_CHECK_INTERVAL_M: int = 24 * 60

# Zeno ZT scan files remain < 50MB until conversion completes
ZENO_ZT_SCAN_FILE_THRESHOLD_BYTES: int = 50 * 1024 * 1024


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
        self._latest_file_size_check_timestamp: float | None = None
        self._last_file_size = -1

        # to track whether the main file showed up (relevant for Bruker only)
        self._main_file_exists: bool = False

        self._file_is_old: bool = False

    def pre_execute(self, context: dict[str, Any]) -> None:
        """_job_id the job id from XCom."""
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

        self._main_file_path = self._raw_file_monitor_wrapper.main_file_path()

        self._main_file_exists = self._main_file_path.exists()

        self._first_poke_timestamp = get_timestamp()
        self._latest_file_size_check_timestamp = self._first_poke_timestamp

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
                f"{AcquisitionMonitorErrors.MAIN_FILE_MISSING}: {self._raw_file_monitor_wrapper.main_file_name}"
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

        # heuristics: for Zeno ZT scan mode, .wiff.scan is very small until the conversion is done
        # This is very rough, and has some downsides, one being that on failed acquisitions the .wiff.scan file can remain small, which then clogs the pipeline
        is_not_zeno_or_zeno_ready = (
            not self._raw_file.original_name.endswith(".wiff")
            or get_file_size(Path(f"{self._main_file_path}.scan"), -1)
            > ZENO_ZT_SCAN_FILE_THRESHOLD_BYTES
        )

        if self._file_is_old:
            logging.info(
                "File is old compared to the youngest file in the directory. Considering acquisition to be done."
            )
            return is_not_zeno_or_zeno_ready

        if not self._main_file_exists:
            if self._main_file_path.exists():
                self._main_file_exists = True
            else:
                return self._main_file_missing_for_too_long()

        if self._file_size_unchanged_for_time():
            return is_not_zeno_or_zeno_ready or self._file_size_unchanged_for_time(
                ZENO_ZT_SIZE_CHECK_INTERVAL_M
            )

        current_dir_content, new_dir_content = self._get_dir_content()

        # this is the standard case
        if len(new_dir_content) > 0:
            logging.info(f"New file(s) found: {new_dir_content}.")

            if len(new_dir_content) == 1:
                # potential additional check: is the new file "small enough" to be considered a freshly started acquisition
                # but: to adjust the threshold the poke frequency and the data output of the instrument need to be considered
                logging.info("Considering previous acquisition to be done.")

                # Handling the case where the file got renamed by the acquisition software.
                # Deliberately limited to the case of a single new file to avoid false positives on race conditions
                if (
                    self._corrupted_file_name is not None
                    and self._corrupted_file_name in new_dir_content
                ):
                    logging.warning(f"File got renamed to {self._corrupted_file_name}.")
                    self._file_got_renamed = True

                return is_not_zeno_or_zeno_ready

            logging.warning(
                f"More than one new file found: {new_dir_content}. "
                f"This could be due to a manual intervention on the file system."
            )
            self._initial_dir_content = current_dir_content

        return False

    @staticmethod
    def _get_youngest_file_age(
        instrument_data_path: Path, dir_content: set[str]
    ) -> float:
        """Return the age (unix epoch) of the youngest file in the directory, 0 (= very old) if the directory is empty."""
        if not dir_content:
            return 0

        file_ages = [
            get_file_ctime(instrument_data_path / file_name)
            for file_name in dir_content
        ]

        # st_ctime gives epoch timestamp ("age in seconds since 1970")
        return sorted(
            file_ages,
            key=lambda item: item,
            reverse=True,  # youngest first
        )[0]

    @staticmethod
    def _is_older_than_threshold(
        file_path_to_check: Path,
        youngest_file_age: float,
        threshold_h: int = 5,
    ) -> bool:
        """Return true if the file age exceeds the youngest file in the directory by the threshold.

        A relative age between files is used to decouple the time settings on the monitored instrument
        from those of the monitoring system.

        :param file_path_to_check: The file to check
        :param youngest_file_age: The age of the youngest file in the directory
        :param threshold_h: The threshold in hours. The default is picked conservatively.

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

    def _main_file_missing_for_too_long(self) -> bool:
        """Return true if the main file has not appeared for a certain amount of time."""
        time_since_first_check_s = get_timestamp() - self._first_poke_timestamp

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

        return current_dir_content, new_dir_content

    def _file_size_unchanged_for_time(
        self, size_check_interval_m: int = SIZE_CHECK_INTERVAL_M
    ) -> bool:
        """Return true if the file size has not changed for a certain amount of time."""
        time_since_last_check_m = (
            (current_timestamp := get_timestamp())
            - self._latest_file_size_check_timestamp
        ) / 60

        if time_since_last_check_m >= size_check_interval_m:
            size = get_file_size(self._main_file_path)

            if size == self._last_file_size:
                logging.info(
                    f"File size {size} has not changed for {time_since_last_check_m} >= {size_check_interval_m} min. "
                )

                if size == 0:
                    # If a file is opened in the acquisition software, but not written to yet (because of, e.g. an LC error)
                    # the size can remain 0 for a long time. Only once the next acquisition is started, the file is written to
                    # and closed. As this could lead to inconsistencies, the acquisition ist not considered to be done in this case.
                    logging.warning(
                        "File size is 0. Presuming file is still open and acquisition is not done."
                    )
                    return False

                logging.info("Considering acquisition to be done.")
                return True

            self._last_file_size = size
            self._latest_file_size_check_timestamp = current_timestamp

        return False
