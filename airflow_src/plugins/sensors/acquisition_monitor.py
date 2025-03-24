"""A custom airflow acquisition monitor.

Wait until acquisition is done.

An acquisition is considered "done" if either:
- exactly one new file has been found
- the main file has not appeared for a certain amount of time (relevant for Bruker only)
- the file size has not changed for a certain amount of time
"""

import logging
from pathlib import Path
from typing import Any

from airflow.sensors.base import BaseSensorOperator
from common.keys import AcquisitionMonitorErrors, DagContext, DagParams, XComKeys
from common.paths import get_internal_instrument_data_path
from common.utils import get_timestamp, put_xcom
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


class AcquisitionMonitor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id

        self._raw_file: RawFile | None = None
        self._raw_file_monitor_wrapper: RawFileMonitorWrapper | None = None
        self._initial_dir_contents: set | None = None

        self._first_poke_timestamp: float | None = None
        self._latest_file_size_check_timestamp: float | None = None
        self._last_file_size = -1

        # to track whether the main file showed up (relevant for Bruker only)
        self._main_file_exists = False

    def pre_execute(self, context: dict[str, Any]) -> None:
        """_job_id the job id from XCom."""
        raw_file_id = context[DagContext.PARAMS][DagParams.RAW_FILE_ID]
        self._raw_file = get_raw_file_by_id(raw_file_id)

        self._raw_file_monitor_wrapper = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id=self._instrument_id, raw_file=self._raw_file
        )

        self._initial_dir_contents = (
            self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
        )

        self._first_poke_timestamp = get_timestamp()
        self._latest_file_size_check_timestamp = self._first_poke_timestamp

        update_raw_file(
            self._raw_file.id, new_status=RawFileStatus.MONITORING_ACQUISITION
        )

        logging.info(
            f"Monitoring {self._raw_file_monitor_wrapper.file_path_to_monitor_acquisition()}"
        )

    def post_execute(self, context: dict[str, Any], result: Any = None) -> None:  # noqa: ANN401
        """Update the status of the raw file in the database."""
        del result  # unused

        acquisition_monitor_errors = (
            []
            if self._main_file_exists
            else [
                f"{AcquisitionMonitorErrors.MAIN_FILE_MISSING}{self._raw_file_monitor_wrapper.main_file_name}"
            ]
        )
        put_xcom(
            context["ti"],
            XComKeys.ACQUISITION_MONITOR_ERRORS,
            acquisition_monitor_errors,
        )

        update_raw_file(self._raw_file.id, new_status=RawFileStatus.MONITORING_DONE)

    def poke(self, context: dict[str, Any]) -> bool:
        """Return True if acquisition is done."""
        del context  # unused

        file_path_to_monitor = (
            self._raw_file_monitor_wrapper.file_path_to_monitor_acquisition()
        )

        if not self._main_file_exists:
            if file_path_to_monitor.exists():
                self._main_file_exists = True
            else:
                return self._main_file_missing_for_too_long()

        if self._file_size_unchanged_for_some_time():
            return True

        current_dir_content, new_dir_content = self._get_dir_content()

        if not self._is_older_than_threshold(
            file_path_to_monitor,
            current_dir_content,
            get_internal_instrument_data_path(self._instrument_id),
        ):
            logging.info(
                f"Current file {file_path_to_monitor} is old compared to the youngest. Assuming acquisition is done."
            )
            # return True

        if len(new_dir_content) > 0:
            logging.info(f"New file(s) found: {new_dir_content}.")

            if len(new_dir_content) == 1:
                # potential additional check: is the new file "small enough" to be considered a freshly started acquisition
                # but: to adjust the threshold the poke frequency and the data output of the instrument need to be considered
                logging.info("Considering previous acquisition to be done.")
                return True

            logging.warning(
                f"More than one new file found: {new_dir_content}. "
                f"This could be due to a manual intervention on the file system."
            )
            self._initial_dir_contents = current_dir_content

        return False

    @staticmethod
    def _is_older_than_threshold(
        file_path_to_check: Path,
        current_dir_content: set[str],
        instrument_data_path: Path,
        threshold_h: int = 5,
    ) -> bool:
        """Return true if the file age exceeds the youngest file in the directory by the threshold.

        :param file_path_to_check: The file to check
        :param current_dir_content: The current directory content
        :param threshold_h: The threshold in hours

        :return: True if the file is older than the youngest file in the directory by the threshold.
        """
        if len(current_dir_content) == 0:
            return False

        files_with_ctime = [
            (file_name, get_file_ctime(instrument_data_path / file_name))
            for file_name in current_dir_content
        ]

        # st_ctime gives epoch timestamp ("age")
        files_youngest_first = [
            (file_path, age)
            for file_path, age in sorted(
                files_with_ctime, key=lambda item: item[1], reverse=False
            )
        ]

        logging.info(f"Files in directory: {files_youngest_first}")

        _, youngest_age = files_youngest_first[0]
        file_ages_h = [
            (file_path, abs(age - youngest_age) / 3600)
            for file_path, age in files_youngest_first
        ]

        files_older_than_threshold = [
            file_path for file_path, age in file_ages_h if age > threshold_h
        ]

        logging.info(f"Checking if {file_path_to_check.name} in {files_youngest_first}")

        return file_path_to_check.name in files_older_than_threshold

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
        current_dir_content = (
            self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
        )

        new_dir_content = current_dir_content - self._initial_dir_contents

        return current_dir_content, new_dir_content

    def _file_size_unchanged_for_some_time(self) -> bool:
        """Return true if the file size has not changed for a certain amount of time."""
        time_since_last_check_m = (
            (current_timestamp := get_timestamp())
            - self._latest_file_size_check_timestamp
        ) / 60

        if time_since_last_check_m >= SIZE_CHECK_INTERVAL_M:
            size = get_file_size(
                self._raw_file_monitor_wrapper.file_path_to_monitor_acquisition()
            )

            if size == self._last_file_size:
                logging.info(
                    f"File size {size} has not changed for {time_since_last_check_m} >= {SIZE_CHECK_INTERVAL_M} min. "
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
