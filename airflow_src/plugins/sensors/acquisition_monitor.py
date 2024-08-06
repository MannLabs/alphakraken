"""A custom airflow acquisition monitor.

Wait until acquisition is done.

An acquisition is considered "done" if either:
- new files have been found or
- the main file has not appeared for a certain amount of time (relevant for Bruker only)
- the file size has not changed for a certain amount of time
"""

import logging
from datetime import datetime
from typing import Any

import pytz
from airflow.sensors.base import BaseSensorOperator
from common.keys import AcquisitionMonitorErrors, DagContext, DagParams, XComKeys
from common.utils import put_xcom
from file_handling import get_file_size
from raw_file_wrapper_factory import RawFileMonitorWrapper, RawFileWrapperFactory

from shared.db.interface import update_raw_file
from shared.db.models import RawFileStatus

# Soft timeout for the second type of check
SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M: int = 60

# For the third type of check, the file size is calculated every SIZE_CHECK_INTERVAL_M minutes,
# if it has not changed between two checks, the acquisition is considered to be done
# This part of the logic is triggered only at the end of an acquisition queue,
# so this value is rather conservative and hard-coded for now.
SIZE_CHECK_INTERVAL_M: int = 60


class AcquisitionMonitor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id

        self._raw_file_name: str | None = None
        self._raw_file_monitor_wrapper: RawFileMonitorWrapper | None = None
        self._initial_dir_contents: set | None = None

        self._first_poke_timestamp: float | None = None
        self._last_poke_timestamp: float | None = None
        self._last_file_size = -1

        # to track whether the main file showed up (relevant for Bruker only)
        self._main_file_exists = False

    def pre_execute(self, context: dict[str, any]) -> None:
        """_job_id the job id from XCom."""
        self._raw_file_name = context[DagContext.PARAMS][DagParams.RAW_FILE_ID]

        self._raw_file_monitor_wrapper = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id=self._instrument_id, raw_file_name=self._raw_file_name
        )

        self._initial_dir_contents = (
            self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
        )

        self._first_poke_timestamp = self._get_timestamp()
        self._last_poke_timestamp = self._first_poke_timestamp

        update_raw_file(
            self._raw_file_name, new_status=RawFileStatus.MONITORING_ACQUISITION
        )

        logging.info(
            f"Monitoring {self._raw_file_monitor_wrapper.file_path_to_monitor_acquisition()}"
        )

    def post_execute(self, context: dict[str, any], result: Any = None) -> None:  # noqa: ANN401
        """Update the status of the raw file in the database."""
        del result  # unused

        acquisition_monitor_errors = (
            []
            if self._main_file_exists
            else [AcquisitionMonitorErrors.MAIN_FILE_MISSING]
        )
        put_xcom(
            context["ti"],
            XComKeys.ACQUISITION_MONITOR_ERRORS,
            acquisition_monitor_errors,
        )

        update_raw_file(self._raw_file_name, new_status=RawFileStatus.MONITORING_DONE)

    @staticmethod
    def _get_timestamp() -> float:
        """Get the current timestamp."""
        return datetime.now(tz=pytz.utc).timestamp()

    def poke(self, context: dict[str, any]) -> bool:
        """Return True if acquisition is done."""
        del context  # unused

        if not self._main_file_exists:
            if self._raw_file_monitor_wrapper.file_path_to_monitor_acquisition().exists():
                self._main_file_exists = True
            else:
                if self._main_file_missing_for_too_long():
                    return True
                return False

        if self._new_files_found():
            return True

        if self._file_size_unchanged_for_some_time():
            return True

        return False

    def _main_file_missing_for_too_long(self) -> bool:
        """Return true if the main file has not appeared for a certain amount of time."""
        time_since_first_check_s = (self._get_timestamp()) - self._first_poke_timestamp

        if time_since_first_check_s / 60 >= SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M:
            logging.info(
                f"Main file has not shown up for >= {SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M} min. Assuming failed acquisition."
            )
            return True

        logging.info("Main file does not exist yet.")

        return False

    def _new_files_found(self) -> bool:
        """Return true if new files have been found."""
        if (
            new_dir_content
            := self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
            - self._initial_dir_contents
        ):
            logging.info(
                f"New file(s) found: {new_dir_content}. Considering previous acquisition to be done."
            )
            return True
        return False

    def _file_size_unchanged_for_some_time(self) -> bool:
        """Return true if the file size has not changed for a certain amount of time."""
        time_since_last_check_s = (
            current_timestamp := self._get_timestamp()
        ) - self._last_poke_timestamp
        if time_since_last_check_s / 60 >= SIZE_CHECK_INTERVAL_M:
            size = get_file_size(
                self._raw_file_monitor_wrapper.file_path_to_monitor_acquisition()
            )
            logging.info(f"File size: {size}")

            if size == self._last_file_size:
                logging.info(
                    f"File size {size} has not changed for >= {SIZE_CHECK_INTERVAL_M} min. "
                    f"Considering acquisition to be done."
                )
                return True

            self._last_file_size = size
            self._last_poke_timestamp = current_timestamp

        return False
