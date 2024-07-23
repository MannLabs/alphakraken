"""A custom airflow acquisition monitor.

Wait until acquisition is done.

An acquisition is considered "done" if either:
- new files have been found or
- the file size has not changed for a certain amount of time
"""

import logging
from datetime import datetime
from typing import Any

import pytz
from airflow.sensors.base import BaseSensorOperator
from common.keys import DagContext, DagParams
from file_handling import get_file_size
from raw_data_wrapper_factory import RawFileWrapperFactory

from shared.db.interface import update_raw_file
from shared.db.models import RawFileStatus

# For the second type of check, the file size is calculated every SIZE_CHECK_INTERVAL_M minutes,
# if it has not changed between two checks, the acquisition is considered to be done
# This part of the logic is triggered only at the end of an acquisition queue,
# so this value is rather conservative and hard-coded for now.
SIZE_CHECK_INTERVAL_M: int = 10


class AcquisitionMonitor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id

        self._raw_file_name: str | None = None
        self._raw_data_wrapper: RawFileWrapperFactory | None = None
        self._initial_dir_contents: set | None = None

        self._last_poke_timestamp = None
        self._last_file_size = -1

    def pre_execute(self, context: dict[str, any]) -> None:
        """_job_id the job id from XCom."""
        self._raw_file_name = context[DagContext.PARAMS][DagParams.RAW_FILE_ID]

        self._raw_data_wrapper = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id=self._instrument_id, raw_file_name=self._raw_file_name
        )

        self._initial_dir_contents = (
            self._raw_data_wrapper.get_raw_files_on_instrument()
        )

        self._last_poke_timestamp = self._get_timestamp()

        update_raw_file(
            self._raw_file_name, new_status=RawFileStatus.MONITORING_ACQUISITION
        )

        logging.info(
            f"Monitoring {self._raw_data_wrapper.file_path_to_monitor_acquisition()}"
        )

    def post_execute(self, context: dict[str, any], result: Any = None) -> None:  # noqa: ANN401
        """Update the status of the raw file in the database."""
        del context  # unused
        del result  # unused

        update_raw_file(self._raw_file_name, new_status=RawFileStatus.MONITORING_DONE)

    @staticmethod
    def _get_timestamp() -> float:
        """Get the current timestamp."""
        return datetime.now(tz=pytz.utc).timestamp()

    def poke(self, context: dict[str, any]) -> bool:
        """Return True if acquisition is done."""
        del context  # unused

        if not self._raw_data_wrapper.file_path_to_monitor_acquisition().exists():
            # this covers the case that sometimes for bruker, the folder exists, but the main file does not
            logging.info("Main file does not exist yet.")
            return False

        if (
            new_dir_content := self._raw_data_wrapper.get_raw_files_on_instrument()
            - self._initial_dir_contents
        ):
            logging.info(
                f"New file(s) found: {new_dir_content}. Considering previous acquisition to be done."
            )
            return True

        time_since_last_check = (
            current_timestamp := self._get_timestamp()
        ) - self._last_poke_timestamp
        if time_since_last_check / 60 >= SIZE_CHECK_INTERVAL_M:
            size = get_file_size(
                self._raw_data_wrapper.file_path_to_monitor_acquisition()
            )
            logging.info(f"File size: {size}")

            # TODO: check for size > threshold?
            if size == self._last_file_size:
                logging.info(
                    f"File size {size} has not changed for >= {SIZE_CHECK_INTERVAL_M} min. "
                    f"Considering acquisition to be done."
                )
                return True

            self._last_file_size = size
            self._last_poke_timestamp = current_timestamp

        return False
