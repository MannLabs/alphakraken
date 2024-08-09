"""A custom airflow acquisition monitor.

Wait until file size has not changed for several tries.
"""

import logging

from airflow.sensors.base import BaseSensorOperator
from common.keys import DagContext, DagParams
from raw_data_wrapper import RawDataWrapper

# TODO: if this is to be made generic, needs to be time -> save start time, count minutes
NUM_FILE_CHECKS_WITH_SAME_SIZE = 5


class AcquisitionMonitor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id

        self._raw_data_wrapper: RawDataWrapper | None = None

        self._file_sizes = []

    def pre_execute(self, context: dict[str, any]) -> None:
        """_job_id the job id from XCom."""
        raw_file_name = context[DagContext.PARAMS][DagParams.RAW_FILE_NAME]

        self._raw_data_wrapper = RawDataWrapper.create(
            instrument_id=self._instrument_id, raw_file_name=raw_file_name
        )
        logging.info(f"Monitoring {self._raw_data_wrapper.file_path_to_watch()}")

    def poke(self, context: dict[str, any]) -> bool:
        """Check if file was created. If so, push the folder contents to xcom and return."""
        del context  # unused

        size = self._raw_data_wrapper.file_path_to_watch().stat().st_size
        logging.info(size)

        self._file_sizes.append(size)

        # TODO: file timings -> Tim
        # TODO: size > threshold
        # TODO: implement: new file -> acquisition finished, TODO: for zeno: filter
        last_sizes_equal = all(
            size == self._file_sizes[-1]
            for size in self._file_sizes[-NUM_FILE_CHECKS_WITH_SAME_SIZE:]
        )
        if len(self._file_sizes) >= NUM_FILE_CHECKS_WITH_SAME_SIZE and last_sizes_equal:
            logging.info(self._file_sizes)
            return True

        return False
