"""A custom airflow acquisition monitor.

Wait until file size has not changed for several tries.
"""

import logging

from airflow.sensors.base import BaseSensorOperator
from common.keys import DagContext, DagParams
from raw_data_wrapper import RawDataWrapper

# Currently, this value together with ACQUISITION_MONITOR_POKE_INTERVAL_S determines the time
# that the file size needs to stay constant in order to be regarded as "acquisition done"
NUM_FILE_CHECKS_WITH_SAME_SIZE = 10


class AcquisitionMonitor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id

        self._raw_data_wrapper: RawDataWrapper | None = None
        self._initial_dir_contents: set | None = None

        self._file_sizes = []

    def pre_execute(self, context: dict[str, any]) -> None:
        """_job_id the job id from XCom."""
        raw_file_name = context[DagContext.PARAMS][DagParams.RAW_FILE_NAME]

        self._raw_data_wrapper = RawDataWrapper.create(
            instrument_id=self._instrument_id, raw_file_name=raw_file_name
        )

        self._initial_dir_contents = (
            self._raw_data_wrapper.get_raw_files_on_instrument()
        )

        logging.info(f"Monitoring {self._raw_data_wrapper.file_path_to_watch()}")

    def poke(self, context: dict[str, any]) -> bool:
        """Check if file was created. If so, push the folder contents to xcom and return."""
        del context  # unused

        if (
            new_dir_content := self._raw_data_wrapper.get_raw_files_on_instrument()
            - self._initial_dir_contents
        ):
            logging.info(
                f"New file(s) found: {new_dir_content}. Considering previous acquisition to be done."
            )
            return True

        size = self._raw_data_wrapper.file_path_to_watch().stat().st_size
        logging.info(size)

        self._file_sizes.append(size)

        # TODO: check for size > threshold?
        last_sizes_equal = all(
            size == self._file_sizes[-1]
            for size in self._file_sizes[-NUM_FILE_CHECKS_WITH_SAME_SIZE:]
        )
        if len(self._file_sizes) >= NUM_FILE_CHECKS_WITH_SAME_SIZE and last_sizes_equal:
            logging.info(self._file_sizes)
            return True

        return False
