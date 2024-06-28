"""A custom airflow acquisition monitor.

Wait until file size has not changed for several tries.
"""

import logging
from pathlib import Path

from airflow.sensors.base import BaseSensorOperator
from common.keys import DagContext, DagParams
from common.settings import get_internal_instrument_data_path

NUM_FILE_CHECKS_WITH_SAME_SIZE = 5


class AcquisitionMonitor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._base_path: Path = get_internal_instrument_data_path(instrument_id)
        self._file_sizes = []
        self._file_path_to_watch: Path | None = None

    def pre_execute(self, context: dict[str, any]) -> None:
        """_job_id the job id from XCom."""
        raw_file_name = context[DagContext.PARAMS][DagParams.RAW_FILE_NAME]

        # TODO: this is vendor-specific!
        self._file_path_to_watch = self._base_path / raw_file_name
        logging.info(f"Monitoring {self._file_path_to_watch}")

    def poke(self, context: dict[str, any]) -> bool:
        """Check if file was created. If so, push the folder contents to xcom and return."""
        del context  # unused

        size = self._file_path_to_watch.stat().st_size
        logging.info(size)

        self._file_sizes.append(size)

        last_sizes_equal = all(
            size == self._file_sizes[-1]
            for size in self._file_sizes[-NUM_FILE_CHECKS_WITH_SAME_SIZE:]
        )
        if len(self._file_sizes) >= NUM_FILE_CHECKS_WITH_SAME_SIZE and last_sizes_equal:
            logging.info(self._file_sizes)
            return True

        return False
