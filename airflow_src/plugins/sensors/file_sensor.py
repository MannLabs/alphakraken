"""A custom airflow file sensor.

Wait until creation of a new file or folder.
"""

import logging

from airflow.sensors.base import BaseSensorOperator
from common.settings import (
    get_internal_backup_path,
    get_internal_instrument_data_path,
    get_internal_output_path,
)
from common.utils import get_timestamp
from raw_file_wrapper_factory import RawFileWrapperFactory

from shared.db.interface import update_kraken_status
from shared.db.models import KrakenStatusValues

HEALTH_CHECK_INTERVAL_M: int = 5


def _check_health(instrument_id: str) -> None:
    """Check the health of the instrument data path and backup path. Update Kraken status."""
    status_details = []
    data_path = get_internal_instrument_data_path(instrument_id)
    if not data_path.exists():
        logging.error(f"Data path {data_path} does not exist.")
        status_details.append("Instrument path not found.")

    backup_path = get_internal_backup_path()
    if not backup_path.exists():
        logging.error(f"Backup path {backup_path} does not exist.")
        status_details.append("Backup path not found.")

    output_path = get_internal_output_path()
    if not output_path.exists():
        logging.error(f"Output path {output_path} does not exist.")
        status_details.append("Output path not found.")

    update_kraken_status(
        instrument_id,
        status=KrakenStatusValues.ERROR if status_details else KrakenStatusValues.OK,
        status_details=";".join(status_details),
    )


class FileCreationSensor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id

        self._raw_file_monitor_wrapper = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id=instrument_id
        )

        self._initial_dir_contents: set | None = None
        self._latest_health_check_timestamp = 0

    def pre_execute(self, context: dict[str, any]) -> None:
        """Check the health of the instrument data path and backup path."""
        del context  # unused

        self._initial_dir_contents = (
            self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
        )

    def poke(self, context: dict[str, any]) -> bool:
        """Check if file was created. If so, push the folder contents to xcom and return."""
        del context  # unused

        logging.info(
            f"Checking for new files since start of this DAG run in {self._raw_file_monitor_wrapper.instrument_path}"
        )

        if (
            current_timestamp := get_timestamp()
        ) - self._latest_health_check_timestamp >= HEALTH_CHECK_INTERVAL_M * 60:
            _check_health(self._instrument_id)
            self._latest_health_check_timestamp = current_timestamp

        if (
            new_dir_content
            := self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
            - self._initial_dir_contents
        ):
            logging.info(f"got new dir_content {new_dir_content}")
            return True

        return False
