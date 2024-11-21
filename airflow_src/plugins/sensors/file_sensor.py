"""A custom airflow file sensor.

Wait until creation of a new file or folder.
"""

import logging
from datetime import datetime, timedelta
from typing import Any

import pytz
from airflow.sensors.base import BaseSensorOperator
from common.settings import (
    get_internal_backup_path,
    get_internal_instrument_data_path,
    get_internal_output_path,
)
from common.utils import get_timestamp
from file_handling import get_disk_usage
from raw_file_wrapper_factory import RawFileWrapperFactory

from shared.db.interface import update_kraken_status
from shared.db.models import KrakenStatusValues

# to reduce network traffic, do the health check only every few minutes. If changed, adapt also webapp color code.
HEALTH_CHECK_INTERVAL_M: int = 5

# consider the sensor as "success" after this time.
# This is to avoid the sensor running into a timeout if no files are found, and to recover more quickly in some edge
# cases.
# Note: The downstream tasks need to be able to handle the "no files found" case.
SOFT_FAIL_TIMEOUT_H: int = 24


def _check_health(instrument_id: str) -> None:
    """Check the health of the instrument data, and the output and backup paths and update Kraken status."""
    status_details = []
    data_path = get_internal_instrument_data_path(instrument_id)
    # using rglob to find out if the mount is sane is a bit hacky
    # as it could give false negatives if the folder is empty.
    if not data_path.exists() or not data_path.rglob("*"):
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

    *_, free_space_gb = get_disk_usage(data_path)

    update_kraken_status(
        instrument_id,
        status=KrakenStatusValues.ERROR if status_details else KrakenStatusValues.OK,
        status_details=";".join(status_details),
        free_space_gb=int(free_space_gb),
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
        self._start_time = datetime.now(tz=pytz.utc)

        self._initial_dir_contents: set | None = None
        self._latest_health_check_timestamp: float = 0.0

    def pre_execute(self, context: dict[str, Any]) -> None:
        """Check the health of the instrument data path and backup path."""
        del context  # unused

        logging.info(
            f"Checking for new files since start of this DAG run in {self._raw_file_monitor_wrapper.instrument_path}"
        )

        self._initial_dir_contents = (
            self._raw_file_monitor_wrapper.get_raw_files_on_instrument()
        )

    def poke(self, context: dict[str, Any]) -> bool:
        """Check if file was created. If so, push the folder contents to xcom and return."""
        del context  # unused

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

        if self._start_time + timedelta(hours=SOFT_FAIL_TIMEOUT_H) < datetime.now(
            tz=pytz.utc
        ):
            logging.info("Sensor timed out.")
            return True

        return False
