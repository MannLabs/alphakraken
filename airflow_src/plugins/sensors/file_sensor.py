"""A custom airflow file sensor.

Wait until creation of a new file or folder.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytz
from airflow.sensors.base import BaseSensorOperator
from common.paths import (
    get_internal_backup_path,
    get_internal_backup_path_for_instrument,
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
    _check_path_health(data_path, "data", status_details)

    backup_path = get_internal_backup_path_for_instrument(instrument_id)
    _check_path_health(backup_path, "backup", status_details)

    output_path = get_internal_output_path()
    _check_path_health(output_path, "output", status_details)

    *_, free_space_gb = get_disk_usage(data_path)

    update_kraken_status(
        instrument_id,
        status=KrakenStatusValues.ERROR if status_details else KrakenStatusValues.OK,
        status_details="; ".join(status_details),
        free_space_gb=int(free_space_gb),
    )

    # Update backup filesystem status
    *_, backup_free_space_gb = get_disk_usage(get_internal_backup_path())
    update_kraken_status(
        "backup",
        status="",
        status_details="",
        free_space_gb=int(backup_free_space_gb),
        type_="file_system",
    )

    # Update output filesystem status
    *_, output_free_space_gb = get_disk_usage(output_path)
    update_kraken_status(
        "output",
        status="",
        status_details="",
        free_space_gb=int(output_free_space_gb),
        type_="file_system",
    )


def _check_path_health(path: Path, description: str, status_details: list[str]) -> None:
    """Check the health of a path and add status details if necessary."""
    is_mount = has_files = None

    # Note: using rglob could give false negatives if the folder is empty
    if (
        not (exists := path.exists())
        or not (is_mount := path.is_mount())
        or not (has_files := (any(True for _ in path.rglob("*"))))
    ):
        logging.warning(
            f"Path {path} failed checks: {exists=} {is_mount=} {has_files=}"
        )
        status_details.append(
            f"{description} path not healthy ({exists=} {is_mount=} {has_files=})"
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

        # check health upfront here, otherwise some errors might be reported in an ugly way in the next line
        _check_health(self._instrument_id)
        self._latest_health_check_timestamp = get_timestamp()

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
