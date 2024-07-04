"""A custom airflow file sensor.

Wait until creation of a new file or folder.
"""

import logging

from airflow.sensors.base import BaseSensorOperator
from common.settings import (
    get_internal_backup_path,
    get_internal_instrument_data_path,
)
from watchdog.events import FileSystemEvent, FileSystemEventHandler

# Over the network, and on some Mac/Docker configurations,
# the dynamically chosen Observer does not work.
# TODO: fix this or get rid of watchdog again
# from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver as Observer

from shared.db.interface import update_kraken_status
from shared.db.models import KrakenStatusValues


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

    update_kraken_status(
        instrument_id,
        status=KrakenStatusValues.ERROR if status_details else KrakenStatusValues.OK,
        status_details=";".join(status_details),
    )


class FileCreationEventHandler(FileSystemEventHandler):
    """Event handler for file creation."""

    def __init__(self) -> None:
        """Initialize the event handler."""
        self.file_created = False

    def on_created(self, event: FileSystemEvent) -> None:
        """Set flag on file creation."""
        del event  # unused
        self.file_created = True


class FileCreationSensor(BaseSensorOperator):
    """Sensor to check for file creation."""

    def __init__(self, instrument_id: str, *args, **kwargs) -> None:
        """Initialize the sensor."""
        super().__init__(*args, **kwargs)

        self._instrument_id = instrument_id
        self._path_to_watch = get_internal_instrument_data_path(instrument_id)
        logging.info(f"Creating FileCreationSensor for {self._path_to_watch}")

        self._event_handler = FileCreationEventHandler()
        self._observer = None

    def poke(self, context: dict[str, any]) -> bool:
        """Check if file was created. If so, push the folder contents to xcom and return."""
        del context  # unused

        if self._observer is None:
            logging.info("self.observer is None")
            self._observer = Observer()

        if not self._observer.is_alive():
            logging.info("not self.observer.is_alive()")
            self._observer.schedule(
                self._event_handler, str(self._path_to_watch), recursive=False
            )
            self._observer.start()

        logging.info(
            f"Checking for new files since start of this DAG run in {self._raw_data_wrapper.instrument_path}"
        )

        _check_health(self._instrument_id)

        if self._event_handler.file_created:
            logging.info("self.event_handler.file_created()")

            self._observer.stop()
            self._observer.join()

            return True

        return False
