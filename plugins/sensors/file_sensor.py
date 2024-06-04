"""A custom airflow file sensor.

Wait until creation of a new file or folder.
"""

import logging

from airflow.sensors.base import BaseSensorOperator
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from shared.utils import get_instrument_data_path


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

        self._path_to_watch = get_instrument_data_path(instrument_id)
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

        logging.info(f"Checking for new files in {self._path_to_watch}")

        if self._event_handler.file_created:
            logging.info("self.event_handler.file_created()")

            self._observer.stop()
            self._observer.join()

            return True

        return False
