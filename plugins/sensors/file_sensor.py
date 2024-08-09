"""A custom airflow file sensor.

On creation of a new file/folder, push the folder contents to xcom and return
"""

import logging
from pathlib import Path

from airflow.sensors.base import BaseSensorOperator
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.utils.dirsnapshot import DirectorySnapshot

from shared.keys import InstrumentKeys, XComKeys
from shared.settings import INSTRUMENTS, InternalPaths
from shared.utils import put_xcom


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

        self._path_to_watch = (
            Path(InternalPaths.APC_PATH_PREFIX)
            / INSTRUMENTS[instrument_id][InstrumentKeys.RAW_DATA_PATH]
        )
        logging.info(f"Creating FileCreationSensor for {self._path_to_watch}")

        self._event_handler = FileCreationEventHandler()
        self._observer = None

    def poke(self, context: dict[str, any]) -> bool:
        """Check if file was created. If so, push the folder contents to xcom and return."""
        logging.info(f"Checking if file was created in {self._path_to_watch}")

        if self._observer is None:
            logging.info("self.observer is None")
            self._observer = Observer()

        if not self._observer.is_alive():
            logging.info("not self.observer.is_alive()")
            self._observer.schedule(
                self._event_handler, self._path_to_watch, recursive=False
            )
            self._observer.start()

        if self._event_handler.file_created:
            logging.info("self.event_handler.file_created()")
            dir_snapshot = DirectorySnapshot(self._path_to_watch, recursive=False)

            self._observer.stop()
            self._observer.join()

            paths = [str(c) for c in dir_snapshot.paths]
            put_xcom(
                context["task_instance"], XComKeys.DIRECTORY_CONTENT, sorted(paths)
            )

            return True

        return False
