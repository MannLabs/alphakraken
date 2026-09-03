"""Helper methods to construct paths for accessing data."""

from pathlib import Path

from shared.db.models import RawFile
from shared.path_layout import get_output_folder_rel_path
from shared.path_views import AIRFLOW_CONTAINER_VIEW, Locations


def get_internal_instrument_data_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/instruments/test2
    """
    return AIRFLOW_CONTAINER_VIEW.resolve(Locations.INSTRUMENTS, instrument_id)


def get_internal_backup_path() -> Path:
    """Get internal backup path.

    e.g. /opt/airflow/mounts/backup
    """
    return AIRFLOW_CONTAINER_VIEW.resolve(Locations.BACKUP)


def get_internal_backup_path_for_instrument(
    instrument_id: str,
) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/backup/test2
    """
    return AIRFLOW_CONTAINER_VIEW.resolve(Locations.BACKUP, instrument_id)


def get_internal_output_path() -> Path:
    """Get absolute internal output path."""
    return AIRFLOW_CONTAINER_VIEW.resolve(Locations.OUTPUT)


def get_internal_output_path_for_raw_file(
    raw_file: RawFile,
    software_type: str | None = None,
) -> Path:
    """Get absolute internal output path for the given raw file name."""
    return AIRFLOW_CONTAINER_VIEW.resolve(
        Locations.OUTPUT, get_output_folder_rel_path(raw_file, software_type)
    )
