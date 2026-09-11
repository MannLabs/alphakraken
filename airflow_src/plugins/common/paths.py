"""Helper methods to construct paths for accessing data."""

from pathlib import Path

from shared.db.models import RawFile
from shared.keys import InternalPaths
from shared.path_layout import get_output_folder_rel_path


def get_internal_instrument_data_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/instruments/test2
    """
    return Path(InternalPaths.MOUNTS_PATH) / InternalPaths.INSTRUMENTS / instrument_id


def get_internal_backup_path() -> Path:
    """Get internal backup path.

    e.g. /opt/airflow/mounts/backup
    """
    return Path(InternalPaths.MOUNTS_PATH) / InternalPaths.BACKUP


def get_internal_backup_path_for_instrument(
    instrument_id: str,
) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/backup/test2
    """
    return get_internal_backup_path() / instrument_id


def get_internal_output_path() -> Path:
    """Get absolute internal output path."""
    return Path(InternalPaths.MOUNTS_PATH) / InternalPaths.OUTPUT


def get_internal_output_path_for_raw_file(
    raw_file: RawFile,
    software_type: str | None = None,
) -> Path:
    """Get absolute internal output path for the given raw file name."""
    return (
        Path(InternalPaths.MOUNTS_PATH)
        / InternalPaths.OUTPUT
        / get_output_folder_rel_path(raw_file, software_type)
    )
