"""Helper methods to construct paths for accessing data."""

from pathlib import Path

from common.constants import (
    OUTPUT_FOLDER_PREFIX,
)

from shared.db.models import RawFile, get_created_at_year_month
from shared.keys import InternalPaths


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


def get_output_folder_rel_path(raw_file: RawFile, project_id_or_fallback: str) -> Path:
    """Get the path of the output directory for given raw file name relative to the `output` folder.

    Only if the raw_file has no project defined, we use a month-specific subfolder
    This is to avoid having too many files in the fallback output folders.

    E.g.
        <project_id_or_fallback>/2024_07/out_RAW-FILE-1.raw in case raw_file has no project ID
        <project_id_or_fallback>/out_RAW-FILE-1.raw in case raw_file has a project ID
    """
    optional_sub_folder = (
        get_created_at_year_month(raw_file) if raw_file.project_id is None else ""
    )
    return (
        Path(project_id_or_fallback)
        / optional_sub_folder
        / f"{OUTPUT_FOLDER_PREFIX}{raw_file.id}"
    )


def get_internal_output_path() -> Path:
    """Get absolute internal output path."""
    return Path(InternalPaths.MOUNTS_PATH) / InternalPaths.OUTPUT


def get_internal_output_path_for_raw_file(
    raw_file: RawFile, project_id_or_fallback: str
) -> Path:
    """Get absolute internal output path for the given raw file name."""
    return (
        Path(InternalPaths.MOUNTS_PATH)
        / InternalPaths.OUTPUT
        / get_output_folder_rel_path(raw_file, project_id_or_fallback)
    )
