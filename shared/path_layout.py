"""Layout of the data directories, independent of the view they are accessed from."""

from pathlib import Path

from shared.db.models import RawFile, Settings, get_created_at_year_month

OUTPUT_FOLDER_PREFIX = "out_"


def get_raw_file_folder_rel_path(raw_file: RawFile) -> Path:
    """Get the path of the folder holding the given raw file, relative to the `backup` folder.

    E.g. test1/2024_07
    """
    return Path(raw_file.instrument_id) / get_created_at_year_month(raw_file)


def get_raw_file_rel_path(raw_file: RawFile) -> Path:
    """Get the path of the given raw file, relative to the `backup` folder.

    E.g. test1/2024_07/RAW-FILE-1.raw
    """
    return get_raw_file_folder_rel_path(raw_file) / raw_file.id


def get_output_folder_rel_path(
    raw_file: RawFile,
    settings: Settings | None = None,
) -> Path:
    """Get the path of the output directory for given raw file name relative to the `output` folder.

    Only if the raw_file has no project defined, we use a month-specific subfolder
    This is to avoid having too many files in the fallback output folders.

    E.g.
        <project_id>/2024_07/out_RAW-FILE-1.raw/<settings_name>_v<version> in case raw_file has no project ID
        <project_id>/out_RAW-FILE-1.raw/<settings_name>_v<version> in case raw_file has a project ID
    """
    optional_sub_folder = (
        get_created_at_year_month(raw_file) if not raw_file.has_project else ""
    )
    path = (
        Path(raw_file.project_id)
        / optional_sub_folder
        / f"{OUTPUT_FOLDER_PREFIX}{raw_file.id}"
    )
    if settings is not None:
        path = (
            path / f"{settings.name}_v{settings.version}"
        )  # TODO: logic of creating unique settings name could be moved to settings class
    return path
