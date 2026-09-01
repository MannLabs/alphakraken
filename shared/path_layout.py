"""Layout of the data directories, independent of the view they are accessed from."""

from pathlib import Path

from shared.db.models import RawFile, get_created_at_year_month

OUTPUT_FOLDER_PREFIX = "out_"


def get_output_folder_rel_path(
    raw_file: RawFile,
    software_type: str | None = None,
) -> Path:
    """Get the path of the output directory for given raw file name relative to the `output` folder.

    Only if the raw_file has no project defined, we use a month-specific subfolder
    This is to avoid having too many files in the fallback output folders.

    E.g.
        <project_id>/2024_07/out_RAW-FILE-1.raw/<software_type> in case raw_file has no project ID
        <project_id>/out_RAW-FILE-1.raw/<software_type> in case raw_file has a project ID
    """
    optional_sub_folder = (
        get_created_at_year_month(raw_file) if not raw_file.has_project else ""
    )
    path = (
        Path(raw_file.project_id)
        / optional_sub_folder
        / f"{OUTPUT_FOLDER_PREFIX}{raw_file.id}"
    )
    if software_type is not None:
        path = path / software_type
    return path
