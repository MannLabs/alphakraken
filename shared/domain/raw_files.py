"""Raw file domain type and constants."""

from dataclasses import dataclass
from datetime import datetime

from shared.keys import FALLBACK_PROJECT_ID

# (size, hash) or (size, hash, etag) of one file of a raw file folder
FileInfoItem = (
    tuple[float | None, str | None] | tuple[float | None, str | None, str | None]
)


class RawFileStatus:
    """Status of raw file.

    Not every status is required for consecutive tasks (e.g. `ACQUISITION_FINISHED`).
    The last task of a DAG should set a status (e.g. `COPYING_FINISHED`).

    Note that you might adapt the webapp when a new status is added, e.g. update TERMINAL_STATUSES, ERROR_STATUSES, highlight_status_cell()
    """

    IGNORED = "ignored"

    QUEUED_FOR_MONITORING = "queued_for_monitoring"
    MONITORING_ACQUISITION = "monitoring_acquisition"
    MONITORING_DONE = "monitoring_done"

    CHECKSUMMING = "checksumming"
    CHECKSUMMING_DONE = "checksumming_done"
    COPYING = "copying"
    COPYING_DONE = "copying_done"
    ACQUISITION_FAILED = "acquisition_failed"
    DISAPPEARED = "disappeared"  # files vanished from the instrument before backup

    QUEUED_FOR_QUANTING = "queued_for_quanting"
    QUANTING = "quanting"
    QUANTING_FAILED = "quanting_failed"

    ERROR = "error"
    DONE = "done"
    DONE_NOT_QUANTED = "done_not_quanted"


class BackupStatus:
    """Status of backup.

    "copying" refers to copying to local backup location, "upload" to uploading to S3.
    """

    COPYING_IN_PROGRESS = "copying_in_progress"
    COPYING_DONE = "copying_done"
    COPYING_FAILED = "copying_failed"

    UPLOAD_IN_PROGRESS = "upload_in_progress"
    UPLOAD_DONE = "upload_done"
    UPLOAD_FAILED = "upload_failed"

    SKIPPED = "skipped"


ERROR_STATUSES = [
    RawFileStatus.ERROR,
    RawFileStatus.QUANTING_FAILED,
    RawFileStatus.ACQUISITION_FAILED,
    RawFileStatus.DISAPPEARED,
]
TERMINAL_STATUSES = [
    *ERROR_STATUSES,
    RawFileStatus.DONE,
    RawFileStatus.DONE_NOT_QUANTED,
    RawFileStatus.IGNORED,
]


class InstrumentFileStatus:
    """Status of files on instrument and their lifecycle."""

    NA = "n/a"  # for backwards compatibility
    INITIAL = "initial"  # File exists on instrument, not yet moved
    RENAMED = "renamed"  # File was renamed on instrument (read as "file does no longer exist under its name as it was renamed")
    DISAPPEARED = (
        "disappeared"  # File disappeared from instrument before it was backed up
    )
    MOVED = "moved"  # File moved from instrument to instrument backup folder
    PURGED = "purged"  # File removed from instrument backup folder


@dataclass(frozen=True)
class RawFile:
    """A raw file as acquired by an instrument. Datetimes are UTC-aware."""

    # the file name, prefixed with the collision flag in case of a name collision
    id: str
    collision_flag: str | None
    original_name: str
    instrument_id: str
    project_id: str
    status: str
    status_details: str | None
    # bytes
    size: int | None
    backup_status: str | None
    # `display_paths.backup` at copy time: records which backup file system holds the file
    backup_display_base_path: str | None
    # bucket_name or bucket_name/sub_path/ -> file_info paths are always relative to that
    s3_upload_path: str | None
    instrument_file_status: str
    # paths relative to the raw file folder (cf. path_layout) to their file info;
    # None if not loaded, cf. `shared.db.raw_files.find`
    file_info: dict[str, FileInfoItem] | None
    created_at: datetime
    # when the acquisition was finished (mtime of main file)
    finalized_at: datetime | None
    created_at_: datetime
    updated_at_: datetime

    @property
    def has_project(self) -> bool:
        """Return whether the raw file has a project assigned or belongs to the fallback project."""
        return self.project_id != FALLBACK_PROJECT_ID

    def require_file_info(self) -> dict[str, FileInfoItem]:
        """Return the file info.

        Raises:
            ValueError: if it was not loaded, cf. `shared.db.raw_files`.

        """
        if self.file_info is None:
            raise ValueError(f"file_info of raw file {self.id} was not loaded.")
        return self.file_info


def parse_file_info_item(
    item: FileInfoItem | None,
) -> tuple[float | None, str | None]:
    """Parse a file_info item tuple.

    This is to hide the details of the file_info storage format from the rest of the codebase.

    Args:
        item: The file_info item tuple to parse.

    Returns:
        A tuple containing the size and hash.

    Raises:
        ValueError: If the item length is invalid.

    """
    if not item:
        return item  # type: ignore[invalid-return-type]

    if len(item) >= 2:  # noqa: PLR2004
        return item[0], item[1]

    raise ValueError(f"Invalid file_info item length {len(item)} {item}.")


def get_created_at_year_month(raw_file: RawFile) -> str:
    """Get the year and month of the raw file creation date."""
    return raw_file.created_at.strftime("%Y_%m")
