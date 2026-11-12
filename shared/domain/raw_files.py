"""Raw file constants."""


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
