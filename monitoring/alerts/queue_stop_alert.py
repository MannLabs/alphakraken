"""Queue-stop alert: notifies the prior operator when their measurement queue ends.

Known initials are those of the active users with a messenger id, the only ones that can be
notified; they identify the operator in the raw file names.

Two mutually exclusive rules per instrument, evaluated on its newest files:
- Stop: newest two files share known initials and the pause exceeds N x gradient.
- Handoff: second- and third-newest share known initials but the newest differs.
"""

import logging
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta

from utils import ensure_utc

from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert, Issue
from .config import (
    BYTES_PER_GB,
    MAX_GRADIENT_LENGTH_HOURS,
    QUEUE_STOP_THRESHOLD_MULTIPLIER,
    Destinations,
)
from .recipients import extract_initials, get_messenger_ids_by_initials

KIND_STOP = "stop"
KIND_HANDOFF = "handoff"

MIN_FILES_FOR_DETECTION = 2
MIN_FILES_FOR_HANDOFF_DETECTION = 3


@dataclass(frozen=True)
class RecentFile:
    """One of the newest raw files of an instrument."""

    file_id: str
    created_at: datetime
    # known initials of the operator in the file name, if any
    initials: str | None


@dataclass(frozen=True)
class QueueStopIssue:
    """A queue-stop event (stop or handoff) on an instrument."""

    kind: str
    instrument_id: str
    subject_file_id: str  # the alerted user's most recent file
    gradient_length: timedelta
    # time since the subject file; None for a handoff
    pause: timedelta | None
    recent_files: tuple[RecentFile, ...]


def _load_recent_files(known_initials: Collection[str]) -> dict[str, list[RecentFile]]:
    """Load the three newest raw files per instrument, newest first."""
    recent_files = {}
    for instrument_id in RawFile.objects.distinct("instrument_id"):
        raw_files = (
            RawFile.objects.filter(instrument_id=instrument_id)
            .only("id", "created_at")
            .order_by("-created_at")
            .limit(MIN_FILES_FOR_HANDOFF_DETECTION)
        )
        recent_files[instrument_id] = [
            RecentFile(
                str(raw_file.id),
                ensure_utc(raw_file.created_at),
                extract_initials(raw_file.id, known_initials),
            )
            for raw_file in raw_files
        ]
    return recent_files


def _detect(
    instrument_id: str,
    files: list[RecentFile],
    now: datetime,
    max_gradient: timedelta,
    multiplier: float,
) -> QueueStopIssue | None:
    """Return the stop or handoff on an instrument given its newest files, newest first, if any."""
    if len(files) < MIN_FILES_FOR_DETECTION:
        return None
    return _stop(instrument_id, files, now, max_gradient, multiplier) or _handoff(
        instrument_id, files, max_gradient
    )


def _stop(
    instrument_id: str,
    files: list[RecentFile],
    now: datetime,
    max_gradient: timedelta,
    multiplier: float,
) -> QueueStopIssue | None:
    """Rule A: the newest two files share known initials and the pause exceeds the threshold.

    Fires when the still-active operator's queue has gone quiet: the pause since the newest
    file must exceed `multiplier` times the gradient length inferred from the two files.
    A gap above `max_gradient` means the two files belong to different queues, so the
    newest file is a fresh queue start.

    Example: files at 12:00 and 11:30 by the same user, now 14:00. gradient = 30 min,
    pause = 2 h > 3 x 30 min → stop for the 12:00 file.
    """
    file1, file2 = files[0], files[1]
    if file1.initials is None or file1.initials != file2.initials:
        return None

    gradient_length = file1.created_at - file2.created_at
    if gradient_length <= timedelta(0):
        logging.warning(
            f"Skipping stop rule for {instrument_id}: non-positive gradient "
            f"{gradient_length} between {file1.file_id} and {file2.file_id}"
        )
        return None
    if gradient_length > max_gradient:
        return None

    pause = now - file1.created_at
    if pause <= multiplier * gradient_length:
        return None

    return QueueStopIssue(
        KIND_STOP, instrument_id, file1.file_id, gradient_length, pause, tuple(files)
    )


def _handoff(
    instrument_id: str, files: list[RecentFile], max_gradient: timedelta
) -> QueueStopIssue | None:
    """Rule B: the second- and third-newest files share known initials, the newest differs.

    Fires when someone else took over, or an unattributable file (e.g. QC) is on top: the
    new file is itself the queue-end signal, so there is no pause check. A gap above
    `max_gradient` between the prior pair means they were not one queue, and a prior pair
    without shared initials means the prior user had no queue to end.

    Example: "_JoeB_" at 12:00 on top of "_MaSc_" files at 11:00 and 10:30 → handoff for
    MaSc's 11:00 file. JoeB is not notified.
    """
    if len(files) < MIN_FILES_FOR_HANDOFF_DETECTION:
        return None
    file1, file2, file3 = files[0], files[1], files[2]
    if file2.initials is None or file2.initials != file3.initials:
        return None
    if file1.initials == file2.initials:
        return None

    prior_gradient_length = file2.created_at - file3.created_at
    if prior_gradient_length > max_gradient:
        return None

    return QueueStopIssue(
        KIND_HANDOFF,
        instrument_id,
        file2.file_id,
        prior_gradient_length,
        None,
        tuple(files),
    )


def _load_sizes(file_ids: Collection[str]) -> dict[str, int | None]:
    """Load the current sizes of the files; sizes may be populated after detection."""
    raw_files = RawFile.objects.filter(id__in=sorted(file_ids)).only("id", "size")
    return {str(raw_file.id): raw_file.size for raw_file in raw_files}


def _format_size(size_bytes: int | None) -> str:
    if size_bytes is None or size_bytes < 0:
        return "n/a"
    return f"{size_bytes / BYTES_PER_GB:.2f} GB"


def _render_issue(issue: QueueStopIssue, sizes: dict[str, int | None]) -> str:
    """Render a single issue to a message string."""
    if issue.kind == KIND_STOP:
        pause_minutes = issue.pause.total_seconds() / 60 if issue.pause else 0
        header = (
            f"Queue stopped on `{issue.instrument_id}`: "
            f"gradient ~{issue.gradient_length.total_seconds() / 60:.0f} min, "
            f"pause {pause_minutes:.0f} min."
        )
    else:
        header = f"Queue handoff on `{issue.instrument_id}`"

    file_lines = [
        f"- `{rf.file_id}` ({_format_size(sizes.get(rf.file_id))}) "
        f"[{rf.created_at:%Y-%m-%d %H:%M UTC}]"
        for rf in issue.recent_files
    ]
    return header + "\nRecent files:\n" + "\n".join(file_lines)


class QueueStopAlert(BaseAlert):
    """Notify the prior operator when their measurement queue has ended."""

    # each issue renders its own header
    title = None
    destinations = frozenset({Destinations.USER})
    # once per (instrument, subject file), shared by the stop and handoff rules so that
    # a stop followed by a handoff for the same user-last-file does not notify twice
    cooldown_minutes = None

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[QueueStopIssue]]:
        """Detect stop/handoff conditions per instrument."""
        del status_objects

        known_initials = get_messenger_ids_by_initials()
        if not known_initials:
            return []

        max_gradient = timedelta(hours=MAX_GRADIENT_LENGTH_HOURS)
        issues: list[Issue[QueueStopIssue]] = []
        for instrument_id, files in _load_recent_files(known_initials).items():
            issue = _detect(
                instrument_id, files, now, max_gradient, QUEUE_STOP_THRESHOLD_MULTIPLIER
            )
            if issue is None:
                continue
            issues.append(
                Issue(
                    f"{instrument_id}:{issue.subject_file_id}",
                    issue,
                    relevant_file=issue.subject_file_id,
                    instrument_id=instrument_id,
                )
            )
        return issues

    def format_message(self, issues: list[Issue[QueueStopIssue]]) -> str:
        """Render all issues with the current file sizes."""
        file_ids = {rf.file_id for issue in issues for rf in issue.details.recent_files}
        sizes = _load_sizes(file_ids)
        return "\n\n".join(_render_issue(issue.details, sizes) for issue in issues)
