"""Queue-stop alert: notifies the prior operator when their measurement queue ends.

Two mutually exclusive modes per instrument (see SPEC.md §2.1):
- Stall: newest two files share mapped initials and the pause exceeds N x gradient.
- Handoff: second- and third-newest share mapped initials but the newest differs.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import ClassVar

import pytz

from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert, Issue
from .config import (
    INSTRUMENT_USER_SLACK_IDS,
    MAX_GRADIENT_LENGTH_HOURS,
    QUEUE_STOP_THRESHOLD_MULTIPLIER,
    Cases,
)
from .recipients import extract_initials

KIND_STOP = "stop"
KIND_HANDOFF = "handoff"

MIN_FILES_FOR_DETECTION = 2
MIN_FILES_FOR_HANDOFF_DETECTION = 3

_BYTES_PER_GB = 1024**3


@dataclass(frozen=True)
class RecentFile:
    """One row in the `Recent files:` section of the message."""

    file_id: str
    size: int | None
    created_at: datetime


@dataclass
class QueueStopIssue:
    """Payload describing a queue-stop event (stop or handoff)."""

    kind: str
    instrument_id: str
    subject_file_id: str  # the alerted user's most recent file
    gradient_length: timedelta | None
    pause: timedelta | None
    recent_files: list[RecentFile]


def _format_size(size_bytes: int | None) -> str:
    if size_bytes is None or size_bytes < 0:
        return "n/a"
    return f"{size_bytes / _BYTES_PER_GB:.2f} GB"


class QueueStopAlert(BaseAlert):
    """Notify the prior operator when their measurement queue has ended."""

    # Class-level so it survives across instances created within a single
    # monitor process; matches the InstrumentStallAlert tracker pattern.
    _alerted_subject_files: ClassVar[set[tuple[str, str]]] = set()

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.QUEUE_STOP

    def _get_issues(
        self, status_objects: list[KrakenStatus]
    ) -> list[Issue[QueueStopIssue]]:
        """Detect stop/handoff conditions per instrument."""
        del status_objects

        if not INSTRUMENT_USER_SLACK_IDS:
            return []

        instrument_ids = RawFile.objects.distinct("instrument_id")
        now = datetime.now(pytz.UTC)
        max_gradient = timedelta(hours=MAX_GRADIENT_LENGTH_HOURS)

        issues: list[Issue[QueueStopIssue]] = []
        for instrument_id in instrument_ids:
            recent = list(
                RawFile.objects.filter(instrument_id=instrument_id)
                .only("id", "created_at", "size", "instrument_id")
                .order_by("-created_at")
                .limit(3)
            )
            if len(recent) < MIN_FILES_FOR_DETECTION:
                logging.debug(
                    f"Skipping {instrument_id}: fewer than "
                    f"{MIN_FILES_FOR_DETECTION} raw files on instrument"
                )
                continue

            has_third_file = len(recent) >= MIN_FILES_FOR_HANDOFF_DETECTION
            file1_id: str = recent[0].id
            file2_id: str = recent[1].id
            file1_created: datetime = _ensure_utc(recent[0].created_at)
            file2_created: datetime = _ensure_utc(recent[1].created_at)
            file3_created: datetime | None = (
                _ensure_utc(recent[2].created_at) if has_third_file else None
            )

            initials1 = extract_initials(file1_id)
            initials2 = extract_initials(file2_id)
            initials3 = extract_initials(recent[2].id) if has_third_file else None

            recent_files: list[RecentFile] = [
                RecentFile(
                    file_id=str(rf.id),
                    size=rf.size,
                    created_at=_ensure_utc(rf.created_at),
                )
                for rf in recent
            ]

            issue = self._evaluate_rule_a(
                instrument_id=instrument_id,
                file1_id=file1_id,
                file2_id=file2_id,
                created1=file1_created,
                created2=file2_created,
                initials1=initials1,
                initials2=initials2,
                now=now,
                max_gradient=max_gradient,
                recent_files=recent_files,
            )
            if issue is None and file3_created is not None:
                issue = self._evaluate_rule_b(
                    instrument_id=instrument_id,
                    file2_id=file2_id,
                    created2=file2_created,
                    created3=file3_created,
                    initials1=initials1,
                    initials2=initials2,
                    initials3=initials3,
                    max_gradient=max_gradient,
                    recent_files=recent_files,
                )

            if issue is None:
                continue

            # Sharing the subject file as key across rules ensures
            # stop→handoff for the same user-last-file does not double-notify.
            subject_file_id = issue.subject_file_id
            dedup_key = (instrument_id, subject_file_id)
            if dedup_key in self._alerted_subject_files:
                logging.debug(f"Skipping already-alerted {issue.kind} for {dedup_key}")
                continue

            identifier = f"{instrument_id}:{subject_file_id}"
            issues.append(
                Issue(
                    identifier,
                    issue,
                    relevant_file=subject_file_id,
                    instrument_id=instrument_id,
                )
            )

        return issues

    @staticmethod
    def _evaluate_rule_a(  # noqa: PLR0913
        *,
        instrument_id: str,
        file1_id: str,
        file2_id: str,
        created1: datetime,
        created2: datetime,
        initials1: str | None,
        initials2: str | None,
        now: datetime,
        max_gradient: timedelta,
        recent_files: list[RecentFile],
    ) -> QueueStopIssue | None:
        """Rule A — stop: file1 & file2 share mapped initials, pause exceeds threshold.

        Fires when the still-active operator's queue has gone quiet. The shared
        mapped initials of `file1`/`file2` identify the user; the pause from
        `file1.created_at` to `now` must exceed `QUEUE_STOP_THRESHOLD_MULTIPLIER`
        times the gradient length inferred from the two files.

        Example (fires):
            file1 = "_MaSc_a.raw" at 12:00, file2 = "_MaSc_b.raw" at 11:30,
            now = 14:00. gradient = 30 min, pause = 2 h > 3 x 30 min → stop
            alert for MaSc with subject_file_id = file1.id.

        Example (skipped, gradient too large):
            file1 at 12:00, file2 at 09:00 → gradient = 3 h > MAX 2 h. The two
            files belong to different queues; `file1` is treated as a fresh
            queue start.

        Example (skipped, pause below threshold):
            file1 at 12:00, file2 at 11:30, now = 13:00. gradient = 30 min,
            pause = 1 h <= 3 x 30 min → no stop yet.

        Example (skipped, initials don't match):
            file1 = "_MaSc_a.raw", file2 = "_JoeB_b.raw" → Rule A doesn't apply
            (Rule B may, evaluated separately).
        """
        if initials1 is None or initials1 != initials2:
            return None

        gradient_length = created1 - created2

        if gradient_length <= timedelta(0):
            logging.warning(
                f"Skipping stop rule for {instrument_id}: non-positive gradient "
                f"{gradient_length} between {file1_id} and {file2_id}"
            )
            return None

        if gradient_length > max_gradient:
            logging.debug(
                f"Skipping stop rule for {instrument_id}: gradient "
                f"{gradient_length} > max {max_gradient}"
            )
            return None

        pause = now - created1
        if pause <= QUEUE_STOP_THRESHOLD_MULTIPLIER * gradient_length:
            return None

        return QueueStopIssue(
            kind=KIND_STOP,
            instrument_id=instrument_id,
            subject_file_id=file1_id,
            gradient_length=gradient_length,
            pause=pause,
            recent_files=recent_files,
        )

    @staticmethod
    def _evaluate_rule_b(  # noqa: PLR0913
        *,
        instrument_id: str,
        file2_id: str,
        created2: datetime,
        created3: datetime,
        initials1: str | None,
        initials2: str | None,
        initials3: str | None,
        max_gradient: timedelta,
        recent_files: list[RecentFile],
    ) -> QueueStopIssue | None:
        """Rule B — handoff: file2 & file3 share mapped initials, file1 differs.

        Fires when someone else took over: the prior operator's last two files
        are now second- and third-newest, and a different (or unattributable)
        file is on top. The prior operator's queue is treated as ended
        unconditionally - no 3x pause check, since a new file has arrived from
        someone else, which is itself the queue-end signal.

        Example (fires, new operator with mapped initials):
            file1 = "_JoeB_n.raw", file2 = "_MaSc_a.raw", file3 = "_MaSc_b.raw"
            with file2→file3 gap ≤ 2 h → handoff alert for MaSc with
            subject_file_id = file2.id. JoeB is NOT notified.

        Example (fires, unattributable newest file):
            file1 = "QC_check.raw", file2/file3 share `_MaSc_` → still alerts
            MaSc; file1 just has no operator.

        Example (skipped, prior pair gap too large):
            file2 at 12:00, file3 at 08:00 → prior gradient = 4 h > MAX 2 h.
            file2 and file3 weren't part of the same queue.

        Example (skipped, single-file prior run):
            file2 = "_MaSc_a.raw", file3 = "_JoeB_b.raw" → prior pair doesn't
            share initials; the "prior" user only had one file, so they didn't
            have a queue to end.
        """
        if initials2 is None or initials2 != initials3:
            return None
        if initials1 == initials2:
            return None

        prior_gradient_length = created2 - created3

        if prior_gradient_length > max_gradient:
            logging.debug(
                f"Skipping handoff rule for {instrument_id}: prior gradient "
                f"{prior_gradient_length} > max {max_gradient}"
            )
            return None

        return QueueStopIssue(
            kind=KIND_HANDOFF,
            instrument_id=instrument_id,
            subject_file_id=file2_id,
            gradient_length=prior_gradient_length,
            pause=None,
            recent_files=recent_files,
        )

    def mark_reported(self, issues: list[Issue[QueueStopIssue]]) -> None:
        """Remember the subject files to not alert again for them."""
        for issue in issues:
            self._alerted_subject_files.add(
                (issue.details.instrument_id, issue.details.subject_file_id)
            )

    def format_message(self, issues: list[Issue[QueueStopIssue]]) -> str:
        """Render all issues, with file metadata refreshed."""
        messages = []
        for issue in issues:
            self.refresh_recent_files(issue.details)
            messages.append(self.render_issue(issue.details))
        return "\n\n".join(messages)

    @staticmethod
    def refresh_recent_files(issue: QueueStopIssue) -> None:
        """Refresh metadata (size) for the exact files captured at detection.

        Mutates `issue.recent_files`. Called right before render: size may
        have been populated in the gap between detection and dispatch. Re-queries
        only the same file IDs - never the ordered top-3 - so files arriving in
        that gap cannot swap out the subject file the alert was raised for.
        """
        file_ids = [rf.file_id for rf in issue.recent_files]
        fresh = RawFile.objects.filter(id__in=file_ids).only("id", "size")
        size_by_id = {str(rf.id): rf.size for rf in fresh}
        issue.recent_files = [
            RecentFile(
                file_id=rf.file_id,
                size=size_by_id.get(rf.file_id, rf.size),
                created_at=rf.created_at,
            )
            for rf in issue.recent_files
        ]

    @staticmethod
    def render_issue(issue: QueueStopIssue) -> str:
        """Render a single issue to a message string."""
        if issue.kind == KIND_STOP:
            gradient_minutes = (
                issue.gradient_length.total_seconds() / 60
                if issue.gradient_length is not None
                else 0
            )
            pause_minutes = (
                issue.pause.total_seconds() / 60 if issue.pause is not None else 0
            )
            header = (
                f"Queue stopped on `{issue.instrument_id}`: "
                f"gradient ~{gradient_minutes:.0f} min, "
                f"pause {pause_minutes:.0f} min."
            )
        else:
            header = f"Queue handoff on `{issue.instrument_id}`"

        file_lines = [
            f"- `{rf.file_id}` ({_format_size(rf.size)}) "
            f"[{rf.created_at:%Y-%m-%d %H:%M UTC}]"
            for rf in issue.recent_files
        ]
        return header + "\nRecent files:\n" + "\n".join(file_lines)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return pytz.utc.localize(value)
    return value
