"""Queue-stop alert: DMs the prior operator when their measurement queue ends.

Two mutually exclusive modes per instrument (see SPEC.md §2.1):
- Stall: newest two files share mapped initials and the pause exceeds N x gradient.
- Handoff: second- and third-newest share mapped initials but the newest differs.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import ClassVar

import pytz
from messenger_clients import send_dm

from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert
from .config import (
    INSTRUMENT_USER_SLACK_IDS,
    MAX_GRADIENT_LENGTH_HOURS,
    QUEUE_END_THRESHOLD_MULTIPLIER,
    SPECIAL_ALERT_SLACK_ID,
    Cases,
)

INITIALS_PATTERN = re.compile(r"_([A-Za-z]{2,8})_")

KIND_STALL = "stall"
KIND_HANDOFF = "handoff"

MIN_FILES_FOR_DETECTION = 2
MIN_FILES_FOR_HANDOFF = 3

_BYTES_PER_GB = 1024**3


@dataclass
class QueueEndIssue:
    """Payload describing a queue-stop event (stall or handoff)."""

    kind: str
    instrument_id: str
    messenger_user_id: str
    gradient_length: timedelta | None
    pause: timedelta | None
    recent_files: list[tuple[str, int | None]]


def _extract_initials(name: str | None) -> str | None:
    """Return the first regex match whose token is a mapped initial, else None."""
    if not name:
        return None
    for match in INITIALS_PATTERN.finditer(name):
        token = match.group(1)
        if token in INSTRUMENT_USER_SLACK_IDS:
            return token
    return None


def _format_size(size_bytes: int | None) -> str:
    if size_bytes is None or size_bytes < 0:
        return "n/a"
    return f"{size_bytes / _BYTES_PER_GB:.2f} GB"


class QueueEndAlert(BaseAlert):
    """Notify the prior operator when their measurement queue has ended."""

    # Class-level so it survives across instances created within a single
    # monitor process; matches the InstrumentStallAlert tracker pattern.
    _alerted_subject_files: ClassVar[set[tuple[str, str]]] = set()

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.QUEUE_END

    def _get_issues(
        self, status_objects: list[KrakenStatus]
    ) -> list[tuple[str, QueueEndIssue]]:
        """Detect stall/handoff conditions per instrument."""
        del status_objects

        if not INSTRUMENT_USER_SLACK_IDS:
            return []

        instrument_ids = RawFile.objects.distinct("instrument_id")
        now = datetime.now(pytz.UTC)
        max_gradient = timedelta(hours=MAX_GRADIENT_LENGTH_HOURS)

        issues: list[tuple[str, QueueEndIssue]] = []
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

            has_third = len(recent) >= MIN_FILES_FOR_HANDOFF
            file1_id: str = recent[0].id
            file2_id: str = recent[1].id
            file1_created: datetime = _ensure_utc(recent[0].created_at)
            file2_created: datetime = _ensure_utc(recent[1].created_at)
            file3_created: datetime | None = (
                _ensure_utc(recent[2].created_at) if has_third else None
            )

            initials1 = _extract_initials(file1_id)
            initials2 = _extract_initials(file2_id)
            initials3 = _extract_initials(recent[2].id) if has_third else None

            recent_files: list[tuple[str, int | None]] = [
                (str(rf.id), rf.size) for rf in recent
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

            subject_file_id = file1_id if issue.kind == KIND_STALL else file2_id
            dedup_key = (instrument_id, subject_file_id)
            if dedup_key in self._alerted_subject_files:
                logging.debug(f"Skipping already-alerted {issue.kind} for {dedup_key}")
                continue
            self._alerted_subject_files.add(dedup_key)

            identifier = f"{instrument_id}:{subject_file_id}"
            issues.append((identifier, issue))

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
        recent_files: list[tuple[str, int | None]],
    ) -> QueueEndIssue | None:
        """Rule A — stall: file1 & file2 share mapped initials, pause exceeds threshold."""
        # USER_COMMENT: expand docstring with examples
        if initials1 is None or initials1 != initials2:
            return None

        gradient_length = created1 - created2

        if gradient_length <= timedelta(0):
            logging.warning(
                f"Skipping stall rule for {instrument_id}: non-positive gradient "
                f"{gradient_length} between {file1_id} and {file2_id}"
            )
            return None

        if gradient_length > max_gradient:
            logging.debug(
                f"Skipping stall rule for {instrument_id}: gradient "
                f"{gradient_length} > max {max_gradient}"
            )
            return None

        pause = now - created1
        if pause <= QUEUE_END_THRESHOLD_MULTIPLIER * gradient_length:
            return None

        return QueueEndIssue(
            kind=KIND_STALL,
            instrument_id=instrument_id,
            messenger_user_id=INSTRUMENT_USER_SLACK_IDS[initials1],
            gradient_length=gradient_length,
            pause=pause,
            recent_files=recent_files,
        )

    @staticmethod
    def _evaluate_rule_b(  # noqa: PLR0913
        *,
        instrument_id: str,
        created2: datetime,
        created3: datetime,
        initials1: str | None,
        initials2: str | None,
        initials3: str | None,
        max_gradient: timedelta,
        recent_files: list[tuple[str, int | None]],
    ) -> QueueEndIssue | None:
        """Rule B — handoff: file2 & file3 share mapped initials, file1 differs."""
        # USER_COMMENT: expand docstring with examples
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

        return QueueEndIssue(
            kind=KIND_HANDOFF,
            instrument_id=instrument_id,
            messenger_user_id=INSTRUMENT_USER_SLACK_IDS[initials2],
            gradient_length=prior_gradient_length,
            pause=None,
            recent_files=recent_files,
        )

    def format_message(self, issues: list[tuple[str, QueueEndIssue]]) -> str:
        """Not used — `dispatch` formats per-recipient messages instead (decision 1.2)."""
        del issues
        return ""

    def dispatch(self, issues: list[tuple[str, QueueEndIssue]]) -> None:
        """Format and DM each issue to the prior operator (plus optional CC)."""
        # USER_COMMENT: the alert should not be responsible for the sending. Move this up to alert_manager
        for identifier, issue in issues:
            message = self._render(issue)
            recipients: list[str] = [issue.messenger_user_id]
            if SPECIAL_ALERT_SLACK_ID and SPECIAL_ALERT_SLACK_ID not in recipients:
                recipients.append(SPECIAL_ALERT_SLACK_ID)

            for recipient in recipients:
                try:
                    send_dm(message, recipient)
                except Exception as exc:  # noqa: BLE001, PERF203
                    logging.warning(
                        f"Failed to send QueueEndAlert DM "
                        f"(recipient={recipient}, kind={issue.kind}, "
                        f"instrument={issue.instrument_id}, identifier={identifier}): {exc}"
                    )

    @staticmethod
    def _render(issue: QueueEndIssue) -> str:
        if issue.kind == KIND_STALL:
            gradient_minutes = (
                issue.gradient_length.total_seconds() / 60
                if issue.gradient_length is not None
                else 0
            )
            pause_minutes = (
                issue.pause.total_seconds() / 60 if issue.pause is not None else 0
            )
            header = (
                f":rotating_light: Queue stall on `{issue.instrument_id}`: "
                f"gradient ~{gradient_minutes:.0f} min, "
                f"pause {pause_minutes:.0f} min."
            )
        else:
            header = (
                f":wave: Queue handoff on `{issue.instrument_id}` "
                f"(your last file is now second-newest)."
            )

        file_lines = [
            f"- `{name}` ({_format_size(size)})" for name, size in issue.recent_files
        ]
        return header + "\nRecent files:\n" + "\n".join(file_lines)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return pytz.utc.localize(value)
    return value
