"""Gradient-pause alert: DM the user when an instrument idles past 3x the gradient length."""

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

if TYPE_CHECKING:
    from mongoengine.queryset.queryset import QuerySet

from shared.db.interface import augment_raw_files_with_metrics
from shared.db.models import KrakenStatus, KrakenStatusEntities, RawFile

from .base_alert import BaseAlert
from .config import (
    GRADIENT_PAUSE_MULTIPLIER,
    SLACK_BOT_TOKEN,
    Cases,
)

# Initials match files via exact substring "_<initials>_" (case-sensitive) in `original_name`.
INITIALS_TO_SLACK_ID: dict[str, str] = {
    # "MaSc": "U231231231231",
}

NUM_FILES_REQUIRED = 3


class GradientPauseAlert(BaseAlert):
    """Alert when an instrument's pause exceeds 3x the user's gradient length."""

    def __init__(self) -> None:
        """Track the most recently alerted youngest filename per instrument."""
        self._alerted_file_names: dict[str, str] = {}

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.GRADIENT_PAUSE

    def _get_issues(
        self, status_objects: list[KrakenStatus]
    ) -> list[tuple[str, dict[str, Any]]]:
        """Find instruments paused for longer than 3x their last gradient length."""
        if not INITIALS_TO_SLACK_ID:
            return []

        instrument_ids = [
            s.id
            for s in status_objects
            if s.entity_type == KrakenStatusEntities.INSTRUMENT
        ]

        now = datetime.now(pytz.UTC)
        issues: list[tuple[str, dict[str, Any]]] = []

        for instrument_id in instrument_ids:
            files = list(
                RawFile.objects.filter(instrument_id=instrument_id)
                .order_by("-created_at")
                .only("id", "original_name", "created_at", "size")[:NUM_FILES_REQUIRED]
            )

            if len(files) < NUM_FILES_REQUIRED:
                continue

            initials = _common_known_initials(f.original_name for f in files)
            if initials is None:
                continue

            youngest = files[0]
            second_youngest = files[1]
            youngest_created = _as_utc(youngest.created_at)
            gradient_length = youngest_created - _as_utc(second_youngest.created_at)
            pause = now - youngest_created

            if pause <= GRADIENT_PAUSE_MULTIPLIER * gradient_length:
                self._alerted_file_names.pop(instrument_id, None)
                continue

            if self._alerted_file_names.get(instrument_id) == youngest.original_name:
                continue

            file_details = _file_details_with_precursors(files)

            issues.append(
                (
                    instrument_id,
                    {
                        "initials": initials,
                        "slack_user_id": INITIALS_TO_SLACK_ID[initials],
                        "gradient_length": gradient_length,
                        "pause": pause,
                        "files": file_details,
                    },
                )
            )
            self._alerted_file_names[instrument_id] = youngest.original_name

        return issues

    def format_message(self, issues: list[tuple[str, dict[str, Any]]]) -> str:
        """Format a single human-readable summary covering all issues (used for logs)."""
        lines = []
        for instrument_id, details in issues:
            lines.append(_format_dm(instrument_id, details))
        return "\n\n".join(lines)

    def dispatch_issues(self, issues: list[tuple[str, dict[str, Any]]]) -> None:
        """DM the responsible user for each instrument issue via Slack Web API."""
        if not SLACK_BOT_TOKEN:
            logging.warning("SLACK_BOT_TOKEN is not configured; skipping DM dispatch.")
            return

        client = WebClient(token=SLACK_BOT_TOKEN)
        for instrument_id, details in issues:
            slack_user_id = details["slack_user_id"]
            text = _format_dm(instrument_id, details)
            try:
                client.chat_postMessage(channel=slack_user_id, text=text)
            except SlackApiError:
                logging.exception(
                    f"Failed to DM {slack_user_id} for instrument {instrument_id}"
                )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return pytz.utc.localize(value)
    return value


def _common_known_initials(file_names: Iterable[str]) -> str | None:
    """Return the single known-initials token shared by all file names, else None."""
    matched: set[str] = set()
    for name in file_names:
        hit = next(
            (initials for initials in INITIALS_TO_SLACK_ID if f"_{initials}_" in name),
            None,
        )
        if hit is None:
            return None
        matched.add(hit)
        if len(matched) > 1:
            return None
    return matched.pop() if matched else None


def _file_details_with_precursors(files: list[RawFile]) -> list[dict[str, Any]]:
    """Return [{name, size_bytes, precursors}, ...] in same order as `files`."""
    augmented = augment_raw_files_with_metrics(
        cast("QuerySet", files), fields=["precursors"], flatten=True
    )

    details: list[dict[str, Any]] = []
    for raw_file in files:
        entry = augmented.get(raw_file.id, {})
        precursors = next(
            (v for k, v in entry.items() if k.endswith("__precursors")),
            None,
        )
        details.append(
            {
                "name": raw_file.original_name,
                "size_bytes": raw_file.size,
                "precursors": precursors,
            }
        )
    return details


def _format_dm(instrument_id: str, details: dict[str, Any]) -> str:
    """Build the Slack DM body for a single instrument issue."""
    pause_min = details["pause"].total_seconds() / 60
    grad_min = details["gradient_length"].total_seconds() / 60

    file_lines = [
        f"  - `{f['name']}` ({_format_size(f['size_bytes'])}, "
        f"precursors={_format_precursors(f['precursors'])})"
        for f in details["files"]
    ]

    return (
        f":warning: Instrument `{instrument_id}` has been paused for "
        f"{pause_min:.0f} min (last gradient length: {grad_min:.0f} min).\n"
        f"Last 3 files:\n" + "\n".join(file_lines)
    )


def _format_size(size_bytes: int | None) -> str:
    if size_bytes is None:
        return "?"
    return f"{size_bytes / (1024 * 1024):.0f} MB"


def _format_precursors(precursors: Any) -> str:
    if precursors is None:
        return "n/a"
    try:
        return f"{int(precursors):,}"
    except (TypeError, ValueError):
        return str(precursors)
