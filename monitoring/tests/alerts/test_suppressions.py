"""Unit tests for the matching of issues against alert suppressions."""

from datetime import datetime

import pytest
import pytz

from monitoring.alerts.base_alert import BaseAlert, Issue
from monitoring.alerts.suppressions import is_muted
from shared.db.models import AlertSuppression

ISSUE = Issue("inst1", "copy failed: disk full", instrument_id="inst1")


class DiskSpaceAlert(BaseAlert):
    """Alert with the default rendering."""

    def _get_issues(self, status_objects: list, now: datetime) -> list[Issue]:
        del status_objects, now
        return []


class StaleStatusAlert(BaseAlert):
    """Alert rendering more than the raw details."""

    def _get_issues(self, status_objects: list, now: datetime) -> list[Issue]:
        del status_objects, now
        return []

    def format_message(self, issues: list[Issue[datetime]]) -> str:
        """Render the date and a word that is not in the raw details."""
        return "\n".join(
            f"- {i.identifier}: {i.details:%Y-%m-%d} stale" for i in issues
        )


ALERT = DiskSpaceAlert()


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({}, True),  # all wildcards
        ({"alert": "DiskSpaceAlert"}, True),
        ({"alert": "OtherAlert"}, False),
        ({"instrument_id": "inst1"}, True),
        ({"instrument_id": "inst2"}, False),
        ({"identifier": "inst1"}, True),
        ({"identifier": "inst"}, False),  # exact, not substring
        ({"details": "disk full"}, True),  # substring
        ({"details": "network"}, False),
        (
            {
                "alert": "DiskSpaceAlert",
                "instrument_id": "inst1",
                "details": "copy",
            },
            True,
        ),
        (
            {
                "alert": "DiskSpaceAlert",
                "instrument_id": "inst2",
                "details": "copy",
            },
            False,
        ),
    ],
)
def test_is_muted_requires_all_fields_to_match(
    fields: dict[str, str], *, expected: bool
) -> None:
    """Every field of a suppression must match, the wildcard matches anything."""
    # given
    suppression = AlertSuppression(comment="c", **fields)

    # when / then
    assert is_muted(ALERT, ISSUE, [suppression]) is expected


def test_is_muted_with_any_matching_suppression() -> None:
    """One matching suppression among several mutes the issue."""
    # given
    suppressions = [
        AlertSuppression(comment="c", alert="OtherAlert"),
        AlertSuppression(comment="c", identifier="inst1"),
    ]

    # when / then
    assert is_muted(ALERT, ISSUE, suppressions) is True
    assert is_muted(ALERT, ISSUE, []) is False


def test_issue_without_instrument_only_matches_wildcard() -> None:
    """Issues of file systems or jobs have no instrument, so only `*` matches them."""
    # given
    issue = Issue("backup", 100)

    # when / then
    assert is_muted(ALERT, issue, [AlertSuppression(comment="c")]) is True
    assert (
        is_muted(
            ALERT,
            issue,
            [AlertSuppression(comment="c", instrument_id="inst1")],
        )
        is False
    )


def test_empty_message_matches_nothing() -> None:
    """An empty details is not a wildcard."""
    # given
    suppression = AlertSuppression(comment="c", details="")

    # when / then
    assert is_muted(ALERT, ISSUE, [suppression]) is False


def test_details_match_the_rendered_issue() -> None:
    """Details are matched against the issue as rendered in the message, not its raw details."""
    # given
    issue = Issue("inst1", datetime(2024, 1, 15, tzinfo=pytz.UTC))

    # when / then
    assert is_muted(
        StaleStatusAlert(), issue, [AlertSuppression(comment="c", details="stale")]
    )
    assert not is_muted(
        StaleStatusAlert(), issue, [AlertSuppression(comment="c", details="UTC")]
    )
