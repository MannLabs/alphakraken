"""Alert issue and suppression queries."""

import logging
from datetime import datetime

from shared.db._mongo import alerts as _backend
from shared.domain.alerts import AlertIssue, AlertSuppression


def find_issues() -> list[AlertIssue]:
    """Return all alert issues."""
    return _backend.find_issues()


def add_issue(*, alert: str, key: str, first_seen: datetime) -> AlertIssue:
    """Add an alert issue that was never notified."""
    issue = _backend.add_issue(alert=alert, key=key, first_seen=first_seen)
    logging.info(f"Added alert issue: {alert=} {key=}")
    return issue


def set_issue_notified(issue_id: str, notified_at: datetime) -> None:
    """Record that the issue was notified."""
    _backend.set_issue_notified(issue_id, notified_at)


def find_suppressions(*, active_only: bool = False) -> list[AlertSuppression]:
    """Return the alert suppressions, newest first.

    :param active_only: leave out the expired ones
    """
    return _backend.find_suppressions(active_only=active_only)


def add_suppression(  # noqa: PLR0913
    *,
    alert: str,
    instrument_id: str,
    identifier: str,
    details: str,
    comment: str,
    expires_at: datetime | None,
    created_by: str | None,
) -> None:
    """Add an alert suppression."""
    logging.info(
        f"Adding alert suppression to DB: {alert=} {instrument_id=} {identifier=} {details=} {expires_at=}"
    )
    _backend.add_suppression(
        alert=alert,
        instrument_id=instrument_id,
        identifier=identifier,
        details=details,
        comment=comment,
        expires_at=expires_at,
        created_by=created_by,
    )


def delete_suppression(suppression_id: str) -> None:
    """Delete an alert suppression."""
    _backend.delete_suppression(suppression_id)
    logging.info(f"Deleted alert suppression: {suppression_id=}")
