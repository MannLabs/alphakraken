"""Alert issue and suppression queries on MongoDB."""

from datetime import UTC, datetime

from mongoengine import Q

from shared.db._mongo import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_alert_issue, to_alert_suppression
from shared.domain.alerts import AlertIssue, AlertSuppression


@translate_errors
def find_issues() -> list[AlertIssue]:
    """Return all alert issues, cf. `shared.db.alerts`."""
    ensure_connected()
    return [to_alert_issue(doc) for doc in models.AlertIssue.objects]


@translate_errors
def add_issue(*, alert: str, key: str, first_seen: datetime) -> AlertIssue:
    """Add an alert issue, cf. `shared.db.alerts`."""
    ensure_connected()
    issue = models.AlertIssue(alert=alert, key=key, first_seen=first_seen)
    issue.save(force_insert=True)
    return to_alert_issue(issue)


@translate_errors
def set_issue_notified(issue_id: str, notified_at: datetime) -> None:
    """Record that the issue was notified, cf. `shared.db.alerts`."""
    ensure_connected()
    models.AlertIssue.objects(id=issue_id).update_one(set__last_notified=notified_at)


@translate_errors
def find_suppressions(*, active_only: bool) -> list[AlertSuppression]:
    """Return the alert suppressions, cf. `shared.db.alerts`."""
    ensure_connected()
    query = models.AlertSuppression.objects.order_by("-created_at_")
    if active_only:
        now = datetime.now(tz=UTC)
        query = query.filter(Q(expires_at=None) | Q(expires_at__gt=now))
    return [to_alert_suppression(doc) for doc in query]


@translate_errors
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
    """Add an alert suppression, cf. `shared.db.alerts`."""
    ensure_connected()
    models.AlertSuppression(
        alert=alert,
        instrument_id=instrument_id,
        identifier=identifier,
        details=details,
        comment=comment,
        expires_at=expires_at,
        created_by=created_by,
    ).save()


@translate_errors
def delete_suppression(suppression_id: str) -> None:
    """Delete an alert suppression, cf. `shared.db.alerts`."""
    ensure_connected()
    models.AlertSuppression.objects(id=suppression_id).delete()
