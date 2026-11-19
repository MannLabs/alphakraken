"""Alert issue and suppression queries on MongoDB."""

from datetime import UTC, datetime

from mongoengine import Q

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import to_alert_issue, to_alert_suppression
from shared.domain.alerts import AlertIssue, AlertSuppression, NewSuppression


@db_backend_function
def find_issues() -> list[AlertIssue]:
    """Return all alert issues, cf. `shared.db.alerts`."""
    return [to_alert_issue(doc) for doc in models.AlertIssue.objects]


@db_backend_function
def add_issue(*, alert: str, key: str, first_seen: datetime) -> AlertIssue:
    """Add an alert issue, cf. `shared.db.alerts`."""
    issue = models.AlertIssue(alert=alert, key=key, first_seen=first_seen)
    issue.save(force_insert=True)
    return to_alert_issue(issue)


@db_backend_function
def set_issue_notified(issue_id: str, notified_at: datetime) -> None:
    """Record that the issue was notified, cf. `shared.db.alerts`."""
    models.AlertIssue.objects(id=issue_id).update_one(set__last_notified=notified_at)


@db_backend_function
def find_suppressions(*, active_only: bool) -> list[AlertSuppression]:
    """Return the alert suppressions, cf. `shared.db.alerts`."""
    query = models.AlertSuppression.objects.order_by("-created_at_")
    if active_only:
        now = datetime.now(tz=UTC)
        query = query.filter(Q(expires_at=None) | Q(expires_at__gt=now))
    return [to_alert_suppression(doc) for doc in query]


@db_backend_function
def add_suppression(new: NewSuppression) -> None:
    """Add an alert suppression, cf. `shared.db.alerts`."""
    models.AlertSuppression(
        alert=new.alert,
        instrument_id=new.instrument_id,
        identifier=new.identifier,
        details=new.details,
        comment=new.comment,
        expires_at=new.expires_at,
        created_by=new.created_by,
    ).save()


@db_backend_function
def delete_suppression(suppression_id: str) -> None:
    """Delete an alert suppression, cf. `shared.db.alerts`."""
    models.AlertSuppression.objects(id=suppression_id).delete()
