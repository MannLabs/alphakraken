"""Matching of issues against alert suppressions."""

from alerts.base_alert import Issue

from shared.db.models import AlertSuppression
from shared.keys import ALERT_SUPPRESSION_WILDCARD


def is_muted(
    alert_name: str, issue: Issue, suppressions: list[AlertSuppression]
) -> bool:
    """Return whether a suppression matches the issue.

    All fields of a suppression must match, `ALERT_SUPPRESSION_WILDCARD` matches anything:
    alert class, instrument and identifier exactly, details as a substring.
    """
    return any(_matches(alert_name, issue, s) for s in suppressions)


def _matches(alert_name: str, issue: Issue, suppression: AlertSuppression) -> bool:
    return (
        suppression.alert in (ALERT_SUPPRESSION_WILDCARD, alert_name)
        and suppression.instrument_id
        in (ALERT_SUPPRESSION_WILDCARD, issue.instrument_id)
        and suppression.identifier in (ALERT_SUPPRESSION_WILDCARD, issue.identifier)
        and (
            suppression.details == ALERT_SUPPRESSION_WILDCARD
            # empty details must not match everything
            or (bool(suppression.details) and suppression.details in str(issue.details))
        )
    )
