"""Matching of issues against alert suppressions."""

from alerts.base_alert import BaseAlert, Issue

from shared.db.models import AlertSuppression
from shared.keys import ALERT_SUPPRESSION_WILDCARD


def is_muted(
    alert: BaseAlert, issue: Issue, suppressions: list[AlertSuppression]
) -> bool:
    """Return whether a suppression matches the issue.

    All fields of a suppression must match, `ALERT_SUPPRESSION_WILDCARD` matches anything:
    alert class, instrument and identifier exactly, details as a substring of the issue as
    rendered in the message.
    """
    return any(_matches(alert, issue, s) for s in suppressions)


def _matches(alert: BaseAlert, issue: Issue, suppression: AlertSuppression) -> bool:
    return (
        suppression.alert in (ALERT_SUPPRESSION_WILDCARD, alert.name)
        and suppression.instrument_id
        in (ALERT_SUPPRESSION_WILDCARD, issue.instrument_id)
        and suppression.identifier in (ALERT_SUPPRESSION_WILDCARD, issue.identifier)
        and (
            suppression.details == ALERT_SUPPRESSION_WILDCARD
            # empty details must not match everything
            or (
                bool(suppression.details)
                and suppression.details in alert.format_message([issue])
            )
        )
    )
