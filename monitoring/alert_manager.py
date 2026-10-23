"""Decide when to send alerts based on monitoring checks."""

import logging
from datetime import datetime, timedelta

from alerts import (
    BaseAlert,
    DiskSpaceAlert,
    HealthCheckFailedAlert,
    InstrumentFilePileUpAlert,
    InstrumentStallAlert,
    LocalDiskSpaceAlert,
    PumpPressureAlert,
    QueueStopAlert,
    RawFileErrorAlert,
    S3UploadFailureAlert,
    StaleStatusAlert,
    StatusPileUpAlert,
    WebAppHealthAlert,
    config,
)
from alerts.base_alert import Issue
from alerts.config import Destinations
from alerts.recipients import get_messenger_ids_by_initials, get_recipients
from messenger import Message, send
from utils import ensure_utc

from shared.db.interface import (
    add_alert_issue,
    get_alert_issues,
    set_alert_issue_notified,
)
from shared.db.models import AlertIssue, KrakenStatus


class AlertManager:
    """Runs the alert checks and decides what to notify."""

    def __init__(self):
        """Initialize with the alerts of both tiers."""
        # checked without DB access, with an in-memory cooldown; work while the DB is down
        self.db_free_alerts: list[BaseAlert] = [
            LocalDiskSpaceAlert(),
            WebAppHealthAlert(),
        ]
        # governed by `AlertIssue` records in the DB
        self.alerts: list[BaseAlert] = [
            StaleStatusAlert(),
            DiskSpaceAlert(),
            HealthCheckFailedAlert(),
            StatusPileUpAlert(),
            InstrumentFilePileUpAlert(),
            InstrumentStallAlert(),
            RawFileErrorAlert(),
            S3UploadFailureAlert(),
            PumpPressureAlert(),
            QueueStopAlert(),
        ]
        self._last_sent: dict[str, datetime] = {}

    def check_for_issues(self, now: datetime) -> None:
        """Run the DB-backed checks and notify the issues that are due."""
        logging.info("Checking for issues...")
        status_objects = list(KrakenStatus.objects)
        records = {(r.alert, r.key): r for r in get_alert_issues()}
        messenger_ids_by_initials = get_messenger_ids_by_initials()

        for alert in self.alerts:
            due: list[tuple[Issue, AlertIssue]] = []
            for issue in alert.get_issues(status_objects, now):
                record = records.get((alert.name, issue.key))
                if record is None:
                    record = add_alert_issue(
                        alert=alert.name,
                        key=issue.key,
                        identifier=issue.identifier,
                        instrument_id=issue.instrument_id,
                        relevant_file=issue.relevant_file,
                        details=str(issue.details),
                        first_seen=now,
                    )
                if _is_due(record, alert.get_cooldown_minutes(issue), now):
                    due.append((issue, record))

            # the records are only marked as notified once sent, so failed sends are retried
            delivered = _dispatch(
                alert, [issue for issue, _ in due], messenger_ids_by_initials
            )
            for issue, record in due:
                if issue in delivered:
                    set_alert_issue_notified(record, now)

    def check_db_free_alerts(self, now: datetime) -> None:
        """Run the checks that need no DB, sending each issue with an in-memory cooldown."""
        for alert in self.db_free_alerts:
            for issue in alert.get_issues([], now):
                self.send_once(
                    f"{alert.name}:{issue.key}",
                    Message(alert.format_message([issue]), alert.title),
                    alert.get_cooldown_minutes(issue),
                    now,
                )

    def send_once(
        self, key: str, message: Message, cooldown_minutes: int | None, now: datetime
    ) -> None:
        """Send a message to the ops channel, at most once per cooldown for the same key.

        A cooldown of None sends only once. A failed send is retried on the next call.
        """
        last_sent = self._last_sent.get(key)
        if last_sent is not None and (
            cooldown_minutes is None
            or now < last_sent + timedelta(minutes=cooldown_minutes)
        ):
            return

        message = Message(f"{message.body} [{key}]", message.title, message.type)
        if send(message, {Destinations.OPS}):
            self._last_sent[key] = now


def _is_due(record: AlertIssue, cooldown_minutes: int | None, now: datetime) -> bool:
    """Return whether the issue should be notified now."""
    if record.last_notified is None:
        return True
    if cooldown_minutes is None:
        return False
    return now > ensure_utc(record.last_notified) + timedelta(minutes=cooldown_minutes)


def _dispatch(
    alert: BaseAlert, issues: list[Issue], messenger_ids_by_initials: dict[str, str]
) -> list[Issue]:
    """Send the issues to the destinations of the alert, return the issues that were delivered.

    Channels get one message with all issues, so they are delivered all or none. Users get
    one message per issue of their files, delivered per issue.
    """
    if not issues:
        return []
    destinations = config.ALERT_DESTINATIONS.get(
        alert.name, config.DEFAULT_ALERT_DESTINATIONS
    )
    delivered = list(issues)

    channels = destinations - {Destinations.USER}
    if channels and not send(
        Message(alert.format_message(issues), alert.title), channels
    ):
        delivered = []

    if Destinations.USER in destinations:
        for issue in issues:
            recipients = get_recipients(issue.relevant_file, messenger_ids_by_initials)
            if not recipients:
                logging.info(
                    f"No recipients found for {alert.name} issue {issue.identifier}"
                )
                continue
            message = Message(alert.format_message([issue]), alert.title)
            if not send(message, recipients=recipients) and issue in delivered:
                delivered.remove(issue)

    return delivered
