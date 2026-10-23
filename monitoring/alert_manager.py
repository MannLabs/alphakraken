"""Decide when to send alerts based on monitoring checks."""

import logging
from dataclasses import dataclass, field
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
        # consecutive failed channel sends per alert class
        self._failed_sends: dict[str, int] = {}

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
            result = _dispatch(
                alert, [issue for issue, _ in due], messenger_ids_by_initials
            )
            undelivered = [(i, r) for i, r in due if i not in result.delivered]
            if not result.channel_failed:
                self._failed_sends.pop(alert.name, None)
            elif self._give_up(alert, len(undelivered), now):
                undelivered = []
            for issue, record in due:
                if (issue, record) not in undelivered:
                    set_alert_issue_notified(record, now)

    def _give_up(self, alert: BaseAlert, n_issues: int, now: datetime) -> bool:
        """Count a failed channel send; after MAX_SEND_ATTEMPTS, tell ops and return True.

        Giving up marks the issues as notified, so that a message failing for good (e.g.
        a dead webhook) does not hold back every later issue of the alert.
        """
        failures = self._failed_sends.get(alert.name, 0) + 1
        if failures < config.MAX_SEND_ATTEMPTS:
            self._failed_sends[alert.name] = failures
            return False

        self._failed_sends.pop(alert.name, None)
        channels = ", ".join(sorted(_channels(alert)))
        self.send_once(
            f"send_failed:{alert.name}",
            Message(
                f"Gave up on {n_issues} issues of {alert.name} after {failures} failed "
                f"sends to {channels}; see the log"
            ),
            config.SPECIAL_ALERT_COOLDOWN_MINUTES,
            now,
        )
        return True

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


@dataclass
class DispatchResult:
    """What a dispatch delivered, and whether a channel send failed."""

    delivered: list[Issue] = field(default_factory=list)
    channel_failed: bool = False


def _channels(alert: BaseAlert) -> set[str]:
    destinations = config.ALERT_DESTINATIONS.get(
        alert.name, config.DEFAULT_ALERT_DESTINATIONS
    )
    return destinations - {Destinations.USER}


def _dispatch(
    alert: BaseAlert, issues: list[Issue], messenger_ids_by_initials: dict[str, str]
) -> DispatchResult:
    """Send the issues to the destinations of the alert and return what was delivered.

    Channels get messages of at most MAX_ISSUES_PER_MESSAGE issues, delivered per message.
    Users get one message per issue of their files, delivered per issue.
    """
    if not issues:
        return DispatchResult()
    result = DispatchResult(delivered=list(issues))

    channels = _channels(alert)
    if channels:
        for start in range(0, len(issues), config.MAX_ISSUES_PER_MESSAGE):
            chunk = issues[start : start + config.MAX_ISSUES_PER_MESSAGE]
            if not send(Message(alert.format_message(chunk), alert.title), channels):
                result.channel_failed = True
                result.delivered = [i for i in result.delivered if i not in chunk]

    if Destinations.USER in config.ALERT_DESTINATIONS.get(
        alert.name, config.DEFAULT_ALERT_DESTINATIONS
    ):
        for issue in issues:
            recipients = get_recipients(issue.relevant_file, messenger_ids_by_initials)
            if not recipients:
                logging.info(
                    f"No recipients found for {alert.name} issue {issue.identifier}"
                )
                continue
            message = Message(alert.format_message([issue]), alert.title)
            if not send(message, recipients=recipients) and issue in result.delivered:
                result.delivered.remove(issue)

    return result
