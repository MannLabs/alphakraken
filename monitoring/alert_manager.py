"""Decide when to send alerts based on monitoring checks."""

import logging
from collections.abc import Hashable
from datetime import datetime, timedelta

import pytz
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
from alerts.base_alert import CustomAlert, Issue
from alerts.config import Destinations
from alerts.recipients import get_recipients
from messenger_clients import AlertTypes, send_channel_message, send_direct_message
from requests.exceptions import RequestException

from shared.db.models import KrakenStatus

_WEBHOOK_URLS = {
    Destinations.OPS: config.OPS_ALERTS_WEBHOOK_URL,
    Destinations.BUSINESS: config.BUSINESS_ALERTS_WEBHOOK_URL,
}


class AlertManager:
    """Manages all alert checks and sending."""

    def __init__(self):
        """Initialize the AlertManager with checkers and last alert times."""
        # TODO: currently this grows indefinitely, we assume regular restarts of the monitoring app
        self.last_alerts: dict[str, datetime] = {}
        # (alert name, unique key) of handled issues, to report those only once
        self.reported_unique_keys: set[tuple[str, Hashable]] = set()

        self.is_first_check = True
        self.alerts: list[BaseAlert] = [
            StaleStatusAlert(),
            DiskSpaceAlert(),
            LocalDiskSpaceAlert(),
            HealthCheckFailedAlert(),
            StatusPileUpAlert(),
            InstrumentFilePileUpAlert(),
            InstrumentStallAlert(),
            RawFileErrorAlert(),
            S3UploadFailureAlert(),
            WebAppHealthAlert(),
            PumpPressureAlert(),
            QueueStopAlert(),
        ]

    def check_for_issues(self) -> None:
        """Run all alert checks."""
        logging.info("Checking for issues...")
        status_objects = list(KrakenStatus.objects)

        for alert in self.alerts:
            issues = alert.get_issues(status_objects)
            if issues:
                self._handle_issues(alert, issues, suppress_alerts=self.is_first_check)

        self.is_first_check = False

    def _handle_issues(
        self, alert: BaseAlert, issues: list[Issue], *, suppress_alerts: bool = False
    ) -> None:
        """Handle sending an alert if cooldown has passed.

        Issues are only marked as reported once handled, so issues blocked by cooldown or
        failed sends are raised again on the next check.
        """
        issues = [
            issue
            for issue in issues
            if (alert.name, issue.unique_key) not in self.reported_unique_keys
        ]
        if not issues or not self.should_send_alert(issues, alert):
            return

        if suppress_alerts:
            logging.info(f"Suppressed alert for {alert.name} ({len(issues)} issues)")
        elif not _dispatch(alert, issues):
            logging.warning(f"Failed to send {alert.name}, retrying on next check")
            return

        for issue in issues:
            self.set_last_alert_time(alert.name, issue)
            if issue.unique_key is not None:
                self.reported_unique_keys.add((alert.name, issue.unique_key))

    def should_send_alert(
        self,
        issues: list[Issue],
        alert: BaseAlert,
        cooldown_time_minutes: int | None = None,
    ) -> bool:
        """Check if we should send an alert based on cooldown period."""
        send_alert = False
        for issue in issues:
            if cooldown_time_minutes is not None:
                effective_cooldown_time_minutes = cooldown_time_minutes
            else:
                effective_cooldown_time_minutes = alert.get_cooldown_time_minutes(issue)

            last_alert_time = self.last_alerts.get(
                self._get_alert_id(alert.name, issue)
            )
            send_alert |= last_alert_time is None or datetime.now(
                pytz.UTC
            ) > last_alert_time + timedelta(minutes=effective_cooldown_time_minutes)
        return send_alert

    def set_last_alert_time(self, alert_name: str, issue: Issue) -> None:
        """Get the alert ID for tracking last sent time."""
        alert_id = self._get_alert_id(alert_name, issue)
        self.last_alerts[alert_id] = datetime.now(pytz.UTC)

    @staticmethod
    def _get_alert_id(alert_name: str, issue: Issue) -> str:
        """Get the alert ID for tracking last sent time."""
        alert_id = f"{alert_name}_{issue.identifier}"
        return f"{alert_id}_{issue.kind}" if issue.kind else alert_id


def _dispatch(alert: BaseAlert, issues: list[Issue]) -> bool:
    """Send the issues to all destinations configured for the alert, return whether all sends succeeded.

    A failed send to one destination or recipient does not abort the rest.
    """
    all_sent = True
    destinations = config.ALERT_DESTINATIONS.get(
        alert.name, config.DEFAULT_ALERT_DESTINATIONS
    )

    # set: the business webhook URL falls back to the ops one if not configured
    webhook_urls = {
        _WEBHOOK_URLS[destination]
        for destination in destinations
        if destination != Destinations.USER
    }
    if webhook_urls:
        message = alert.format_message(issues)
        for webhook_url in webhook_urls:
            try:
                send_channel_message(message, webhook_url)
            except Exception:  # noqa: PERF203
                logging.exception(f"Failed to send {alert.name} channel message")
                all_sent = False

    if Destinations.USER in destinations:
        all_sent &= _send_direct_messages(alert, issues)

    return all_sent


def _send_direct_messages(alert: BaseAlert, issues: list[Issue]) -> bool:
    """Send one direct message per issue to each recipient derived from the issue's relevant file.

    Return whether all sends succeeded.
    """
    all_sent = True
    for issue in issues:
        recipients = get_recipients(issue.relevant_file)
        if not recipients:
            logging.info(
                f"No recipients found for {alert.name} issue {issue.identifier}"
            )
            continue

        message = alert.format_message([issue])
        for recipient in recipients:
            try:
                send_direct_message(message, recipient)
            except Exception:  # noqa: PERF203
                logging.exception(
                    f"Failed to send {alert.name} direct message "
                    f"(recipient={recipient}, identifier={issue.identifier})"
                )
                all_sent = False
    return all_sent


def send_special_alert(
    identifier: str,
    alert_name: str,
    message: str,
    alert_manager: AlertManager,
    alert_type: str = AlertTypes.ALERT,
) -> None:
    """Send simple alerts."""
    # alert is only used for determining cooldown here
    alert = CustomAlert(alert_name, 10)
    issue = Issue(identifier, message)
    if not alert_manager.should_send_alert([issue], alert):
        return

    message = f"{message} [{alert_name} {identifier}]"
    try:
        send_channel_message(message, config.OPS_ALERTS_WEBHOOK_URL, alert_type)
    except RequestException:
        logging.exception("Failed to send special alert message.")
    else:
        alert_manager.set_last_alert_time(alert_name, issue)
