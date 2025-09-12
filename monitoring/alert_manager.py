"""Decide when to send alerts based on monitoring checks."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
from alerts import (
    BaseAlert,
    DiskSpaceAlert,
    HealthCheckFailedAlert,
    InstrumentFilePileUpAlert,
    RawFileErrorAlert,
    StaleStatusAlert,
    StatusPileUpAlert,
    config,
)
from messenger_clients import send_message
from requests.exceptions import RequestException

from shared.db.models import KrakenStatus


def _default_value() -> datetime:
    """Special default value for defaultdict to have an alert on the first occurrence."""
    return datetime.now(pytz.UTC) - timedelta(minutes=config.ALERT_COOLDOWN_MINUTES + 1)


class AlertManager:
    """Manages all alert checks and sending."""

    def __init__(self):
        """Initialize the AlertManager with checkers and last alert times."""
        self.last_alerts = defaultdict(_default_value)
        self.alerts: list[BaseAlert] = [
            StaleStatusAlert(),
            DiskSpaceAlert(),
            HealthCheckFailedAlert(),
            StatusPileUpAlert(),
            InstrumentFilePileUpAlert(),
            RawFileErrorAlert(),
        ]

    def check_for_issues(self) -> None:
        """Run all alert checks."""
        logging.info("Checking kraken update status...")
        status_objects = list(KrakenStatus.objects)

        for alert in self.alerts:
            issues = alert.get_issues(status_objects)
            if issues:
                self._handle_issues(alert, issues)

    def _handle_issues(self, alert: BaseAlert, issues: list[tuple]) -> None:
        """Handle sending an alert if cooldown has passed."""
        alert_name = alert.name
        identifiers = [issue[0] for issue in issues]

        if self.should_send_alert(identifiers, alert_name):
            message = alert.format_message(issues)
            try:
                send_message(message)
                for identifier in identifiers:
                    self.last_alerts[f"{alert_name}_{identifier}"] = datetime.now(
                        pytz.UTC
                    )
            except RequestException:
                logging.exception("Failed to send message.")

    def should_send_alert(self, identifiers: list[str], alert_name: str) -> bool:
        """Check if we should send an alert based on cooldown period."""
        send_alert = False
        for identifier in identifiers:
            cooldown_time = self.last_alerts[f"{alert_name}_{identifier}"] + timedelta(
                minutes=config.ALERT_COOLDOWN_MINUTES
            )
            send_alert |= datetime.now(pytz.UTC) > cooldown_time
        return send_alert


def send_db_alert(alert_name: str, alert_manager: AlertManager) -> None:
    """Send message about MongoDB error."""
    identifier = "db"

    if not alert_manager.should_send_alert([identifier], alert_name):
        return

    logging.info(f"Error connecting to MongoDB: {alert_name}")

    message = f"Error connecting to MongoDB: {alert_name}"
    try:
        send_message(message)
    except RequestException:
        logging.exception("Failed to send DB alert message.")
    else:
        alert_manager.last_alerts[f"{alert_name}_{identifier}"] = datetime.now(pytz.UTC)
