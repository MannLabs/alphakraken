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
    PumpPressureAlert,
    RawFileErrorAlert,
    StaleStatusAlert,
    StatusPileUpAlert,
    WebAppHealthAlert,
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
            WebAppHealthAlert(),
            PumpPressureAlert(),
        ]

    def check_for_issues(self) -> None:
        """Run all alert checks."""
        logging.info("Checking for issues...")
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
            send_message(message)
            for identifier in identifiers:
                self.set_last_alert_time(alert_name, identifier)

    def should_send_alert(
        self,
        identifiers: list[str],
        alert_name: str,
        cooldown_minutes: int | None = None,
    ) -> bool:
        """Check if we should send an alert based on cooldown period."""
        send_alert = False
        for identifier in identifiers:
            cooldown_time = self._get_last_alert_time(
                alert_name, identifier
            ) + timedelta(
                minutes=config.ALERT_COOLDOWN_MINUTES
                if cooldown_minutes is None
                else cooldown_minutes
            )
            send_alert |= datetime.now(pytz.UTC) > cooldown_time
        return send_alert

    def set_last_alert_time(self, alert_name: str, identifier: str) -> None:
        """Get the alert ID for tracking last sent time."""
        alert_id = self._get_alert_id(alert_name, identifier)
        self.last_alerts[alert_id] = datetime.now(pytz.UTC)

    def _get_last_alert_time(self, alert_name: str, identifier: str) -> datetime:
        """Get the alert ID for tracking last sent time."""
        alert_id = self._get_alert_id(alert_name, identifier)
        return self.last_alerts[alert_id]

    @staticmethod
    def _get_alert_id(alert_name: str, identifier: str) -> str:
        """Get the alert ID for tracking last sent time."""
        return f"{alert_name}_{identifier}"


def send_special_alert(
    identifier: str, alert_name: str, message: str, alert_manager: AlertManager
) -> None:
    """Send simple alerts."""
    if not alert_manager.should_send_alert(
        [identifier], alert_name, cooldown_minutes=10
    ):
        return

    message = f"{message} [{alert_name} {identifier}]"
    try:
        send_message(message)
    except RequestException:
        logging.exception("Failed to send special alert message.")
    else:
        alert_manager.set_last_alert_time(alert_name, identifier)
