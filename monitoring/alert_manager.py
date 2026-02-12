"""Decide when to send alerts based on monitoring checks."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
from alerts import (
    BackupFailureAlert,
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
from alerts.base_alert import CustomAlert
from messenger_clients import AlertTypes, send_message
from requests.exceptions import RequestException

from shared.db.models import KrakenStatus


def _default_value() -> datetime:
    """Special default value for defaultdict to have an alert on the first occurrence."""
    return datetime.now(pytz.UTC) - timedelta(
        minutes=config.DEFAULT_ALERT_COOLDOWN_TIME_MINUTES + 1
    )


class AlertManager:
    """Manages all alert checks and sending."""

    def __init__(self):
        """Initialize the AlertManager with checkers and last alert times."""
        self.last_alerts = defaultdict(_default_value)
        self.is_first_check = True
        self.alerts: list[BaseAlert] = [
            StaleStatusAlert(),
            DiskSpaceAlert(),
            HealthCheckFailedAlert(),
            StatusPileUpAlert(),
            InstrumentFilePileUpAlert(),
            RawFileErrorAlert(),
            BackupFailureAlert(),
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
                self._handle_issues(alert, issues, suppress_alerts=self.is_first_check)

        self.is_first_check = False

    def _handle_issues(
        self, alert: BaseAlert, issues: list[tuple], *, suppress_alerts: bool = False
    ) -> None:
        """Handle sending an alert if cooldown has passed."""
        alert_name = alert.name
        identifiers = [issue[0] for issue in issues]

        if self.should_send_alert(identifiers, alert):
            message = alert.format_message(issues)

            webhook_url = alert.get_webhook_url()
            if not suppress_alerts:
                send_message(message, webhook_url)
            else:
                logging.info(f"Suppressed alert for {alert_name}: {message}")

            for identifier in identifiers:
                self.set_last_alert_time(alert_name, identifier)

    def should_send_alert(
        self,
        identifiers: list[str],
        alert: BaseAlert,
        cooldown_time_minutes: int | None = None,
    ) -> bool:
        """Check if we should send an alert based on cooldown period."""
        send_alert = False
        for identifier in identifiers:
            if cooldown_time_minutes is not None:
                effective_cooldown_time_minutes = cooldown_time_minutes
            else:
                effective_cooldown_time_minutes = alert.get_cooldown_time_minutes(
                    identifier
                )

            earliest_next_alert_time = self._get_last_alert_time(
                alert.name, identifier
            ) + timedelta(minutes=effective_cooldown_time_minutes)
            send_alert |= datetime.now(pytz.UTC) > earliest_next_alert_time
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
    identifier: str,
    alert_name: str,
    message: str,
    alert_manager: AlertManager,
    alert_type: str = AlertTypes.ALERT,
) -> None:
    """Send simple alerts."""
    # alert is only used for determining cooldown here
    alert = CustomAlert(alert_name, 10)
    if not alert_manager.should_send_alert([identifier], alert):
        return

    message = f"{message} [{alert_name} {identifier}]"
    try:
        send_message(message, config.OPS_ALERTS_WEBHOOK_URL, alert_type)
    except RequestException:
        logging.exception("Failed to send special alert message.")
    else:
        alert_manager.set_last_alert_time(alert_name, identifier)
