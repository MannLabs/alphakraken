"""Decide when to send alerts based on monitoring checks."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pytz

try:
    # Try relative imports when used as part of the monitoring package
    from . import config
    from .alert_checkers import (
        BaseAlert,
        DiskSpaceAlert,
        HealthCheckAlert,
        InstrumentFilePileUpAlert,
        RawFileErrorAlert,
        StaleStatusAlert,
        StatusPileUpAlert,
    )
    from .messenger_clients import send_message
except ImportError:
    # Fallback to direct imports when run from within the monitoring directory
    import config
    from alert_checkers import (
        BaseAlert,
        DiskSpaceAlert,
        HealthCheckAlert,
        InstrumentFilePileUpAlert,
        RawFileErrorAlert,
        StaleStatusAlert,
        StatusPileUpAlert,
    )
    from messenger_clients import send_message
from requests.exceptions import RequestException

from shared.db.models import KrakenStatus


def _default_value() -> datetime:
    """Default value for defaultdict to have an alert on the first occurrence."""
    return datetime.now(pytz.UTC) - timedelta(minutes=config.ALERT_COOLDOWN_MINUTES + 1)


class AlertManager:
    """Manages all alert checks and sending."""

    def __init__(self):
        """Initialize the AlertManager with checkers and last alert times."""
        self.last_alerts = defaultdict(_default_value)
        self.checkers: list[BaseAlert] = [
            StaleStatusAlert(),
            DiskSpaceAlert(),
            HealthCheckAlert(),
            StatusPileUpAlert(),
            InstrumentFilePileUpAlert(),
            RawFileErrorAlert(),
        ]

    def check_all(self) -> None:
        """Run all alert checks."""
        logging.info("Checking kraken update status...")
        kraken_statuses = KrakenStatus.objects

        for checker in self.checkers:
            issues = checker.check(kraken_statuses)
            if issues:
                self._handle_alert(checker, issues)

    def _handle_alert(self, checker: BaseAlert, issues: list[tuple]) -> None:
        """Handle sending an alert if cooldown has passed."""
        case = checker.case_name
        identifiers = [item[0] for item in issues]

        if self.should_send_alert(identifiers, case):
            message = checker.format_message(issues)
            try:
                send_message(message)
                for identifier in identifiers:
                    self.last_alerts[f"{case}{identifier}"] = datetime.now(pytz.UTC)
            except RequestException:
                logging.exception("Failed to send message.")

    def should_send_alert(self, issue_types: list[str], case: str) -> bool:
        """Check if we should send an alert based on cooldown period."""
        send_alert = False
        for issue_type in issue_types:
            cooldown_time = self.last_alerts[f"{case}{issue_type}"] + timedelta(
                minutes=config.ALERT_COOLDOWN_MINUTES
            )
            send_alert |= datetime.now(pytz.UTC) > cooldown_time
        return send_alert


def send_db_alert(error_type: str, alert_manager: AlertManager) -> None:
    """Send message about MongoDB connection error."""
    if not alert_manager.should_send_alert([error_type], "db"):
        return

    logging.info("Error connecting to MongoDB")

    message = f"Error connecting to MongoDB: {error_type}"
    try:
        send_message(message)
        alert_manager.last_alerts[f"db{error_type}"] = datetime.now(pytz.UTC)
    except RequestException:
        logging.exception("Failed to send DB alert message.")
