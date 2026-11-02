"""Unit tests for the alert destinations read from the yaml settings."""

from unittest.mock import patch

from monitoring.alerts.queue_stop_alert import QueueStopAlert


@patch(
    "monitoring.alerts.base_alert.ALERT_DESTINATIONS",
    {"QueueStopAlert": frozenset({"business"})},
)
def test_destinations_come_from_the_yaml_mapping() -> None:
    """An alert's destinations are looked up by its class name."""
    assert QueueStopAlert().destinations == frozenset({"business"})
