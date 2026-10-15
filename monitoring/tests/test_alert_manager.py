"""Unit tests for AlertManager class."""

from collections.abc import Generator
from contextlib import AbstractContextManager
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests

from monitoring.alert_manager import AlertManager, _dispatch, send_special_alert
from monitoring.alerts.base_alert import Issue
from monitoring.alerts.config import Destinations

BUSINESS, OPS, USER = Destinations.BUSINESS, Destinations.OPS, Destinations.USER


class TestAlertManager:
    """Test suite for AlertManager class."""

    def test_check_for_issues_should_run_all_alert_checks_and_send_alerts(self) -> None:
        """Test that check_for_issues runs all checks and sends alerts for issues found."""
        # given
        with (
            patch("monitoring.alert_manager.config.ALERT_DESTINATIONS", {}),
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch(
                "monitoring.alert_manager.send_channel_message"
            ) as mock_send_channel_message,
        ):
            # Mock status objects
            mock_status_obj = Mock()
            mock_kraken_status.objects = [mock_status_obj]

            alert_manager = AlertManager()

            # Mock one alert to return issues
            mock_issue = Issue("test_id", "test_data")
            alert_manager.alerts[0].get_issues = Mock(return_value=[mock_issue])
            alert_manager.alerts[0].format_message = Mock(
                return_value="Test alert message"
            )

            # Set is_first_check to False so alerts are sent
            alert_manager.is_first_check = False

            # Mock all other alerts to return no issues
            for alert in alert_manager.alerts[1:]:
                alert.get_issues = Mock(return_value=[])

            # when
            alert_manager.check_for_issues()

            # then
            # Should call get_issues on all alerts
            for alert in alert_manager.alerts:
                alert.get_issues.assert_called_once_with([mock_status_obj], [])

            # Should send message for the one alert with issues
            mock_send_channel_message.assert_called_once_with(
                "Test alert message", "http://ops.url"
            )

            # Should mark as not first check
            assert alert_manager.is_first_check is False

    def test_check_for_issues_should_suppress_alerts_on_first_check(self) -> None:
        """Test that alerts are suppressed when is_first_check is True."""
        # given
        with (
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch(
                "monitoring.alert_manager.send_channel_message"
            ) as mock_send_channel_message,
            patch("monitoring.alert_manager.logging") as mock_logging,
        ):
            # Mock status objects
            mock_status_obj = Mock()
            mock_kraken_status.objects = [mock_status_obj]

            alert_manager = AlertManager()

            # Mock one alert to return issues
            mock_issue = Issue("test_id", "test_data")
            alert_with_issue = alert_manager.alerts[0]
            alert_name = alert_with_issue.name
            alert_with_issue.get_issues = Mock(return_value=[mock_issue])
            alert_with_issue.format_message = Mock(return_value="Test alert message")

            # Keep is_first_check as True (default)
            assert alert_manager.is_first_check is True

            # Mock all other alerts to return no issues
            for alert in alert_manager.alerts[1:]:
                alert.get_issues = Mock(return_value=[])

            # when
            alert_manager.check_for_issues()

            # then
            # Should NOT send message because is_first_check is True
            mock_send_channel_message.assert_not_called()

            # Should log the suppressed alert
            mock_logging.info.assert_any_call(
                f"Suppressed alert for {alert_name} (1 issues)"
            )

            # Should mark as not first check after running
            assert alert_manager.is_first_check is False

            # Should still update last alert time even when suppressed
            alert_id = alert_manager._get_alert_id(alert_name, mock_issue)
            assert alert_id in alert_manager.last_alerts

    def test_check_for_issues_should_handle_multiple_alerts_with_issues(self) -> None:
        """Test that multiple alerts with issues are all handled correctly."""
        # given
        with (
            patch("monitoring.alert_manager.config.ALERT_DESTINATIONS", {}),
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch(
                "monitoring.alert_manager.send_channel_message"
            ) as mock_send_channel_message,
        ):
            # Mock status objects
            mock_status_obj = Mock()
            mock_kraken_status.objects = [mock_status_obj]

            alert_manager = AlertManager()

            # Mock three alerts to return issues
            mock_issue_1 = Issue("id_1", "data_1")
            mock_issue_2 = Issue("id_2", "data_2")
            mock_issue_3 = Issue("id_3", "data_3")

            alert_1 = alert_manager.alerts[0]
            alert_1_name = alert_1.name
            alert_1.get_issues = Mock(return_value=[mock_issue_1])
            alert_1.format_message = Mock(return_value="Message 1")

            alert_2 = alert_manager.alerts[1]
            alert_2_name = alert_2.name
            alert_2.get_issues = Mock(return_value=[mock_issue_2])
            alert_2.format_message = Mock(return_value="Message 2")

            alert_3 = alert_manager.alerts[2]
            alert_3_name = alert_3.name
            alert_3.get_issues = Mock(return_value=[mock_issue_3])
            alert_3.format_message = Mock(return_value="Message 3")

            # Set is_first_check to False so alerts are sent
            alert_manager.is_first_check = False

            # Mock remaining alerts to return no issues
            for alert in alert_manager.alerts[3:]:
                alert.get_issues = Mock(return_value=[])

            # when
            alert_manager.check_for_issues()

            # then
            # Should send all three messages
            assert mock_send_channel_message.call_count == 3
            mock_send_channel_message.assert_any_call("Message 1", "http://ops.url")
            mock_send_channel_message.assert_any_call("Message 2", "http://ops.url")
            mock_send_channel_message.assert_any_call("Message 3", "http://ops.url")

            # Should update last alert times for all identifiers
            alert_id_1 = alert_manager._get_alert_id(alert_1_name, mock_issue_1)
            alert_id_2 = alert_manager._get_alert_id(alert_2_name, mock_issue_2)
            alert_id_3 = alert_manager._get_alert_id(alert_3_name, mock_issue_3)

            assert alert_id_1 in alert_manager.last_alerts
            assert alert_id_2 in alert_manager.last_alerts
            assert alert_id_3 in alert_manager.last_alerts

    def test_should_send_alert_uses_alert_specific_cooldown(self) -> None:
        """Test that alert-specific cooldown overrides are used when configured."""
        # given
        from datetime import datetime, timedelta

        import pytz

        from monitoring.alerts.health_check_failed_alert import HealthCheckFailedAlert

        alert_manager = AlertManager()
        alert = HealthCheckFailedAlert()

        # Set last alert time to 3 hours ago for both identifiers
        three_hours_ago = datetime.now(pytz.UTC) - timedelta(hours=3)
        alert_manager.last_alerts["health_check_failed_file_remover"] = three_hours_ago
        alert_manager.last_alerts["health_check_failed_other_instrument"] = (
            three_hours_ago
        )

        # when & then
        # file_remover should NOT send alert (needs 12 hours, only 3 passed)
        should_send_file_remover = alert_manager.should_send_alert(
            [Issue("file_remover", "x")], alert
        )
        assert should_send_file_remover is False

        # other_instrument should send alert (needs 2 hours, 3 passed)
        should_send_other = alert_manager.should_send_alert(
            [Issue("other_instrument", "x")], alert
        )
        assert should_send_other is True

    def test_should_send_alert_immediately_for_new_issue_with_long_cooldown(
        self,
    ) -> None:
        """A never-alerted issue is sent right away, regardless of its cooldown."""
        # given
        from monitoring.alerts.health_check_failed_alert import HealthCheckFailedAlert

        alert_manager = AlertManager()
        issue = Issue("file_remover", "x")  # cooldown 720 min

        # when
        should_send = alert_manager.should_send_alert([issue], HealthCheckFailedAlert())

        # then
        assert should_send is True
        assert alert_manager.last_alerts == {}  # checking must not record an alert

    def test_cooldown_should_be_tracked_per_kind(self) -> None:
        """Issues with the same identifier but different kinds have separate cooldowns."""
        # given
        alert_manager = AlertManager()
        alert = _mock_alert()
        alert.get_cooldown_time_minutes.return_value = 60
        issue_a = Issue("inst1", "x", kind="a")
        issue_b = Issue("inst1", "x", kind="b")

        # when
        alert_manager.set_last_alert_time(alert.name, issue_a)

        # then
        assert alert_manager._get_alert_id(alert.name, issue_a) == "my_alert_inst1_a"
        assert alert_manager.should_send_alert([issue_a], alert) is False
        assert alert_manager.should_send_alert([issue_b], alert) is True


def _mock_alert(name: str = "my_alert") -> Mock:
    alert = Mock()
    alert.name = name
    alert.format_message.side_effect = lambda issues: "|".join(
        i.identifier for i in issues
    )
    return alert


ISSUES = [
    Issue("id1", "d1", relevant_file="file_id1"),
    Issue("id2", "d2", relevant_file="file_id2"),
]


@pytest.fixture
def mock_send_channel_message() -> Generator[Mock, None, None]:
    """Patch channel message sending."""
    with patch("monitoring.alert_manager.send_channel_message") as mock:
        yield mock


@pytest.fixture
def mock_send_direct_message() -> Generator[Mock, None, None]:
    """Patch direct message sending."""
    with patch("monitoring.alert_manager.send_direct_message") as mock:
        yield mock


@pytest.fixture(autouse=True)
def no_suppressions() -> Generator[None, None, None]:
    """Patch the suppressions loaded from the DB."""
    with patch(
        "monitoring.alert_manager.get_active_alert_suppressions", return_value=[]
    ):
        yield


@pytest.fixture(autouse=True)
def webhook_urls() -> Generator[None, None, None]:
    """Patch the webhook URLs per destination."""
    with patch.dict(
        "monitoring.alert_manager._WEBHOOK_URLS",
        {OPS: "http://ops.url", BUSINESS: "http://business.url"},
    ):
        yield


def _set_destinations(destinations: set[str]) -> AbstractContextManager[Any]:
    return patch(
        "monitoring.alert_manager.config.ALERT_DESTINATIONS",
        {"my_alert": destinations},
    )


class TestDispatch:
    """Test suite for routing alerts to their configured destinations."""

    def test_dispatch_should_use_default_destinations_for_unconfigured_alert(
        self, mock_send_channel_message: Mock, mock_send_direct_message: Mock
    ) -> None:
        """Alerts missing from ALERT_DESTINATIONS go to the ops channel."""
        # given
        alert = _mock_alert("unconfigured_alert")

        # when
        with _set_destinations({BUSINESS}):
            _dispatch(alert, ISSUES)

        # then
        mock_send_channel_message.assert_called_once_with("id1|id2", "http://ops.url")
        mock_send_direct_message.assert_not_called()

    def test_dispatch_should_send_to_all_configured_destinations(
        self, mock_send_channel_message: Mock, mock_send_direct_message: Mock
    ) -> None:
        """One aggregated message per channel, one direct message per issue and recipient."""
        # given
        alert = _mock_alert()
        recipients = {"file_id1": ["U_A", "U_SUP"], "file_id2": ["U_B", "U_SUP"]}

        # when
        with (
            _set_destinations({USER, OPS, BUSINESS}),
            patch(
                "monitoring.alert_manager.get_recipients",
                side_effect=lambda f: recipients[f],
            ),
        ):
            all_sent = _dispatch(alert, ISSUES)

        # then
        assert all_sent is True
        assert sorted(c.args for c in mock_send_channel_message.call_args_list) == [
            ("id1|id2", "http://business.url"),
            ("id1|id2", "http://ops.url"),
        ]
        assert [c.args for c in mock_send_direct_message.call_args_list] == [
            ("id1", "U_A"),
            ("id1", "U_SUP"),
            ("id2", "U_B"),
            ("id2", "U_SUP"),
        ]

    def test_dispatch_should_send_once_when_channels_share_webhook_url(
        self, mock_send_channel_message: Mock
    ) -> None:
        """The business webhook URL falling back to the ops one must not yield duplicates."""
        # given
        alert = _mock_alert()

        # when
        with (
            _set_destinations({OPS, BUSINESS}),
            patch.dict(
                "monitoring.alert_manager._WEBHOOK_URLS", {BUSINESS: "http://ops.url"}
            ),
        ):
            _dispatch(alert, ISSUES)

        # then
        mock_send_channel_message.assert_called_once_with("id1|id2", "http://ops.url")

    def test_dispatch_should_skip_issues_without_recipients(
        self, mock_send_channel_message: Mock, mock_send_direct_message: Mock
    ) -> None:
        """Issues whose relevant file maps to no user are not sent as direct messages."""
        # given
        alert = _mock_alert()
        recipients = {"file_id1": [], "file_id2": ["U_B"]}

        # when
        with (
            _set_destinations({USER}),
            patch(
                "monitoring.alert_manager.get_recipients",
                side_effect=lambda f: recipients[f],
            ),
        ):
            _dispatch(alert, ISSUES)

        # then
        mock_send_direct_message.assert_called_once_with("id2", "U_B")
        mock_send_channel_message.assert_not_called()

    def test_dispatch_should_continue_after_failed_sends(
        self, mock_send_channel_message: Mock, mock_send_direct_message: Mock
    ) -> None:
        """A failed send to one destination or recipient does not abort the rest."""
        # given
        alert = _mock_alert()
        mock_send_channel_message.side_effect = requests.HTTPError("500")
        mock_send_direct_message.side_effect = [requests.HTTPError("500"), None, None]

        # when
        with (
            _set_destinations({USER, OPS}),
            patch(
                "monitoring.alert_manager.get_recipients",
                side_effect=[["U_A", "U_SUP"], ["U_B"]],
            ),
        ):
            all_sent = _dispatch(alert, ISSUES)

        # then
        assert all_sent is False
        assert mock_send_channel_message.call_count == 1
        assert [c.args for c in mock_send_direct_message.call_args_list] == [
            ("id1", "U_A"),
            ("id1", "U_SUP"),
            ("id2", "U_B"),
        ]


UNIQUE_KEY_ISSUES = [
    Issue("id1", "d1", unique_key="k1"),
    Issue("id2", "d2", unique_key="k2"),
]


class TestHandleIssues:
    """Test suite for reporting issues with a unique key only once they are handled."""

    @pytest.mark.parametrize(
        ("suppress_alerts", "all_sent"),
        [(False, True), (True, False)],
    )
    def test_handle_issues_should_report_unique_keys_once_when_sent_or_suppressed(
        self, *, suppress_alerts: bool, all_sent: bool
    ) -> None:
        """Sent or deliberately suppressed issues are not raised again, even without cooldown."""
        # given
        alert_manager = AlertManager()
        alert = _mock_alert()
        alert.get_cooldown_time_minutes.return_value = 0

        # when
        with patch(
            "monitoring.alert_manager._dispatch", return_value=all_sent
        ) as mock_dispatch:
            alert_manager._handle_issues(
                alert, UNIQUE_KEY_ISSUES, suppress_alerts=suppress_alerts
            )
            alert_manager._handle_issues(alert, UNIQUE_KEY_ISSUES)

        # then
        assert alert_manager.reported_unique_keys == {
            ("my_alert", "k1"),
            ("my_alert", "k2"),
        }
        assert set(alert_manager.last_alerts) == {"my_alert_id1", "my_alert_id2"}
        assert mock_dispatch.call_count == (0 if suppress_alerts else 1)

    def test_handle_issues_should_only_send_issues_not_reported_yet(self) -> None:
        """Already reported issues are dropped, new ones are sent."""
        # given
        alert_manager = AlertManager()
        alert = _mock_alert()
        alert.get_cooldown_time_minutes.return_value = 0
        alert_manager.reported_unique_keys.add(("my_alert", "k1"))

        # when
        with patch(
            "monitoring.alert_manager._dispatch", return_value=True
        ) as mock_dispatch:
            alert_manager._handle_issues(alert, UNIQUE_KEY_ISSUES)

        # then
        mock_dispatch.assert_called_once_with(alert, [UNIQUE_KEY_ISSUES[1]])

    def test_handle_issues_should_send_issues_without_unique_key_again(self) -> None:
        """Issues without unique key are only limited by their cooldown."""
        # given
        alert_manager = AlertManager()
        alert = _mock_alert()
        alert.get_cooldown_time_minutes.return_value = 0

        # when
        with patch(
            "monitoring.alert_manager._dispatch", return_value=True
        ) as mock_dispatch:
            alert_manager._handle_issues(alert, ISSUES)
            alert_manager._handle_issues(alert, ISSUES)

        # then
        assert mock_dispatch.call_count == 2
        assert alert_manager.reported_unique_keys == set()

    def test_handle_issues_should_retry_when_send_failed(self) -> None:
        """Issues whose send failed are neither reported nor start their cooldown."""
        # given
        alert_manager = AlertManager()
        alert = _mock_alert()
        alert.get_cooldown_time_minutes.return_value = 60

        # when
        with patch(
            "monitoring.alert_manager._dispatch", side_effect=[False, True]
        ) as mock_dispatch:
            alert_manager._handle_issues(alert, UNIQUE_KEY_ISSUES)
            assert alert_manager.reported_unique_keys == set()
            alert_manager._handle_issues(alert, UNIQUE_KEY_ISSUES)

        # then
        assert mock_dispatch.call_count == 2
        assert len(alert_manager.reported_unique_keys) == 2

    def test_handle_issues_should_not_report_when_blocked_by_cooldown(
        self,
    ) -> None:
        """Issues blocked by cooldown are not reported, so they are raised again later."""
        # given
        alert_manager = AlertManager()
        alert = _mock_alert()
        alert.get_cooldown_time_minutes.return_value = 60
        for issue in UNIQUE_KEY_ISSUES:
            alert_manager.set_last_alert_time(alert.name, issue)

        # when
        with patch("monitoring.alert_manager._dispatch") as mock_dispatch:
            alert_manager._handle_issues(alert, UNIQUE_KEY_ISSUES)

        # then
        mock_dispatch.assert_not_called()
        assert alert_manager.reported_unique_keys == set()


class TestCheckForIssues:
    """Test suite for sending alerts only from the active instance."""

    @pytest.mark.parametrize("is_active", [True, False])
    def test_check_for_issues_should_send_only_as_active_instance(
        self, *, is_active: bool
    ) -> None:
        """Inactive instances handle issues as suppressed, keeping their state in sync."""
        # given
        with (
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch(
                "monitoring.alert_manager._dispatch", return_value=True
            ) as mock_dispatch,
        ):
            mock_kraken_status.objects = []
            alert_manager = AlertManager()
            alert_manager.is_first_check = False
            alert_manager.alerts = [_mock_alert()]
            alert_manager.alerts[0].get_issues.return_value = UNIQUE_KEY_ISSUES
            alert_manager.alerts[0].get_cooldown_time_minutes.return_value = 60

            # when
            alert_manager.check_for_issues(is_active=is_active)

        # then
        alert_manager.alerts[0].get_issues.assert_called_once_with([], [])
        assert mock_dispatch.call_count == (1 if is_active else 0)
        assert alert_manager.reported_unique_keys == {
            ("my_alert", "k1"),
            ("my_alert", "k2"),
        }


class TestSendSpecialAlert:
    """Test suite for send_special_alert function."""

    def test_send_special_alert_should_send_channel_message_when_cooldown_expired(
        self,
    ) -> None:
        """Test that send_special_alert sends message when cooldown has expired."""
        # given
        with patch(
            "monitoring.alert_manager.send_channel_message"
        ) as mock_send_channel_message:
            alert_manager = AlertManager()
            identifier = "test_identifier"
            alert_name = "test_alert"
            message = "Test message"

            # when
            send_special_alert(identifier, alert_name, message, alert_manager)

            # then
            expected_message = f"{message} [{alert_name} {identifier}]"
            mock_send_channel_message.assert_called_once()
            assert mock_send_channel_message.call_args[0][0] == expected_message
