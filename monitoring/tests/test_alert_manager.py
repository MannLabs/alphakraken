"""Unit tests for AlertManager class."""

from unittest.mock import Mock, patch

import pytest

from monitoring.alert_manager import AlertManager, send_special_alert


class TestAlertManager:
    """Test suite for AlertManager class."""

    def test_check_for_issues_should_run_all_alert_checks_and_send_alerts(self) -> None:
        """Test that check_for_issues runs all checks and sends alerts for issues found."""
        # given
        with (
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch("monitoring.alert_manager.send_message") as mock_send_message,
        ):
            # Mock status objects
            mock_status_obj = Mock()
            mock_kraken_status.objects = [mock_status_obj]

            alert_manager = AlertManager()

            # Mock one alert to return issues
            mock_issue = ("test_id", "test_data")
            alert_manager.alerts[0].get_issues = Mock(return_value=[mock_issue])
            alert_manager.alerts[0].format_message = Mock(
                return_value="Test alert message"
            )
            alert_manager.alerts[0].get_webhook_url = Mock(
                return_value="http://webhook.url"
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
                alert.get_issues.assert_called_once_with([mock_status_obj])

            # Should send message for the one alert with issues
            mock_send_message.assert_called_once_with(
                "Test alert message", "http://webhook.url"
            )

            # Should mark as not first check
            assert alert_manager.is_first_check is False

    def test_check_for_issues_should_suppress_alerts_on_first_check(self) -> None:
        """Test that alerts are suppressed when is_first_check is True."""
        # given
        with (
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch("monitoring.alert_manager.send_message") as mock_send_message,
            patch("monitoring.alert_manager.logging") as mock_logging,
        ):
            # Mock status objects
            mock_status_obj = Mock()
            mock_kraken_status.objects = [mock_status_obj]

            alert_manager = AlertManager()

            # Mock one alert to return issues
            mock_issue = ("test_id", "test_data")
            alert_with_issue = alert_manager.alerts[0]
            alert_name = alert_with_issue.name
            alert_with_issue.get_issues = Mock(return_value=[mock_issue])
            alert_with_issue.format_message = Mock(return_value="Test alert message")
            alert_with_issue.get_webhook_url = Mock(return_value="http://webhook.url")

            # Keep is_first_check as True (default)
            assert alert_manager.is_first_check is True

            # Mock all other alerts to return no issues
            for alert in alert_manager.alerts[1:]:
                alert.get_issues = Mock(return_value=[])

            # when
            alert_manager.check_for_issues()

            # then
            # Should NOT send message because is_first_check is True
            mock_send_message.assert_not_called()

            # Should log the suppressed alert
            mock_logging.info.assert_any_call(
                f"Suppressed alert for {alert_name}: Test alert message"
            )

            # Should mark as not first check after running
            assert alert_manager.is_first_check is False

            # Should still update last alert time even when suppressed
            alert_id = alert_manager._get_alert_id(alert_name, "test_id")
            assert alert_id in alert_manager.last_alerts

    def test_check_for_issues_should_handle_multiple_alerts_with_issues(self) -> None:
        """Test that multiple alerts with issues are all handled correctly."""
        # given
        with (
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch("monitoring.alert_manager.send_message") as mock_send_message,
        ):
            # Mock status objects
            mock_status_obj = Mock()
            mock_kraken_status.objects = [mock_status_obj]

            alert_manager = AlertManager()

            # Mock three alerts to return issues
            mock_issue_1 = ("id_1", "data_1")
            mock_issue_2 = ("id_2", "data_2")
            mock_issue_3 = ("id_3", "data_3")

            alert_1 = alert_manager.alerts[0]
            alert_1_name = alert_1.name
            alert_1.get_issues = Mock(return_value=[mock_issue_1])
            alert_1.format_message = Mock(return_value="Message 1")
            alert_1.get_webhook_url = Mock(return_value="http://webhook1.url")

            alert_2 = alert_manager.alerts[1]
            alert_2_name = alert_2.name
            alert_2.get_issues = Mock(return_value=[mock_issue_2])
            alert_2.format_message = Mock(return_value="Message 2")
            alert_2.get_webhook_url = Mock(return_value="http://webhook2.url")

            alert_3 = alert_manager.alerts[2]
            alert_3_name = alert_3.name
            alert_3.get_issues = Mock(return_value=[mock_issue_3])
            alert_3.format_message = Mock(return_value="Message 3")
            alert_3.get_webhook_url = Mock(return_value="http://webhook3.url")

            # Set is_first_check to False so alerts are sent
            alert_manager.is_first_check = False

            # Mock remaining alerts to return no issues
            for alert in alert_manager.alerts[3:]:
                alert.get_issues = Mock(return_value=[])

            # when
            alert_manager.check_for_issues()

            # then
            # Should send all three messages
            assert mock_send_message.call_count == 3
            mock_send_message.assert_any_call("Message 1", "http://webhook1.url")
            mock_send_message.assert_any_call("Message 2", "http://webhook2.url")
            mock_send_message.assert_any_call("Message 3", "http://webhook3.url")

            # Should update last alert times for all identifiers
            alert_id_1 = alert_manager._get_alert_id(alert_1_name, "id_1")
            alert_id_2 = alert_manager._get_alert_id(alert_2_name, "id_2")
            alert_id_3 = alert_manager._get_alert_id(alert_3_name, "id_3")

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
            ["file_remover"], alert
        )
        assert should_send_file_remover is False

        # other_instrument should send alert (needs 2 hours, 3 passed)
        should_send_other = alert_manager.should_send_alert(["other_instrument"], alert)
        assert should_send_other is True

    def test_alert_manager_dispatches_queuestopalert_via_isinstance_branch(
        self,
    ) -> None:
        """QueueEndAlert routes through _dispatch_queue_end_dms, not send_message."""
        # given
        with (
            patch("monitoring.alert_manager.KrakenStatus") as mock_kraken_status,
            patch("monitoring.alert_manager.send_message") as mock_send_message,
            patch.object(AlertManager, "_dispatch_queue_end_dms") as mock_dispatch,
        ):
            mock_status_obj = Mock()
            mock_kraken_status.objects = [mock_status_obj]

            alert_manager = AlertManager()
            alert_manager.is_first_check = False

            queue_alert = next(
                a
                for a in alert_manager.alerts
                if a.__class__.__name__ == "QueueEndAlert"
            )
            queue_issue = ("inst1:f1", Mock())
            queue_alert.get_issues = Mock(return_value=[queue_issue])

            for alert in alert_manager.alerts:
                if alert is not queue_alert:
                    alert.get_issues = Mock(return_value=[])

            # when
            alert_manager.check_for_issues()

            # then
            mock_dispatch.assert_called_once_with(queue_alert, [queue_issue])
            mock_send_message.assert_not_called()

    def test_dispatch_queue_end_dms_fans_out_per_recipient_per_issue(self) -> None:
        """Each (issue x recipient) yields a send_dm call; messages aren't bundled."""
        # given
        from monitoring.alerts.queue_stop_alert import QueueEndIssue

        issue1 = QueueEndIssue(
            kind="stall",
            instrument_id="inst1",
            messenger_user_id="U_MASC",
            gradient_length=None,
            pause=None,
            recent_files=[],
        )
        issue2 = QueueEndIssue(
            kind="handoff",
            instrument_id="inst2",
            messenger_user_id="U_JOEB",
            gradient_length=None,
            pause=None,
            recent_files=[],
        )
        mock_alert = Mock()
        mock_alert.render_issue.side_effect = ["msg1", "msg2"]
        mock_alert.get_recipients.side_effect = [
            ["U_MASC", "U_SUP"],
            ["U_JOEB", "U_SUP"],
        ]

        with patch("monitoring.alert_manager.send_dm") as mock_send_dm:
            # when
            AlertManager._dispatch_queue_end_dms(
                mock_alert, [("inst1:f1", issue1), ("inst2:f2", issue2)]
            )

        # then - 4 DMs, separate per recipient per issue
        assert mock_send_dm.call_count == 4
        recipients = [call.args[1] for call in mock_send_dm.call_args_list]
        assert recipients == ["U_MASC", "U_SUP", "U_JOEB", "U_SUP"]

    def test_dispatch_queue_end_dms_continues_after_failed_send(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """One bad recipient must not abort delivery to the rest."""
        # given
        import requests

        from monitoring.alerts.queue_stop_alert import QueueEndIssue

        issue = QueueEndIssue(
            kind="stall",
            instrument_id="inst1",
            messenger_user_id="U_MASC",
            gradient_length=None,
            pause=None,
            recent_files=[],
        )
        mock_alert = Mock()
        mock_alert.render_issue.return_value = "msg"
        mock_alert.get_recipients.return_value = ["U_MASC", "U_SUP"]

        def _side_effect(_msg: str, recipient: str, **_kw: object) -> None:
            if recipient == "U_MASC":
                raise requests.HTTPError("500")

        with (
            patch(
                "monitoring.alert_manager.send_dm", side_effect=_side_effect
            ) as mock_send_dm,
            caplog.at_level("WARNING"),
        ):
            # when
            AlertManager._dispatch_queue_end_dms(mock_alert, [("inst1:f1", issue)])

        # then
        assert mock_send_dm.call_count == 2
        assert any(
            "U_MASC" in rec.message
            and "stall" in rec.message
            and "inst1" in rec.message
            for rec in caplog.records
        )


class TestSendSpecialAlert:
    """Test suite for send_special_alert function."""

    def test_send_special_alert_should_send_message_when_cooldown_expired(self) -> None:
        """Test that send_special_alert sends message when cooldown has expired."""
        # given
        with patch("monitoring.alert_manager.send_message") as mock_send_message:
            alert_manager = AlertManager()
            identifier = "test_identifier"
            alert_name = "test_alert"
            message = "Test message"

            # when
            send_special_alert(identifier, alert_name, message, alert_manager)

            # then
            expected_message = f"{message} [{alert_name} {identifier}]"
            mock_send_message.assert_called_once()
            assert mock_send_message.call_args[0][0] == expected_message
