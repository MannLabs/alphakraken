"""Unit tests for the messenger facade."""

from unittest.mock import Mock, call, patch

import pytest
from apprise import NotifyType

from monitoring.messenger import AlertTypes, Message, _notify, send

_USER_URL = "slack://xoxb-test/@{messenger_id}"


class TestSend:
    """send fans out to channels and recipients and reports failures."""

    @patch("monitoring.messenger._notify", return_value=True)
    @patch("alerts.config.USER_ALERTS_URL", _USER_URL)
    @patch(
        "monitoring.messenger._CHANNEL_URLS",
        {"ops": "http://ops", "business": "http://ops"},
    )
    def test_sends_once_per_distinct_url_and_once_per_recipient(
        self, mock_notify: Mock
    ) -> None:
        """Destinations sharing a URL get one message; each recipient gets one at their URL."""
        # when
        result = send(Message("m"), {"ops", "business"}, ["U_1", "U_2"])

        # then
        assert result is True
        assert mock_notify.call_args_list == [
            call("http://ops", Message("m"), "channel message"),
            call("slack://xoxb-test/@U_1", Message("m"), "direct message to U_1"),
            call("slack://xoxb-test/@U_2", Message("m"), "direct message to U_2"),
        ]

    @patch("monitoring.messenger._notify", side_effect=[False, True])
    @patch("alerts.config.USER_ALERTS_URL", _USER_URL)
    @patch("monitoring.messenger._CHANNEL_URLS", {"ops": "http://ops"})
    def test_failed_send_does_not_abort_the_rest(self, mock_notify: Mock) -> None:
        """One failure is reported, the other sends still happen."""
        # when
        result = send(Message("m"), {"ops"}, ["U_1"])

        # then
        assert result is False
        assert mock_notify.call_count == 2

    @patch("monitoring.messenger._notify")
    @patch("alerts.config.USER_ALERTS_URL", "")
    def test_without_user_alerts_url_drops_recipients(
        self, mock_notify: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Recipients are dropped with a warning, which counts as sent."""
        # when
        with caplog.at_level("WARNING"):
            result = send(Message("m"), recipients=["U_1"])

        # then
        assert result is True
        mock_notify.assert_not_called()
        assert any("U_1" in rec.message for rec in caplog.records)

    @patch("monitoring.messenger._notify")
    def test_nothing_to_send_succeeds(self, mock_notify: Mock) -> None:
        """Without destinations and recipients nothing is sent."""
        assert send(Message("m")) is True
        mock_notify.assert_not_called()


@patch.dict("os.environ", {"ENV_NAME": "sandbox", "KRAKEN_HOSTNAME": "host1"})
class TestNotify:
    """_notify hands the message to apprise and reports the result."""

    @patch("monitoring.messenger.apprise.Apprise")
    def test_sends_prefixed_alert(self, mock_apprise_cls: Mock) -> None:
        """An alert outside production carries the env prefix and the sender."""
        # given
        notifier = mock_apprise_cls.return_value
        notifier.add.return_value = True
        notifier.notify.return_value = True

        # when
        result = _notify("slack://x", Message("hi", "T"), "channel message")

        # then
        assert result is True
        notifier.add.assert_called_once_with("slack://x?footer=no")
        notifier.notify.assert_called_once_with(
            title="T",
            body="[sandbox] hi\n(sent from host1)",
            notify_type=NotifyType.FAILURE,
        )

    @patch("monitoring.messenger.apprise.Apprise")
    def test_info_has_no_prefix(self, mock_apprise_cls: Mock) -> None:
        """Info messages get no env prefix and an empty title."""
        # given
        notifier = mock_apprise_cls.return_value
        notifier.add.return_value = True
        notifier.notify.return_value = True

        # when
        _notify("slack://x", Message("hi", type=AlertTypes.INFO), "channel message")

        # then
        notifier.notify.assert_called_once_with(
            title="", body="hi\n(sent from host1)", notify_type=NotifyType.INFO
        )

    @patch.dict("os.environ", {"ENV_NAME": "production"})
    @patch("monitoring.messenger.apprise.Apprise")
    def test_production_alert_has_no_prefix(self, mock_apprise_cls: Mock) -> None:
        """In production the env prefix is omitted."""
        # given
        notifier = mock_apprise_cls.return_value
        notifier.add.return_value = True
        notifier.notify.return_value = True

        # when
        _notify("slack://x", Message("hi"), "channel message")

        # then
        assert notifier.notify.call_args.kwargs["body"] == "hi\n(sent from host1)"

    @patch("monitoring.messenger.apprise.Apprise")
    def test_footer_arg_is_appended_to_existing_query(
        self, mock_apprise_cls: Mock
    ) -> None:
        """A URL with a query string keeps it."""
        # given
        notifier = mock_apprise_cls.return_value

        # when
        _notify("https://h/x?sig=1", Message("hi"), "channel message")

        # then
        notifier.add.assert_called_once_with("https://h/x?sig=1&footer=no")

    @patch("monitoring.messenger.apprise.Apprise")
    def test_invalid_url_fails_without_sending(
        self, mock_apprise_cls: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A URL apprise cannot parse is reported, without the URL in the log."""
        # given
        notifier = mock_apprise_cls.return_value
        notifier.add.return_value = False

        # when
        with caplog.at_level("ERROR"):
            result = _notify("nonsense://secret", Message("hi"), "channel message")

        # then
        assert result is False
        notifier.notify.assert_not_called()
        assert any("Invalid notification URL" in rec.message for rec in caplog.records)
        assert all("secret" not in rec.message for rec in caplog.records)

    @patch("monitoring.messenger.apprise.Apprise")
    def test_failed_delivery_is_reported(
        self, mock_apprise_cls: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A falsy apprise result is a failed send."""
        # given
        notifier = mock_apprise_cls.return_value
        notifier.add.return_value = True
        notifier.notify.return_value = False

        # when
        with caplog.at_level("ERROR"):
            result = _notify("slack://x", Message("hi"), "direct message to U_1")

        # then
        assert result is False
        assert any("direct message to U_1" in rec.message for rec in caplog.records)
