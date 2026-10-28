"""Unit tests for the messenger facade."""

from unittest.mock import Mock, call, patch

import apprise
import pytest
import requests
from apprise import NotifyType

from monitoring.messenger import AlertTypes, Message, _notify, _send_slack_text, send

_USER_URL = "slack://xoxb-test/@{{ALPHAKRAKEN_MESSENGER_ID}}"


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


class TestSendSlackText:
    """The slacktext service posts top-level text through the Slack bot API."""

    @patch("monitoring.messenger.requests.post")
    def test_posts_emoji_title_and_body_to_chat_postmessage(
        self, mock_post: Mock
    ) -> None:
        """The host is the bearer token, the path the channel; text has emoji, bold title and body."""
        # given
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {"ok": True}

        # when
        result = _send_slack_text(
            "body", "Title", "failure", {"host": "xoxb-1-a", "fullpath": "/C0123"}
        )

        # then
        assert result is True
        mock_post.assert_called_once_with(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": "Bearer xoxb-1-a",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"channel": "C0123", "text": "🚨 *Title*\nbody"},
            timeout=10,
        )

    @patch("monitoring.messenger.requests.post")
    def test_info_without_title(self, mock_post: Mock) -> None:
        """Without a title only the emoji precedes the body."""
        # given
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {"ok": True}

        # when
        _send_slack_text("body", "", "info", {"host": "xoxb-1", "fullpath": "/C0123"})

        # then
        assert mock_post.call_args.kwargs["json"]["text"] == "ℹ️ body"  # noqa: RUF001

    @pytest.mark.parametrize(
        ("fullpath", "channel"),
        [("/@U0123", "U0123"), ("/%23ops", "ops"), ("/ops", "ops")],
    )
    @patch("monitoring.messenger.requests.post")
    def test_user_and_channel_name_targets(
        self, mock_post: Mock, fullpath: str, channel: str
    ) -> None:
        """A user id loses its `@`, a channel name its percent-encoded `#`."""
        # given
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {"ok": True}

        # when
        _send_slack_text("b", "", "failure", {"host": "xoxb-1", "fullpath": fullpath})

        # then
        assert mock_post.call_args.kwargs["json"]["channel"] == channel

    @patch("monitoring.messenger.requests.post")
    def test_http_error_is_a_failed_send(self, mock_post: Mock) -> None:
        """A non-2xx response returns False."""
        # given
        mock_post.return_value.ok = False
        mock_post.return_value.status_code = 500

        # when / then
        assert (
            _send_slack_text("b", "", "failure", {"host": "xoxb-1", "fullpath": "/C"})
            is False
        )

    @patch("monitoring.messenger.requests.post")
    def test_api_error_is_logged_as_failed_send(
        self, mock_post: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Slack answers API errors with HTTP 200 and ok=false; target, status and body are logged."""
        # given
        mock_post.return_value.ok = True
        mock_post.return_value.status_code = 200
        mock_post.return_value.text = '{"ok":false,"error":"channel_not_found"}'
        mock_post.return_value.json.return_value = {"ok": False}

        # when
        with caplog.at_level("ERROR"):
            result = _send_slack_text(
                "b", "", "failure", {"host": "xoxb-1", "fullpath": "/@U"}
            )

        # then
        assert result is False
        assert any(
            "to U: HTTP 200" in rec.message and "channel_not_found" in rec.message
            for rec in caplog.records
        )

    @patch("monitoring.messenger.requests.post")
    def test_non_json_response_is_a_failed_send(
        self, mock_post: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A 200 without a JSON body is logged as a rejection, not raised."""
        # given
        mock_post.return_value.ok = True
        mock_post.return_value.status_code = 200
        mock_post.return_value.text = "<html>proxy</html>"
        mock_post.return_value.json.side_effect = requests.JSONDecodeError("x", "", 0)

        # when
        with caplog.at_level("ERROR"):
            result = _send_slack_text(
                "b", "", "failure", {"host": "xoxb-1", "fullpath": "/@U"}
            )

        # then
        assert result is False
        assert any("<html>proxy</html>" in rec.message for rec in caplog.records)

    @patch("monitoring.messenger.requests.post", side_effect=requests.ConnectionError)
    def test_request_error_is_logged_as_failed_send(
        self, mock_post: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A failed connection is logged with its traceback and returns False."""
        # when
        with caplog.at_level("ERROR"):
            result = _send_slack_text(
                "b", "", "failure", {"host": "xoxb-1", "fullpath": "/C"}
            )

        # then
        assert result is False
        mock_post.assert_called_once()
        assert any(
            "Slack request to C failed" in rec.message and rec.exc_info
            for rec in caplog.records
        )

    @patch("monitoring.messenger.requests.post")
    def test_is_registered_with_apprise(self, mock_post: Mock) -> None:
        """A slacktext:// URL goes through the custom service, with the footer argument ignored."""
        # given
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = {"ok": True}

        # when
        notifier = apprise.Apprise()
        assert notifier.add("slacktext://xoxb-1-a/C0123?footer=no")
        result = bool(
            notifier.notify(body="body", title="", notify_type=NotifyType.FAILURE)
        )

        # then
        assert result is True
        assert (
            mock_post.call_args.kwargs["headers"]["Authorization"] == "Bearer xoxb-1-a"
        )
        assert mock_post.call_args.kwargs["json"]["channel"] == "C0123"
