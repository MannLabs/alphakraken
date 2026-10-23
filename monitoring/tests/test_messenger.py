"""Unit tests for the messenger facade."""

from unittest.mock import Mock, call, patch

import pytest
import requests

from monitoring.messenger import (
    Message,
    PermanentSendError,
    _send_direct_message,
    _send_direct_message_slack,
    send,
)


class TestSend:
    """send fans out to channels and recipients and reports failures."""

    @patch("monitoring.messenger._send_direct_message")
    @patch("monitoring.messenger._send_channel_message")
    @patch(
        "monitoring.messenger._WEBHOOK_URLS",
        {"ops": "http://ops", "business": "http://ops"},
    )
    def test_sends_once_per_distinct_webhook_and_once_per_recipient(
        self, mock_channel: Mock, mock_direct: Mock
    ) -> None:
        """Destinations sharing a webhook URL get one message; each recipient gets one."""
        # when
        result = send(Message("m"), {"ops", "business"}, ["U_1", "U_2"])

        # then
        assert result is True
        mock_channel.assert_called_once_with(Message("m"), "http://ops")
        assert mock_direct.call_args_list == [
            call(Message("m"), "U_1"),
            call(Message("m"), "U_2"),
        ]

    @patch("monitoring.messenger._send_direct_message", side_effect=RuntimeError)
    @patch("monitoring.messenger._send_channel_message")
    @patch("monitoring.messenger._WEBHOOK_URLS", {"ops": "http://ops"})
    def test_failed_send_does_not_abort_the_rest(
        self, mock_channel: Mock, mock_direct: Mock
    ) -> None:
        """One failure is reported, the other sends still happen."""
        # when
        result = send(Message("m"), {"ops"}, ["U_1"])

        # then
        assert result is False
        mock_channel.assert_called_once()
        mock_direct.assert_called_once()

    @patch(
        "monitoring.messenger._send_direct_message",
        side_effect=PermanentSendError("user_not_found"),
    )
    def test_permanent_failure_counts_as_done(
        self, mock_direct: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A send that can never succeed is logged and not reported for a retry."""
        # when
        with caplog.at_level("WARNING"):
            result = send(Message("m"), recipients=["U_STALE"])

        # then
        assert result is True
        mock_direct.assert_called_once()
        assert any("Not retrying" in rec.message for rec in caplog.records)

    @patch("monitoring.messenger._send_channel_message")
    def test_nothing_to_send_succeeds(self, mock_channel: Mock) -> None:
        """Without destinations and recipients nothing is sent."""
        assert send(Message("m")) is True
        mock_channel.assert_not_called()


class TestSendDirectMessage:
    """_send_direct_message dispatches by configured credentials."""

    @patch("monitoring.messenger._send_direct_message_slack")
    @patch("alerts.config.SLACK_BOT_TOKEN", "xoxb-test")
    def test_with_slack_token_configured_calls_slack(self, mock_slack: Mock) -> None:
        """When Slack token is configured, the Slack implementation is called."""
        # given / when
        _send_direct_message(Message("hello"), "U_123")
        # then
        mock_slack.assert_called_once_with(Message("hello"), "U_123", "xoxb-test")

    @patch("monitoring.messenger._send_direct_message_slack")
    @patch("alerts.config.SLACK_BOT_TOKEN", "")
    def test_with_no_credentials_logs_and_returns(
        self,
        mock_slack: Mock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When no DM creds are configured, a warning is logged and nothing raised."""
        # given / when
        with caplog.at_level("WARNING"):
            _send_direct_message(Message("hello"), "U_123")
        # then - no platform call; warning logged
        mock_slack.assert_not_called()
        assert any(
            "No DM credentials" in rec.message and "U_123" in rec.message
            for rec in caplog.records
        )


class TestSendSlackDm:
    """Direct tests for _send_direct_message_slack: endpoint, auth, payload, ok-handling."""

    @patch("monitoring.messenger.requests.post")
    @patch("monitoring.messenger.os.environ.get", return_value="sandbox")
    def test_posts_to_chat_postmessage_with_bearer_and_payload(
        self,
        _mock_env: Mock,  # noqa: PT019
        mock_post: Mock,
    ) -> None:
        """POST goes to chat.postMessage with Bearer auth and prefixed text."""
        # given
        mock_resp = Mock()
        mock_resp.json.return_value = {"ok": True}
        mock_post.return_value = mock_resp
        # when
        _send_direct_message_slack(Message("hi"), "U_123", "xoxb-test")
        # then
        mock_post.assert_called_once()
        url = mock_post.call_args.args[0]
        kwargs = mock_post.call_args.kwargs
        assert url == "https://slack.com/api/chat.postMessage"
        assert kwargs["headers"]["Authorization"] == "Bearer xoxb-test"
        assert kwargs["json"]["channel"] == "U_123"
        # alert + non-production → env prefix and emoji
        assert kwargs["json"]["text"].startswith("🚨 [sandbox] hi")

    @patch("monitoring.messenger.requests.post")
    @patch("monitoring.messenger.os.environ.get", return_value="production")
    def test_production_strips_env_prefix(
        self,
        _mock_env: Mock,  # noqa: PT019
        mock_post: Mock,
    ) -> None:
        """In production env, the [<env>] prefix is omitted."""
        # given
        mock_resp = Mock()
        mock_resp.json.return_value = {"ok": True}
        mock_post.return_value = mock_resp
        # when
        _send_direct_message_slack(Message("hi"), "U_123", "xoxb-test")
        # then
        text = mock_post.call_args.kwargs["json"]["text"]
        assert text == "🚨 hi"

    @patch("monitoring.messenger.requests.post")
    def test_raises_on_ok_false(self, mock_post: Mock) -> None:
        """Slack returns 200 + `ok: false` for API errors — must raise."""
        # given
        mock_resp = Mock()
        mock_resp.json.return_value = {"ok": False, "error": "ratelimited"}
        mock_post.return_value = mock_resp
        # when / then
        with pytest.raises(RuntimeError, match="ratelimited"):
            _send_direct_message_slack(Message("hi"), "U_123", "xoxb-test")

    @patch("monitoring.messenger.requests.post")
    def test_raises_permanent_error_for_unknown_user(self, mock_post: Mock) -> None:
        """A stale user id is a permanent failure."""
        # given
        mock_resp = Mock()
        mock_resp.json.return_value = {"ok": False, "error": "user_not_found"}
        mock_post.return_value = mock_resp
        # when / then
        with pytest.raises(PermanentSendError, match="user_not_found"):
            _send_direct_message_slack(Message("hi"), "U_123", "xoxb-test")

    @patch("monitoring.messenger.requests.post")
    def test_raises_on_http_non_2xx(self, mock_post: Mock) -> None:
        """HTTP non-2xx responses propagate via raise_for_status."""
        # given - raise_for_status raises
        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("500")
        mock_post.return_value = mock_resp
        # when / then
        with pytest.raises(requests.HTTPError):
            _send_direct_message_slack(Message("hi"), "U_123", "xoxb-test")
