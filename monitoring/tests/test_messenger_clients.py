"""Unit tests for messenger_clients DM transport (send_dm, _send_slack_dm, _send_msteams_dm)."""

from unittest.mock import Mock, patch

import pytest
import requests

from monitoring.messenger_clients import (
    AlertTypes,
    _send_msteams_dm,
    _send_slack_dm,
    send_dm,
)


class TestSendDm:
    """Top-level send_dm dispatches by configured credentials."""

    @patch("monitoring.messenger_clients._send_slack_dm")
    @patch("alerts.config.SLACK_BOT_TOKEN", "xoxb-test")
    def test_send_dm_with_slack_token_configured_calls_send_slack_dm(
        self, mock_slack: Mock
    ) -> None:
        """When Slack token is configured, send_dm delegates to _send_slack_dm."""
        # given / when
        send_dm("hello", "U_123")
        # then
        mock_slack.assert_called_once_with(
            "hello", "U_123", "xoxb-test", AlertTypes.ALERT
        )

    @patch("monitoring.messenger_clients._send_slack_dm")
    @patch("alerts.config.SLACK_BOT_TOKEN", "")
    def test_send_dm_with_no_credentials_logs_and_returns(
        self,
        mock_slack: Mock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When no DM creds are configured, send_dm logs a warning and does not raise."""
        # given / when
        with caplog.at_level("WARNING"):
            send_dm("hello", "U_123")
        # then - no platform call; warning logged
        mock_slack.assert_not_called()
        assert any(
            "No DM credentials" in rec.message and "U_123" in rec.message
            for rec in caplog.records
        )


class TestSendSlackDm:
    """Direct tests for _send_slack_dm: endpoint, auth, payload, ok-handling."""

    @patch("monitoring.messenger_clients.requests.post")
    @patch("monitoring.messenger_clients.os.environ.get", return_value="sandbox")
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
        _send_slack_dm("hi", "U_123", "xoxb-test")
        # then
        mock_post.assert_called_once()
        url = mock_post.call_args.args[0]
        kwargs = mock_post.call_args.kwargs
        assert url == "https://slack.com/api/chat.postMessage"
        assert kwargs["headers"]["Authorization"] == "Bearer xoxb-test"
        assert kwargs["json"]["channel"] == "U_123"
        # alert + non-production → env prefix and emoji
        assert kwargs["json"]["text"].startswith("🚨 [sandbox] hi")

    @patch("monitoring.messenger_clients.requests.post")
    @patch("monitoring.messenger_clients.os.environ.get", return_value="production")
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
        _send_slack_dm("hi", "U_123", "xoxb-test")
        # then
        text = mock_post.call_args.kwargs["json"]["text"]
        assert text == "🚨 hi"

    @patch("monitoring.messenger_clients.requests.post")
    def test_raises_on_ok_false(self, mock_post: Mock) -> None:
        """Slack returns 200 + `ok: false` for API errors — must raise."""
        # given
        mock_resp = Mock()
        mock_resp.json.return_value = {"ok": False, "error": "channel_not_found"}
        mock_post.return_value = mock_resp
        # when / then
        with pytest.raises(RuntimeError, match="channel_not_found"):
            _send_slack_dm("hi", "U_123", "xoxb-test")

    @patch("monitoring.messenger_clients.requests.post")
    def test_raises_on_http_non_2xx(self, mock_post: Mock) -> None:
        """HTTP non-2xx responses propagate via raise_for_status."""
        # given - raise_for_status raises
        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("500")
        mock_post.return_value = mock_resp
        # when / then
        with pytest.raises(requests.HTTPError):
            _send_slack_dm("hi", "U_123", "xoxb-test")


class TestSendMsteamsDmStub:
    """The MS Teams DM stub must raise NotImplementedError."""

    def test_raises_not_implemented_error(self) -> None:
        """The MS Teams DM stub is not yet implemented and must raise."""
        with pytest.raises(NotImplementedError, match="MS Teams"):
            _send_msteams_dm("hi", "U_123", "token")
