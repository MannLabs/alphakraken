"""Unit tests for AlertManager."""

from collections.abc import Generator
from datetime import datetime, timedelta
from unittest.mock import Mock, call, patch

import pytest
import pytz

# alert_manager imports `messenger_clients` via the monitoring dir on sys.path, so
# `monitoring.messenger_clients.Message` would be a different class and never compare equal
from alerts import config
from messenger_clients import Message

from monitoring.alert_manager import AlertManager, _dispatch_queue_stop_dms, _is_due
from monitoring.alerts.base_alert import Issue

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=pytz.UTC)


@pytest.mark.parametrize(
    ("last_notified", "cooldown_minutes", "expected"),
    [
        (None, 120, True),
        (None, None, True),
        (NOW - timedelta(minutes=121), 120, True),
        (NOW - timedelta(minutes=119), 120, False),
        (NOW - timedelta(days=30), None, False),
        # naive timestamps from the DB are UTC
        ((NOW - timedelta(minutes=121)).replace(tzinfo=None), 120, True),
    ],
)
def test_is_due(
    last_notified: datetime | None, cooldown_minutes: int | None, *, expected: bool
) -> None:
    """An issue is due if never notified or the cooldown has passed; None never repeats."""
    # given
    record = Mock(last_notified=last_notified)

    # when / then
    assert _is_due(record, cooldown_minutes, NOW) is expected


def _alert(name: str = "TestAlert", issues: list[Issue] | None = None) -> Mock:
    alert = Mock()
    alert.name = name
    alert.title = f"{name} title"
    alert.get_issues.return_value = issues or []
    alert.format_message.side_effect = lambda issues: ",".join(
        i.identifier for i in issues
    )
    alert.get_webhook_url.return_value = "http://webhook.url"
    alert.get_cooldown_minutes.return_value = 120
    return alert


def _record(alert: str, key: str, last_notified: datetime | None = None) -> Mock:
    return Mock(alert=alert, key=key, last_notified=last_notified)


@pytest.fixture
def db() -> Generator[dict[str, Mock], None, None]:
    """Patch the DB access of the manager."""
    with (
        patch("monitoring.alert_manager.KrakenStatus") as kraken_status,
        patch("monitoring.alert_manager.get_open_alert_issues") as get_open,
        patch("monitoring.alert_manager.add_alert_issue") as add,
        patch("monitoring.alert_manager.set_alert_issue_notified") as set_notified,
        patch("monitoring.alert_manager.resolve_alert_issues") as resolve,
    ):
        kraken_status.objects = []
        get_open.return_value = []
        add.side_effect = lambda **kwargs: _record(kwargs["alert"], kwargs["key"])
        yield {
            "get_open": get_open,
            "add": add,
            "set_notified": set_notified,
            "resolve": resolve,
        }


class TestCheckForIssues:
    """check_for_issues opens, notifies and resolves records."""

    @patch("monitoring.alert_manager.send_channel_message")
    def test_new_issue_is_recorded_and_notified(
        self, mock_send: Mock, db: dict[str, Mock]
    ) -> None:
        """A new issue gets a record and is sent at once."""
        # given
        issue = Issue("inst1", 150, instrument_id="inst1", key="inst1:k")
        manager = AlertManager()
        manager.alerts = [_alert(issues=[issue])]

        # when
        manager.check_for_issues(NOW)

        # then
        db["add"].assert_called_once_with(
            alert="TestAlert",
            key="inst1:k",
            identifier="inst1",
            instrument_id="inst1",
            relevant_file=None,
            details="150",
            first_seen=NOW,
        )
        mock_send.assert_called_once_with(
            Message("inst1", "TestAlert title"), "http://webhook.url"
        )
        notified_record, notified_at = db["set_notified"].call_args.args
        assert (notified_record.alert, notified_record.key) == ("TestAlert", "inst1:k")
        assert notified_at == NOW
        db["resolve"].assert_called_once_with([], NOW)

    @patch("monitoring.alert_manager.send_channel_message")
    def test_only_due_issues_are_sent(
        self, mock_send: Mock, db: dict[str, Mock]
    ) -> None:
        """Issues in cooldown are left out of the message and keep their timestamp."""
        # given
        due = _record("TestAlert", "a", NOW - timedelta(minutes=121))
        in_cooldown = _record("TestAlert", "b", NOW - timedelta(minutes=10))
        db["get_open"].return_value = [due, in_cooldown]
        manager = AlertManager()
        manager.alerts = [_alert(issues=[Issue("a", "details"), Issue("b", "details")])]

        # when
        manager.check_for_issues(NOW)

        # then
        db["add"].assert_not_called()
        mock_send.assert_called_once_with(
            Message("a", "TestAlert title"), "http://webhook.url"
        )
        db["set_notified"].assert_called_once_with(due, NOW)

    @patch("monitoring.alert_manager.send_channel_message")
    def test_no_message_when_nothing_is_due(
        self, mock_send: Mock, db: dict[str, Mock]
    ) -> None:
        """Nothing is sent if all issues are in cooldown."""
        # given
        db["get_open"].return_value = [_record("TestAlert", "a", NOW)]
        manager = AlertManager()
        manager.alerts = [_alert(issues=[Issue("a", "details")])]

        # when
        manager.check_for_issues(NOW)

        # then
        mock_send.assert_not_called()
        db["set_notified"].assert_not_called()

    @patch("monitoring.alert_manager.send_channel_message", side_effect=RuntimeError)
    def test_failed_send_is_not_recorded(
        self,
        mock_send: Mock,  # noqa: ARG002
        db: dict[str, Mock],
    ) -> None:
        """A failed send leaves the record untouched, so it is retried next check."""
        # given
        manager = AlertManager()
        manager.alerts = [_alert(issues=[Issue("a", "details")])]

        # when
        manager.check_for_issues(NOW)

        # then
        db["add"].assert_called_once()
        db["set_notified"].assert_not_called()

    @patch("monitoring.alert_manager.send_channel_message")
    def test_undetected_records_are_resolved(
        self,
        mock_send: Mock,  # noqa: ARG002
        db: dict[str, Mock],
    ) -> None:
        """Open records of issues that are no longer detected are resolved."""
        # given
        gone = _record("TestAlert", "gone", NOW)
        still_there = _record("TestAlert", "a", NOW)
        db["get_open"].return_value = [gone, still_there]
        manager = AlertManager()
        manager.alerts = [_alert(issues=[Issue("a", "details")])]

        # when
        manager.check_for_issues(NOW)

        # then
        db["resolve"].assert_called_once_with([gone], NOW)

    @patch("monitoring.alert_manager.send_channel_message")
    def test_alerts_use_their_own_cooldown(
        self,
        mock_send: Mock,  # noqa: ARG002
        db: dict[str, Mock],
    ) -> None:
        """The cooldown is asked from the alert per issue."""
        # given
        record = _record("TestAlert", "a", NOW - timedelta(minutes=5))
        db["get_open"].return_value = [record]
        alert = _alert(issues=[Issue("a", "details")])
        alert.get_cooldown_minutes.return_value = 1
        manager = AlertManager()
        manager.alerts = [alert]

        # when
        manager.check_for_issues(NOW)

        # then
        db["set_notified"].assert_called_once_with(record, NOW)

    @patch("monitoring.alert_manager.send_direct_message")
    @patch("monitoring.alert_manager.send_channel_message")
    def test_queue_stop_alert_is_sent_as_direct_messages(
        self, mock_send_channel: Mock, mock_send_direct: Mock, db: dict[str, Mock]
    ) -> None:
        """QueueStopAlert issues go to users, not to a channel."""
        # given
        manager = AlertManager()
        queue_alert = next(a for a in manager.alerts if a.name == "QueueStopAlert")
        details = Mock(kind="stop")
        queue_alert.get_issues = Mock(return_value=[Issue("inst1:f1", details)])
        queue_alert.get_recipients = Mock(return_value=["U_1"])
        queue_alert.render_issue = Mock(return_value="msg")
        queue_alert.refresh_recent_files = Mock()
        for alert in manager.alerts:
            if alert is not queue_alert:
                alert.get_issues = Mock(return_value=[])

        # when
        manager.check_for_issues(NOW)

        # then
        mock_send_direct.assert_called_once_with(Message("msg"), "U_1")
        mock_send_channel.assert_not_called()
        db["set_notified"].assert_called_once()


class TestDispatchQueueStopDms:
    """Direct messages are sent per issue and recipient."""

    @patch("monitoring.alert_manager.send_direct_message")
    def test_fans_out_per_recipient_per_issue(self, mock_send: Mock) -> None:
        """Each (issue x recipient) yields one send; files are refreshed before rendering."""
        # given
        alert = Mock()
        alert.render_issue.side_effect = ["msg1", "msg2"]
        alert.get_recipients.side_effect = [["U_A", "U_SUP"], ["U_B", "U_SUP"]]
        details1, details2 = Mock(), Mock()

        # when
        result = _dispatch_queue_stop_dms(
            alert, [Issue("inst1:f1", details1), Issue("inst2:f2", details2)]
        )

        # then
        assert result is True
        assert [c.args[1] for c in mock_send.call_args_list] == [
            "U_A",
            "U_SUP",
            "U_B",
            "U_SUP",
        ]
        assert alert.refresh_recent_files.call_args_list == [
            call(details1),
            call(details2),
        ]

    @patch("monitoring.alert_manager.send_direct_message")
    def test_continues_after_failed_send_and_reports_failure(
        self, mock_send: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """One bad recipient does not abort the rest, but the result is a failure."""
        # given
        alert = Mock()
        alert.render_issue.return_value = "msg"
        alert.get_recipients.return_value = ["U_BAD", "U_OK"]
        mock_send.side_effect = lambda _m, recipient: (
            (_ for _ in ()).throw(RuntimeError("500")) if recipient == "U_BAD" else None
        )

        # when
        with caplog.at_level("WARNING"):
            result = _dispatch_queue_stop_dms(
                alert, [Issue("inst1:f1", Mock(kind="stop"))]
            )

        # then
        assert result is False
        assert mock_send.call_count == 2
        assert any("U_BAD" in rec.message for rec in caplog.records)


class TestSendOnce:
    """send_once sends to the ops channel with an in-memory cooldown."""

    @patch("monitoring.alert_manager.send_channel_message")
    def test_sends_with_key_suffix_and_respects_cooldown(self, mock_send: Mock) -> None:
        """The key is appended to the body; repeats within the cooldown are dropped."""
        # given
        manager = AlertManager()
        message = Message("db down")

        # when
        manager.send_once("db", message, 10, NOW)
        manager.send_once("db", message, 10, NOW + timedelta(minutes=9))
        manager.send_once("db", message, 10, NOW + timedelta(minutes=11))

        # then
        assert mock_send.call_args_list == [
            call(Message("db down [db]"), config.OPS_ALERTS_WEBHOOK_URL),
            call(Message("db down [db]"), config.OPS_ALERTS_WEBHOOK_URL),
        ]

    @patch("monitoring.alert_manager.send_channel_message")
    def test_cooldown_none_sends_only_once(self, mock_send: Mock) -> None:
        """Without cooldown a key is sent once per process."""
        # given
        manager = AlertManager()

        # when
        manager.send_once("k", Message("m"), None, NOW)
        manager.send_once("k", Message("m"), None, NOW + timedelta(days=1))

        # then
        mock_send.assert_called_once()

    @patch("monitoring.alert_manager.send_channel_message", side_effect=RuntimeError)
    def test_failed_send_is_retried(self, mock_send: Mock) -> None:
        """A failed send does not start the cooldown."""
        # given
        manager = AlertManager()

        # when
        manager.send_once("k", Message("m"), 10, NOW)
        manager.send_once("k", Message("m"), 10, NOW)

        # then
        assert mock_send.call_count == 2


class TestCheckDbFreeAlerts:
    """DB-free alerts are sent via send_once."""

    def test_issues_are_sent_once_per_alert_and_key(self) -> None:
        """Each issue of a DB-free alert is sent with its own cooldown key."""
        # given
        manager = AlertManager()
        alert = _alert("LocalDiskSpaceAlert", [Issue("local", 5)])
        manager.db_free_alerts = [alert]

        # when
        with patch.object(manager, "send_once") as mock_send_once:
            manager.check_db_free_alerts(NOW)

        # then
        alert.get_issues.assert_called_once_with([], NOW)
        mock_send_once.assert_called_once_with(
            "LocalDiskSpaceAlert:local",
            Message("local", "LocalDiskSpaceAlert title"),
            120,
            NOW,
        )
