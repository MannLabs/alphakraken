"""Unit tests for AlertManager."""

from collections.abc import Generator
from datetime import datetime, timedelta
from unittest.mock import Mock, call, patch

import pytest
import pytz

# alert_manager imports `messenger` via the monitoring dir on sys.path, so
# `monitoring.messenger.Message` would be a different class and never compare equal
from messenger import Message

from monitoring.alert_manager import AlertManager, _dispatch, _is_due
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
    alert.get_cooldown_minutes.return_value = 120
    alert.destinations = frozenset({"ops"})
    return alert


def _record(alert: str, key: str, last_notified: datetime | None = None) -> Mock:
    return Mock(alert=alert, key=key, last_notified=last_notified)


@pytest.fixture
def db() -> Generator[dict[str, Mock], None, None]:
    """Patch the DB access of the manager."""
    with (
        patch("monitoring.alert_manager.KrakenStatus") as kraken_status,
        patch("monitoring.alert_manager.get_alert_issues") as get_open,
        patch("monitoring.alert_manager.add_alert_issue") as add,
        patch("monitoring.alert_manager.set_alert_issue_notified") as set_notified,
        patch("monitoring.alert_manager.get_messenger_ids_by_initials") as ids,
        patch("monitoring.alert_manager.get_active_alert_suppressions") as supp,
    ):
        kraken_status.objects = []
        ids.return_value = {"MaSc": "U_MASC"}
        supp.return_value = []
        get_open.return_value = []
        add.side_effect = lambda **kwargs: _record(kwargs["alert"], kwargs["key"])
        yield {
            "get_open": get_open,
            "add": add,
            "set_notified": set_notified,
            "suppressions": supp,
        }


class TestCheckForIssues:
    """check_for_issues opens, notifies and resolves records."""

    @patch("monitoring.alert_manager.send", return_value=True)
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
        mock_send.assert_called_once_with(Message("inst1", "TestAlert title"), {"ops"})
        notified_record, notified_at = db["set_notified"].call_args.args
        assert (notified_record.alert, notified_record.key) == ("TestAlert", "inst1:k")
        assert notified_at == NOW

    @patch("monitoring.alert_manager.send", return_value=True)
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
        mock_send.assert_called_once_with(Message("a", "TestAlert title"), {"ops"})
        db["set_notified"].assert_called_once_with(due, NOW)

    @patch("monitoring.alert_manager.send", return_value=True)
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

    @patch("monitoring.alert_manager.send", return_value=False)
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

    @patch("monitoring.alert_manager.send", return_value=True)
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

    @patch("monitoring.alert_manager.send", side_effect=[False, True, True])
    def test_a_failed_user_send_is_retried_without_resending_the_others(
        self, mock_send: Mock, db: dict[str, Mock]
    ) -> None:
        """Only the issue whose send failed is sent again next check."""
        # given
        issues = [
            Issue("a", "d", relevant_file="x_MaSc_a.raw"),
            Issue("b", "d", relevant_file="x_MaSc_b.raw"),
        ]
        record_a, record_b = _record("UserAlert", "a"), _record("UserAlert", "b")
        db["get_open"].return_value = [record_a, record_b]
        alert = _alert("UserAlert", issues)
        alert.destinations = frozenset({"user"})
        manager = AlertManager()
        manager.alerts = [alert]

        # when - first check: a fails, b succeeds
        manager.check_for_issues(NOW)
        db["set_notified"].assert_called_once_with(record_b, NOW)
        record_b.last_notified = NOW

        # when - second check: only a is sent
        manager.check_for_issues(NOW + timedelta(minutes=1))

        # then
        assert [c.args[0].body for c in mock_send.call_args_list] == ["a", "b", "a"]
        assert db["set_notified"].call_args_list[-1].args[0] is record_a

    @patch("monitoring.alert_manager.send", return_value=True)
    def test_user_destination_sends_per_issue_to_the_files_user(
        self, mock_send: Mock, db: dict[str, Mock]
    ) -> None:
        """Alerts with the user destination go to the user of each issue's file."""
        # given
        alert = _alert(
            "QueueStopAlert",
            [Issue("inst1:x_MaSc_a.raw", "d", relevant_file="x_MaSc_a.raw")],
        )
        alert.title = None
        alert.destinations = frozenset({"user"})
        manager = AlertManager()
        manager.alerts = [alert]

        # when
        manager.check_for_issues(NOW)

        # then
        mock_send.assert_called_once_with(
            Message("inst1:x_MaSc_a.raw", None), recipients=["U_MASC"]
        )
        db["set_notified"].assert_called_once()

    @patch("monitoring.alert_manager.send", return_value=True)
    def test_muted_issue_is_neither_recorded_nor_sent(
        self, mock_send: Mock, db: dict[str, Mock]
    ) -> None:
        """An issue matching the last read suppressions is ignored."""
        # given
        manager = AlertManager()
        manager._suppressions = [
            Mock(alert="TestAlert", instrument_id="*", identifier="*", details="*")
        ]
        manager.alerts = [_alert(issues=[Issue("a", "details")])]

        # when
        manager.check_for_issues(NOW)

        # then
        db["add"].assert_not_called()
        mock_send.assert_not_called()
        db["suppressions"].assert_not_called()


class TestDispatch:
    """_dispatch routes an alert's issues to its configured destinations."""

    @patch("monitoring.alert_manager.send", return_value=True)
    def test_channels_get_one_message_with_all_issues(self, mock_send: Mock) -> None:
        """All issues go into one message to every channel destination."""
        # given
        alert = _alert("BusinessAlert")
        alert.destinations = frozenset({"business", "ops"})

        # when
        result = _dispatch(alert, [Issue("a", 1), Issue("b", 2)], {})

        # then
        assert result == [Issue("a", 1), Issue("b", 2)]
        mock_send.assert_called_once_with(
            Message("a,b", "BusinessAlert title"), frozenset({"business", "ops"})
        )

    @patch("monitoring.alert_manager.config.MAX_ISSUES_PER_MESSAGE", 2)
    @patch("monitoring.alert_manager.send", side_effect=[True, False])
    def test_channel_messages_are_chunked_and_delivered_per_chunk(
        self, mock_send: Mock
    ) -> None:
        """Issues are sent in chunks; a failed chunk leaves only its issues undelivered."""
        # given
        issues = [Issue("a", 1), Issue("b", 2), Issue("c", 3)]

        # when
        result = _dispatch(_alert(), issues, {})

        # then
        assert [c.args[0].body for c in mock_send.call_args_list] == ["a,b", "c"]
        assert result == issues[:2]

    @patch("monitoring.alert_manager.send", side_effect=[True, False])
    def test_users_get_one_message_per_issue_and_only_delivered_ones_are_returned(
        self, mock_send: Mock
    ) -> None:
        """Each issue goes to its file's user; a failed send leaves that issue undelivered."""
        # given
        alert = _alert("UserAlert")
        alert.destinations = frozenset({"user"})
        issues = [
            Issue("a", 1, relevant_file="x_MaSc_a.raw"),
            Issue("b", 2, relevant_file="x_AnSc_b.raw"),
        ]

        # when
        result = _dispatch(alert, issues, {"MaSc": "U_MASC", "AnSc": "U_ANSC"})

        # then
        assert result == [issues[0]]
        assert mock_send.call_args_list == [
            call(Message("a", "UserAlert title"), recipients=["U_MASC"]),
            call(Message("b", "UserAlert title"), recipients=["U_ANSC"]),
        ]

    @patch("monitoring.alert_manager.send", return_value=True)
    def test_issue_without_recipient_is_skipped(self, mock_send: Mock) -> None:
        """An issue whose file has no user with a messenger id is not sent, but counts as handled."""
        # given
        alert = _alert("UserAlert")
        alert.destinations = frozenset({"user"})

        # when
        result = _dispatch(alert, [Issue("a", 1, relevant_file="x_UNK_a.raw")], {})

        # then
        assert result == [Issue("a", 1, relevant_file="x_UNK_a.raw")]
        mock_send.assert_not_called()

    @patch("monitoring.alert_manager.send", return_value=True)
    def test_an_issue_that_comes_back_within_its_cooldown_is_not_notified_again(
        self, mock_send: Mock, db: dict[str, Mock]
    ) -> None:
        """The record survives a disappearance, so flapping issues keep their cooldown."""
        # given
        record = _record("TestAlert", "a", NOW - timedelta(minutes=10))
        db["get_open"].return_value = [record]
        alert = _alert()
        alert.get_issues.side_effect = [[], [Issue("a", "details")]]
        manager = AlertManager()
        manager.alerts = [alert]

        # when - absent, then back
        manager.check_for_issues(NOW)
        manager.check_for_issues(NOW + timedelta(minutes=1))

        # then
        db["add"].assert_not_called()
        mock_send.assert_not_called()
        db["set_notified"].assert_not_called()


class TestSendOnce:
    """send_once sends to the ops channel with an in-memory cooldown."""

    @patch("monitoring.alert_manager.send", return_value=True)
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
            call(Message("db down [db]"), {"ops"}),
            call(Message("db down [db]"), {"ops"}),
        ]

    @patch("monitoring.alert_manager.send", return_value=True)
    def test_cooldown_none_sends_only_once(self, mock_send: Mock) -> None:
        """Without cooldown a key is sent once per process."""
        # given
        manager = AlertManager()

        # when
        manager.send_once("k", Message("m"), None, NOW)
        manager.send_once("k", Message("m"), None, NOW + timedelta(days=1))

        # then
        mock_send.assert_called_once()

    @patch("monitoring.alert_manager.send", return_value=False)
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
    """DB-free alerts are sent via send_once, honoring the last known suppressions."""

    def test_muted_issue_is_not_sent(self, db: dict[str, Mock]) -> None:
        """A DB-free issue matching a suppression is skipped."""
        # given
        db["suppressions"].return_value = [
            Mock(
                alert="WebAppHealthAlert",
                instrument_id="*",
                identifier="*",
                details="*",
            )
        ]
        manager = AlertManager()
        manager.db_free_alerts = [
            _alert("WebAppHealthAlert", [Issue("webapp", "down")])
        ]

        # when
        with patch.object(manager, "send_once") as mock_send_once:
            manager.check_db_free_alerts(NOW)

        # then
        mock_send_once.assert_not_called()

    def test_last_known_suppressions_apply_while_the_db_is_down(
        self, db: dict[str, Mock]
    ) -> None:
        """If the suppressions cannot be read, the previous ones stay in effect."""
        # given
        manager = AlertManager()
        manager.db_free_alerts = [
            _alert("WebAppHealthAlert", [Issue("webapp", "down")])
        ]
        manager._suppressions = [
            Mock(
                alert="WebAppHealthAlert",
                instrument_id="*",
                identifier="*",
                details="*",
            )
        ]
        db["suppressions"].side_effect = RuntimeError("no db")

        # when
        with patch.object(manager, "send_once") as mock_send_once:
            manager.check_db_free_alerts(NOW)

        # then
        mock_send_once.assert_not_called()

    def test_issues_are_sent_once_per_alert_and_key(self, db: dict[str, Mock]) -> None:  # noqa: ARG002
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
