"""Unit tests for GradientPauseAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz
from slack_sdk.errors import SlackApiError

from monitoring.alerts.config import Cases
from monitoring.alerts.gradient_pause_alert import GradientPauseAlert
from shared.db.models import KrakenStatusEntities


def _make_status(instrument_id: str) -> Mock:
    s = Mock()
    s.id = instrument_id
    s.entity_type = KrakenStatusEntities.INSTRUMENT
    return s


def _make_raw_file(
    name: str, created_at: datetime, size: int = 1_000_000_000, file_id: str = ""
) -> Mock:
    f = Mock()
    f.original_name = name
    f.created_at = created_at
    f.size = size
    f.id = file_id or name
    return f


def _patch_query(files: list[Mock]) -> Mock:
    """Build a mock that mimics RawFile.objects.filter(...).order_by(...).only(...)[:3]."""
    sliced = Mock()
    sliced.__getitem__ = lambda _s, _k: files
    only_chain = Mock()
    only_chain.only = Mock(return_value=sliced)
    order_by_chain = Mock()
    order_by_chain.order_by = Mock(return_value=only_chain)
    objects = Mock()
    objects.filter = Mock(return_value=order_by_chain)
    return objects


class TestGradientPauseAlert:
    """Test suite for GradientPauseAlert class."""

    def test_name_returns_gradient_pause_case(self) -> None:
        """Name should map to Cases.GRADIENT_PAUSE."""
        assert GradientPauseAlert().name == Cases.GRADIENT_PAUSE

    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {},
        clear=True,
    )
    def test_get_issues_empty_when_dict_empty(self) -> None:
        """No alerts produced when initials dict is empty."""
        alert = GradientPauseAlert()
        assert alert._get_issues([_make_status("instr1")]) == []

    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {"MaSc": "U123"},
        clear=True,
    )
    def test_get_issues_skips_when_no_instruments(self) -> None:
        """Non-instrument status entries should not produce alerts."""
        alert = GradientPauseAlert()
        non_instrument = Mock()
        non_instrument.id = "file_remover"
        non_instrument.entity_type = KrakenStatusEntities.JOB
        assert alert._get_issues([non_instrument]) == []

    @patch("monitoring.alerts.gradient_pause_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.gradient_pause_alert.RawFile")
    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {"MaSc": "U123"},
        clear=True,
    )
    def test_get_issues_skips_when_fewer_than_three_files(
        self,
        mock_rawfile: Mock,
        _mock_augment: Mock,  # noqa: PT019
    ) -> None:
        """No alert when there are fewer than 3 files."""
        now = datetime.now(pytz.UTC)
        files = [
            _make_raw_file("a_MaSc_run1.d", now - timedelta(minutes=200)),
            _make_raw_file("a_MaSc_run2.d", now - timedelta(minutes=300)),
        ]
        mock_rawfile.objects = _patch_query(files)

        alert = GradientPauseAlert()
        assert alert._get_issues([_make_status("instr1")]) == []

    @patch("monitoring.alerts.gradient_pause_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.gradient_pause_alert.RawFile")
    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {"MaSc": "U123"},
        clear=True,
    )
    def test_get_issues_skips_when_initials_differ(
        self,
        mock_rawfile: Mock,
        _mock_augment: Mock,  # noqa: PT019
    ) -> None:
        """No alert when not all 3 files share the same known initials."""
        now = datetime.now(pytz.UTC)
        files = [
            _make_raw_file("a_MaSc_run3.d", now - timedelta(minutes=600)),
            _make_raw_file("a_MaSc_run2.d", now - timedelta(minutes=620)),
            _make_raw_file("a_OtHr_run1.d", now - timedelta(minutes=640)),
        ]
        mock_rawfile.objects = _patch_query(files)

        alert = GradientPauseAlert()
        assert alert._get_issues([_make_status("instr1")]) == []

    @patch("monitoring.alerts.gradient_pause_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.gradient_pause_alert.RawFile")
    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {"MaSc": "U123"},
        clear=True,
    )
    def test_get_issues_skips_when_initials_unknown(
        self,
        mock_rawfile: Mock,
        _mock_augment: Mock,  # noqa: PT019
    ) -> None:
        """Files with unknown initials produce no alert."""
        now = datetime.now(pytz.UTC)
        files = [
            _make_raw_file("a_XXXX_run3.d", now - timedelta(minutes=600)),
            _make_raw_file("a_XXXX_run2.d", now - timedelta(minutes=620)),
            _make_raw_file("a_XXXX_run1.d", now - timedelta(minutes=640)),
        ]
        mock_rawfile.objects = _patch_query(files)

        alert = GradientPauseAlert()
        assert alert._get_issues([_make_status("instr1")]) == []

    @patch("monitoring.alerts.gradient_pause_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.gradient_pause_alert.RawFile")
    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {"MaSc": "U123"},
        clear=True,
    )
    def test_get_issues_skips_when_pause_below_threshold(
        self,
        mock_rawfile: Mock,
        _mock_augment: Mock,  # noqa: PT019
    ) -> None:
        """No alert when pause <= 3 * gradient_length."""
        now = datetime.now(pytz.UTC)
        # gradient_length = 30 min, pause = 60 min => 60 < 90
        files = [
            _make_raw_file("a_MaSc_run3.d", now - timedelta(minutes=60)),
            _make_raw_file("a_MaSc_run2.d", now - timedelta(minutes=90)),
            _make_raw_file("a_MaSc_run1.d", now - timedelta(minutes=120)),
        ]
        mock_rawfile.objects = _patch_query(files)
        _mock_augment.return_value = {f.id: {} for f in files}

        alert = GradientPauseAlert()
        assert alert._get_issues([_make_status("instr1")]) == []

    @patch("monitoring.alerts.gradient_pause_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.gradient_pause_alert.RawFile")
    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {"MaSc": "U123"},
        clear=True,
    )
    def test_get_issues_returns_issue_when_pause_above_threshold(
        self, mock_rawfile: Mock, mock_augment: Mock
    ) -> None:
        """Issue raised when pause > 3 * gradient_length and initials match."""
        now = datetime.now(pytz.UTC)
        # gradient_length = 30 min, pause = 200 min (> 90)
        files = [
            _make_raw_file(
                "a_MaSc_run3.d",
                now - timedelta(minutes=200),
                size=2 * 1024 * 1024,
                file_id="id3",
            ),
            _make_raw_file(
                "a_MaSc_run2.d",
                now - timedelta(minutes=230),
                size=3 * 1024 * 1024,
                file_id="id2",
            ),
            _make_raw_file(
                "a_MaSc_run1.d",
                now - timedelta(minutes=260),
                size=4 * 1024 * 1024,
                file_id="id1",
            ),
        ]
        mock_rawfile.objects = _patch_query(files)
        mock_augment.return_value = {
            "id3": {"alphadia__precursors": 12345},
            "id2": {"alphadia__precursors": 23456},
            "id1": {"alphadia__precursors": 34567},
        }

        alert = GradientPauseAlert()
        issues = alert._get_issues([_make_status("instr1")])

        assert len(issues) == 1
        instrument_id, details = issues[0]
        assert instrument_id == "instr1"
        assert details["initials"] == "MaSc"
        assert details["slack_user_id"] == "U123"
        assert len(details["files"]) == 3
        assert details["files"][0]["name"] == "a_MaSc_run3.d"
        assert details["files"][0]["precursors"] == 12345

    @patch("monitoring.alerts.gradient_pause_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.gradient_pause_alert.RawFile")
    @patch.dict(
        "monitoring.alerts.gradient_pause_alert.INITIALS_TO_SLACK_ID",
        {"MaSc": "U123"},
        clear=True,
    )
    def test_cooldown_does_not_realert_for_same_youngest_file(
        self, mock_rawfile: Mock, mock_augment: Mock
    ) -> None:
        """Subsequent calls with the same youngest file should not re-alert."""
        now = datetime.now(pytz.UTC)
        files = [
            _make_raw_file(
                "a_MaSc_run3.d", now - timedelta(minutes=200), file_id="id3"
            ),
            _make_raw_file(
                "a_MaSc_run2.d", now - timedelta(minutes=230), file_id="id2"
            ),
            _make_raw_file(
                "a_MaSc_run1.d", now - timedelta(minutes=260), file_id="id1"
            ),
        ]
        mock_rawfile.objects = _patch_query(files)
        mock_augment.return_value = {f.id: {} for f in files}

        alert = GradientPauseAlert()
        first = alert._get_issues([_make_status("instr1")])
        second = alert._get_issues([_make_status("instr1")])

        assert len(first) == 1
        assert second == []

    @patch("monitoring.alerts.gradient_pause_alert.WebClient")
    @patch("monitoring.alerts.gradient_pause_alert.SLACK_BOT_TOKEN", "xoxb-test")
    def test_dispatch_issues_dms_each_user(self, mock_webclient_cls: Mock) -> None:
        """dispatch_issues should DM the slack_user_id for each issue."""
        client = Mock()
        mock_webclient_cls.return_value = client

        alert = GradientPauseAlert()
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=pytz.UTC)
        details = {
            "initials": "MaSc",
            "slack_user_id": "U123",
            "gradient_length": timedelta(minutes=30),
            "pause": timedelta(minutes=200),
            "files": [
                {
                    "name": "a.d",
                    "size_bytes": 1024 * 1024,
                    "precursors": 100,
                    "created_at": ts,
                },
                {
                    "name": "b.d",
                    "size_bytes": 2 * 1024 * 1024,
                    "precursors": 200,
                    "created_at": ts - timedelta(minutes=30),
                },
                {
                    "name": "c.d",
                    "size_bytes": 3 * 1024 * 1024,
                    "precursors": 300,
                    "created_at": ts - timedelta(minutes=60),
                },
            ],
        }
        alert.dispatch_issues([("instr1", details)])

        client.chat_postMessage.assert_called_once()
        kwargs = client.chat_postMessage.call_args.kwargs
        assert kwargs["channel"] == "U123"
        assert "instr1" in kwargs["text"]
        assert "a.d" in kwargs["text"]
        assert "2026-05-07 12:00:00 UTC" in kwargs["text"]

    @patch("monitoring.alerts.gradient_pause_alert.WebClient")
    @patch("monitoring.alerts.gradient_pause_alert.SLACK_BOT_TOKEN", "xoxb-test")
    def test_dispatch_issues_continues_after_slack_error(
        self, mock_webclient_cls: Mock
    ) -> None:
        """A failed DM must not stop the loop."""
        client = Mock()
        client.chat_postMessage.side_effect = [
            SlackApiError("boom", response={"error": "channel_not_found"}),
            None,
        ]
        mock_webclient_cls.return_value = client

        alert = GradientPauseAlert()
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=pytz.UTC)
        details_template = {
            "initials": "MaSc",
            "gradient_length": timedelta(minutes=30),
            "pause": timedelta(minutes=200),
            "files": [
                {
                    "name": "a.d",
                    "size_bytes": 1024 * 1024,
                    "precursors": 1,
                    "created_at": ts,
                },
                {
                    "name": "b.d",
                    "size_bytes": 1024 * 1024,
                    "precursors": 2,
                    "created_at": ts - timedelta(minutes=30),
                },
                {
                    "name": "c.d",
                    "size_bytes": 1024 * 1024,
                    "precursors": 3,
                    "created_at": ts - timedelta(minutes=60),
                },
            ],
        }
        issues = [
            ("instr1", {**details_template, "slack_user_id": "U_BAD"}),
            ("instr2", {**details_template, "slack_user_id": "U_OK"}),
        ]
        alert.dispatch_issues(issues)

        assert client.chat_postMessage.call_count == 2

    @patch("monitoring.alerts.gradient_pause_alert.ADMIN_SLACK_ID", "U_ADMIN")
    @patch("monitoring.alerts.gradient_pause_alert.WebClient")
    @patch("monitoring.alerts.gradient_pause_alert.SLACK_BOT_TOKEN", "xoxb-test")
    def test_dispatch_issues_cc_admin(self, mock_webclient_cls: Mock) -> None:
        """When ADMIN_SLACK_ID is set, the admin is cc'd on every issue DM."""
        client = Mock()
        mock_webclient_cls.return_value = client

        alert = GradientPauseAlert()
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=pytz.UTC)
        details = {
            "initials": "MaSc",
            "slack_user_id": "U_USER",
            "gradient_length": timedelta(minutes=30),
            "pause": timedelta(minutes=200),
            "files": [
                {
                    "name": f"f{i}.d",
                    "size_bytes": 1024 * 1024,
                    "precursors": 1,
                    "created_at": ts - timedelta(minutes=30 * i),
                }
                for i in range(3)
            ],
        }
        alert.dispatch_issues([("instr1", details)])

        channels = [c.kwargs["channel"] for c in client.chat_postMessage.call_args_list]
        assert channels == ["U_USER", "U_ADMIN"]

    @patch("monitoring.alerts.gradient_pause_alert.ADMIN_SLACK_ID", "U_USER")
    @patch("monitoring.alerts.gradient_pause_alert.WebClient")
    @patch("monitoring.alerts.gradient_pause_alert.SLACK_BOT_TOKEN", "xoxb-test")
    def test_dispatch_issues_does_not_double_dm_when_admin_is_user(
        self, mock_webclient_cls: Mock
    ) -> None:
        """If the admin and the responsible user are the same person, send only once."""
        client = Mock()
        mock_webclient_cls.return_value = client

        alert = GradientPauseAlert()
        ts = datetime(2026, 5, 7, 12, 0, 0, tzinfo=pytz.UTC)
        details = {
            "initials": "MaSc",
            "slack_user_id": "U_USER",
            "gradient_length": timedelta(minutes=30),
            "pause": timedelta(minutes=200),
            "files": [
                {
                    "name": f"f{i}.d",
                    "size_bytes": 1024 * 1024,
                    "precursors": 1,
                    "created_at": ts - timedelta(minutes=30 * i),
                }
                for i in range(3)
            ],
        }
        alert.dispatch_issues([("instr1", details)])

        assert client.chat_postMessage.call_count == 1

    @patch("monitoring.alerts.gradient_pause_alert.WebClient")
    @patch("monitoring.alerts.gradient_pause_alert.SLACK_BOT_TOKEN", "")
    def test_dispatch_issues_skips_when_token_missing(
        self, mock_webclient_cls: Mock
    ) -> None:
        """No DM is attempted when SLACK_BOT_TOKEN is unset."""
        alert = GradientPauseAlert()
        alert.dispatch_issues(
            [
                (
                    "instr1",
                    {
                        "initials": "MaSc",
                        "slack_user_id": "U123",
                        "gradient_length": timedelta(minutes=30),
                        "pause": timedelta(minutes=200),
                        "files": [],
                    },
                )
            ]
        )
        mock_webclient_cls.assert_not_called()
