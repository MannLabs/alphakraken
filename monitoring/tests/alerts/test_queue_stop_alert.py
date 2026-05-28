"""Unit tests for QueueEndAlert (queue-stop user alert)."""

from collections.abc import Generator
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
import pytz

from monitoring.alerts.config import Cases
from monitoring.alerts.queue_stop_alert import (
    KIND_HANDOFF,
    KIND_STALL,
    QueueEndAlert,
    QueueEndIssue,
)


def _make_file(
    file_id: str, name: str, created_at: datetime, size: int | None = 1_000_000
) -> MagicMock:
    """Build a Mock RawFile with the fields the alert reads."""
    raw_file = MagicMock()
    raw_file.id = file_id
    raw_file.original_name = name
    raw_file.created_at = created_at
    raw_file.size = size
    return raw_file


def _install_rawfile_mock(
    mock_rawfile: Mock, instrument_files: dict[str, list]
) -> None:
    """Wire `RawFile.objects.distinct(...)` and per-instrument `.filter(...).only(...).order_by(...).limit(...)`."""
    mock_rawfile.objects.distinct.return_value = list(instrument_files.keys())

    def _filter(*, instrument_id: str) -> MagicMock:
        files = instrument_files[instrument_id]
        chain = MagicMock()
        chain.only.return_value.order_by.return_value.limit.return_value = files
        return chain

    mock_rawfile.objects.filter.side_effect = _filter


@pytest.fixture(autouse=True)
def _reset_class_tracker() -> Generator[None, None, None]:
    """Each test gets a fresh `_alerted_subject_files` to keep tests independent."""
    QueueEndAlert._alerted_subject_files.clear()
    yield
    QueueEndAlert._alerted_subject_files.clear()


# -- Setup / common -----------------------------------------------------------


class TestQueueEndAlertBasics:
    """Tests for the alert's identity and trivial guard paths."""

    def test_name_returns_queue_stop_case(self) -> None:
        """`name` property returns the QUEUE_END case."""
        # given / when
        result = QueueEndAlert().name
        # then
        assert result == Cases.QUEUE_END

    @patch(
        "monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS",
        {"MaSc": "U_MASC"},
    )
    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_no_alert_when_fewer_than_two_files(self, mock_rawfile: Mock) -> None:
        """Instruments with fewer than two raw files do not produce alerts."""
        # given
        now = datetime.now(tz=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {"inst1": [_make_file("f1", "x_MaSc_y.raw", now)]},
        )
        # when
        result = QueueEndAlert()._get_issues([])
        # then
        assert result == []

    @patch("monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS", {})
    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_no_alert_when_no_files_have_mapped_initials(
        self, mock_rawfile: Mock
    ) -> None:
        """Empty user-id map short-circuits before any DB query."""
        # given - empty user map -> alert short-circuits without querying DB
        # when
        result = QueueEndAlert()._get_issues([])
        # then
        assert result == []
        mock_rawfile.objects.distinct.assert_not_called()

    def test_initials_pattern_matches_underscored_token_only(self) -> None:
        """The initials regex matches only underscored 2-8 letter tokens."""
        # given
        from monitoring.alerts.queue_stop_alert import INITIALS_PATTERN

        # when / then
        assert INITIALS_PATTERN.search("_MaSc_") is not None
        assert INITIALS_PATTERN.search("xxx_AB_yyy") is not None
        # Bare token (no surrounding underscores) must NOT match
        assert INITIALS_PATTERN.search("MaScfoo") is None
        # 1-char tokens excluded by {2,8}
        assert INITIALS_PATTERN.search("_A_") is None
        # 9-char tokens excluded
        assert INITIALS_PATTERN.search("_ABCDEFGHI_") is None

    @patch(
        "monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS",
        {"MaSc": "U_MASC", "JoeB": "U_JOEB"},
    )
    @patch("monitoring.alerts.queue_stop_alert.MAX_GRADIENT_LENGTH_HOURS", 2)
    @patch("monitoring.alerts.queue_stop_alert.QUEUE_END_THRESHOLD_MULTIPLIER", 3)
    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_rules_a_and_b_are_mutually_exclusive(self, mock_rawfile: Mock) -> None:
        """For any (file1, file2, file3) initials triplet, at most one rule fires."""
        # given - try every initials triplet of {MaSc, JoeB, None}
        # for each, ensure at most one rule fires
        now = datetime.now(tz=pytz.UTC)
        long_pause_ago = now - timedelta(hours=10)
        thirty_min_before = long_pause_ago - timedelta(minutes=30)
        sixty_min_before = thirty_min_before - timedelta(minutes=30)

        token_table = ["MaSc", "JoeB", None]
        for t1 in token_table:
            for t2 in token_table:
                for t3 in token_table:
                    n1 = f"x_{t1}_z.raw" if t1 else "noinitials1.raw"
                    n2 = f"x_{t2}_z.raw" if t2 else "noinitials2.raw"
                    n3 = f"x_{t3}_z.raw" if t3 else "noinitials3.raw"
                    files = [
                        _make_file("a", n1, long_pause_ago),
                        _make_file("b", n2, thirty_min_before),
                        _make_file("c", n3, sixty_min_before),
                    ]
                    QueueEndAlert._alerted_subject_files.clear()
                    _install_rawfile_mock(mock_rawfile, {"inst1": files})
                    with patch(
                        "monitoring.alerts.queue_stop_alert.datetime"
                    ) as mock_dt:
                        mock_dt.now.return_value = now
                        result = QueueEndAlert()._get_issues([])
                    # then - at most one issue per combination, never both
                    assert len(result) <= 1, (t1, t2, t3)


# -- Rule A - Stall -----------------------------------------------------------


@patch(
    "monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS", {"MaSc": "U_MASC"}
)
@patch("monitoring.alerts.queue_stop_alert.MAX_GRADIENT_LENGTH_HOURS", 2)
@patch("monitoring.alerts.queue_stop_alert.QUEUE_END_THRESHOLD_MULTIPLIER", 3)
class TestRuleAStall:
    """Stall: file1 & file2 share mapped initials, pause > 3 x gradient."""

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_no_alert_when_pause_below_threshold(
        self, mock_rawfile: Mock
    ) -> None:
        """Pause of 2 x gradient is below the 3x threshold and does not alert."""
        # given - pause = 2 x gradient_length (below 3x threshold)
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        file1_t = now - 2 * gradient  # pause = 2 x gradient
        file2_t = file1_t - gradient

        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", file1_t),
                    _make_file("f2", "x_MaSc_b.raw", file2_t),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            # when
            result = QueueEndAlert()._get_issues([])
        # then
        assert result == []

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_alert_fires_when_pause_exceeds_threshold(
        self, mock_rawfile: Mock
    ) -> None:
        """Pause of 3.5 x gradient triggers a stall alert for the shared user."""
        # given - pause = 3.5 x gradient_length, gradient = 30 min
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        file1_t = now - 3.5 * gradient
        file2_t = file1_t - gradient

        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", file1_t),
                    _make_file("f2", "x_MaSc_b.raw", file2_t),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            # when
            result = QueueEndAlert()._get_issues([])
        # then
        assert len(result) == 1
        identifier, issue = result[0]
        assert identifier == "inst1:f1"
        assert issue.kind == KIND_STALL
        assert issue.messenger_user_id == "U_MASC"

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_skips_when_gradient_length_exceeds_max(
        self, mock_rawfile: Mock
    ) -> None:
        """A gap > MAX_GRADIENT_LENGTH_HOURS is treated as a new-queue start, not a stall."""
        # given - gap = 3 h > 2 h max -> treat file1 as new queue start
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        file1_t = now - timedelta(hours=10)  # pause is huge but gradient is too big
        file2_t = file1_t - timedelta(hours=3)

        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", file1_t),
                    _make_file("f2", "x_MaSc_b.raw", file2_t),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            # when
            result = QueueEndAlert()._get_issues([])
        # then
        assert result == []

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_skips_when_gradient_length_zero_or_negative(
        self, mock_rawfile: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Non-positive gradient (clock skew / duplicates) is logged and skipped."""
        # given - both files share the same timestamp -> gradient = 0
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        same = now - timedelta(hours=5)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", same),
                    _make_file("f2", "x_MaSc_b.raw", same),
                ]
            },
        )
        with (
            patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt,
            caplog.at_level("WARNING"),
        ):
            mock_dt.now.return_value = now
            # when
            result = QueueEndAlert()._get_issues([])
        # then
        assert result == []
        assert any("non-positive gradient" in rec.message for rec in caplog.records)

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_includes_recent_files_with_size(self, mock_rawfile: Mock) -> None:
        """Issue carries up to three recent files with their (name, size) tuples."""
        # given - three files; first two share initials and trigger stall
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        f1_t = now - 4 * gradient
        f2_t = f1_t - gradient
        f3_t = f2_t - timedelta(minutes=20)

        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", f1_t, size=2 * 1024**3),
                    _make_file("f2", "x_MaSc_b.raw", f2_t, size=512 * 1024**2),
                    _make_file("f3", "x_MaSc_c.raw", f3_t, size=None),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            # when
            result = QueueEndAlert()._get_issues([])
        # then
        assert len(result) == 1
        _, issue = result[0]
        assert issue.recent_files == [
            ("x_MaSc_a.raw", 2 * 1024**3),
            ("x_MaSc_b.raw", 512 * 1024**2),
            ("x_MaSc_c.raw", None),
        ]

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_renders_two_files_when_instrument_has_only_two(
        self, mock_rawfile: Mock
    ) -> None:
        """Two-file instruments still trigger stall and render just those two files."""
        # given - only two files on the instrument
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        f1_t = now - 4 * gradient
        f2_t = f1_t - gradient
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", f1_t, size=1024**3),
                    _make_file("f2", "x_MaSc_b.raw", f2_t, size=1024**3),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            # when
            result = QueueEndAlert()._get_issues([])
        # then
        assert len(result) == 1
        _, issue = result[0]
        assert len(issue.recent_files) == 2

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_cooldown_no_repeat_alert_for_same_subject_file_id(
        self, mock_rawfile: Mock
    ) -> None:
        """Same (instrument, subject_file_id) does not re-fire on repeated polls."""
        # given - same data twice; second call must not re-fire
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        f1_t = now - 4 * gradient
        f2_t = f1_t - gradient
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", f1_t),
                    _make_file("f2", "x_MaSc_b.raw", f2_t),
                ]
            },
        )
        alert = QueueEndAlert()
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            # when - first call fires; second is suppressed
            first = alert._get_issues([])
            second = alert._get_issues([])
        # then
        assert len(first) == 1
        assert second == []

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_cooldown_releases_when_newer_file_appears(
        self, mock_rawfile: Mock
    ) -> None:
        """A new newest file with a different id can re-trigger stall (different cooldown key)."""
        # given - first call fires with f1 newest; later a new f0 file becomes newest
        now1 = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        f1_t = now1 - 4 * gradient
        f2_t = f1_t - gradient
        # f0 appears 30 min after f1 (preserving valid gradient <=2h),
        # and now2 is far enough later that pause(f0) > 3 x gradient.
        f0_t = f1_t + gradient
        now2 = f0_t + 4 * gradient

        alert = QueueEndAlert()

        # first scan
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", f1_t),
                    _make_file("f2", "x_MaSc_b.raw", f2_t),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now1
            first = alert._get_issues([])

        # second scan with newer f0
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f0", "x_MaSc_c.raw", f0_t),
                    _make_file("f1", "x_MaSc_a.raw", f1_t),
                    _make_file("f2", "x_MaSc_b.raw", f2_t),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now2
            second = alert._get_issues([])

        # then - both fire, different subject_file_ids
        assert len(first) == 1
        assert first[0][0] == "inst1:f1"
        assert len(second) == 1
        assert second[0][0] == "inst1:f0"


# -- Rule B - Handoff ---------------------------------------------------------


@patch(
    "monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS",
    {"MaSc": "U_MASC", "JoeB": "U_JOEB"},
)
@patch("monitoring.alerts.queue_stop_alert.MAX_GRADIENT_LENGTH_HOURS", 2)
@patch("monitoring.alerts.queue_stop_alert.QUEUE_END_THRESHOLD_MULTIPLIER", 3)
class TestRuleBHandoff:
    """Handoff: file2 & file3 share mapped initials, file1 differs (any kind)."""

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_fires_when_prior_two_share_mapped_initials_and_newest_differs(
        self, mock_rawfile: Mock
    ) -> None:
        """Standard handoff: prior pair shares mapped initials, newest differs -> alert prior."""
        # given - JoeB just took over from MaSc
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        # New file is recent; prior pair is in same queue (gap = 30 min <= 2h)
        f1_t = now - timedelta(minutes=5)
        f2_t = now - timedelta(hours=1)
        f3_t = f2_t - timedelta(minutes=30)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_JoeB_n.raw", f1_t),
                    _make_file("f2", "x_MaSc_a.raw", f2_t),
                    _make_file("f3", "x_MaSc_b.raw", f3_t),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert len(result) == 1
        identifier, issue = result[0]
        assert identifier == "inst1:f2"
        assert issue.kind == KIND_HANDOFF
        assert issue.messenger_user_id == "U_MASC"

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_skips_when_only_two_files_on_instrument(
        self, mock_rawfile: Mock
    ) -> None:
        """With only two files there's no third to identify the prior user's queue."""
        # given - only two files; no file3, can't identify prior queue
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        # Stall path also disabled (initials differ)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_JoeB_n.raw", now - timedelta(minutes=10)),
                    _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert result == []

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_skips_when_prior_pair_does_not_share_initials(
        self, mock_rawfile: Mock
    ) -> None:
        """Single-file prior runs (f2 != f3) do not trigger handoff."""
        # given - prior pair (f2, f3) has differing initials
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_JoeB_n.raw", now - timedelta(minutes=5)),
                    _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                    _make_file("f3", "x_JoeB_b.raw", now - timedelta(hours=2)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert result == []

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_skips_when_prior_pair_initials_unmapped(
        self, mock_rawfile: Mock
    ) -> None:
        """Prior pair shares an initials token but it's not in the user-id map."""
        # given - prior pair (f2, f3) share initials, but NOT in mapping
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_JoeB_n.raw", now - timedelta(minutes=5)),
                    _make_file("f2", "x_UNK_a.raw", now - timedelta(hours=1)),
                    _make_file("f3", "x_UNK_b.raw", now - timedelta(hours=2)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert result == []

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_fires_when_newest_has_different_mapped_initials(
        self, mock_rawfile: Mock
    ) -> None:
        """X took over from Y -> alert Y only (the prior user)."""
        # given - X took over from Y -> alert Y only
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_JoeB_n.raw", now - timedelta(minutes=5)),
                    _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                    _make_file("f3", "x_MaSc_b.raw", now - timedelta(hours=2)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert len(result) == 1
        _, issue = result[0]
        assert issue.messenger_user_id == "U_MASC"

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_fires_when_newest_has_no_initials_token(
        self, mock_rawfile: Mock
    ) -> None:
        """Unattributable QC file as newest -> prior user still alerted."""
        # given - unattributable QC file -> still alert prior user
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "QC_check.raw", now - timedelta(minutes=5)),
                    _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                    _make_file("f3", "x_MaSc_b.raw", now - timedelta(hours=2)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert len(result) == 1
        _, issue = result[0]
        assert issue.messenger_user_id == "U_MASC"

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_fires_when_newest_has_unmapped_initials(
        self, mock_rawfile: Mock
    ) -> None:
        """Newest file has an initials token but it isn't in the map -> alert prior user only."""
        # given - file1 has an initials-like token but not in mapping
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_UNK_n.raw", now - timedelta(minutes=5)),
                    _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                    _make_file("f3", "x_MaSc_b.raw", now - timedelta(hours=2)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert len(result) == 1
        _, issue = result[0]
        assert issue.messenger_user_id == "U_MASC"

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_skips_when_prior_gap_exceeds_max_gradient_length_hours(
        self, mock_rawfile: Mock
    ) -> None:
        """Prior pair gap > MAX_GRADIENT_LENGTH_HOURS means it wasn't a real queue."""
        # given - prior pair gap = 3 h > 2 h -> not a real queue
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_JoeB_n.raw", now - timedelta(minutes=5)),
                    _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                    _make_file("f3", "x_MaSc_b.raw", now - timedelta(hours=4)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = QueueEndAlert()._get_issues([])
        # then
        assert result == []

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_cooldown_no_repeat_alert_for_same_subject_file_id(
        self, mock_rawfile: Mock
    ) -> None:
        """Same handoff polled twice fires only once."""
        # given - same handoff scenario polled twice; second poll suppressed
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        files = {
            "inst1": [
                _make_file("f1", "x_JoeB_n.raw", now - timedelta(minutes=5)),
                _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                _make_file("f3", "x_MaSc_b.raw", now - timedelta(hours=2)),
            ]
        }
        _install_rawfile_mock(mock_rawfile, files)
        alert = QueueEndAlert()
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            first = alert._get_issues([])
            second = alert._get_issues([])
        # then
        assert len(first) == 1
        assert second == []


# -- Unified cooldown ---------------------------------------------------------


@patch(
    "monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS",
    {"MaSc": "U_MASC", "JoeB": "U_JOEB"},
)
@patch("monitoring.alerts.queue_stop_alert.MAX_GRADIENT_LENGTH_HOURS", 2)
@patch("monitoring.alerts.queue_stop_alert.QUEUE_END_THRESHOLD_MULTIPLIER", 3)
class TestUnifiedCooldown:
    """A stall about user-last-file F must suppress later handoff alerts about the same F."""

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_unified_cooldown_handoff_suppressed_after_stall_for_same_user_last_file(
        self, mock_rawfile: Mock
    ) -> None:
        """Stall on F then handoff on F (now second-newest) does not re-fire."""
        # given - first scan triggers stall on F (newest). Later, JoeB takes over
        # so F becomes second-newest. The handoff for F must NOT re-fire.
        now1 = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        f_t = now1 - 4 * gradient  # 2 h pause = 4 x 30 min (well above 3x)
        prev_t = f_t - gradient
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("F", "x_MaSc_a.raw", f_t),
                    _make_file("prev", "x_MaSc_b.raw", prev_t),
                ]
            },
        )
        alert = QueueEndAlert()
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now1
            first = alert._get_issues([])
        assert len(first) == 1
        assert first[0][0] == "inst1:F"

        # later: a JoeB file becomes newest; F is now second-newest
        now2 = now1 + timedelta(hours=4)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("newF", "x_JoeB_n.raw", now2 - timedelta(minutes=5)),
                    _make_file("F", "x_MaSc_a.raw", f_t),
                    _make_file("prev", "x_MaSc_b.raw", prev_t),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now2
            second = alert._get_issues([])
        # then - same (instrument, F) key already fired -> suppressed
        assert second == []


# -- Recipients & delivery ----------------------------------------------------


def _build_stall_issue(
    messenger_user_id: str = "U_MASC", instrument_id: str = "inst1"
) -> QueueEndIssue:
    """Construct a stall QueueEndIssue for delivery tests."""
    return QueueEndIssue(
        kind=KIND_STALL,
        instrument_id=instrument_id,
        messenger_user_id=messenger_user_id,
        gradient_length=timedelta(minutes=30),
        pause=timedelta(minutes=120),
        recent_files=[("x_MaSc_a.raw", 1024**3), ("x_MaSc_b.raw", 512 * 1024**2)],
    )


def _build_handoff_issue(messenger_user_id: str = "U_MASC") -> QueueEndIssue:
    """Construct a handoff QueueEndIssue for delivery tests."""
    return QueueEndIssue(
        kind=KIND_HANDOFF,
        instrument_id="inst1",
        messenger_user_id=messenger_user_id,
        gradient_length=timedelta(minutes=30),
        pause=None,
        recent_files=[("x_JoeB_n.raw", 1024**3), ("x_MaSc_a.raw", 1024**3)],
    )


class TestRecipientsAndDelivery:
    """Recipient list construction and DM fan-out via `send_dm`."""

    @patch(
        "monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS",
        {"MaSc": "U_MASC"},
    )
    @patch("monitoring.alerts.queue_stop_alert.MAX_GRADIENT_LENGTH_HOURS", 2)
    @patch("monitoring.alerts.queue_stop_alert.QUEUE_END_THRESHOLD_MULTIPLIER", 3)
    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_stall_recipient_is_shared_initials_user(self, mock_rawfile: Mock) -> None:
        """Stall recipient is the Slack user mapped from the shared initials."""
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        gradient = timedelta(minutes=30)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_MaSc_a.raw", now - 4 * gradient),
                    _make_file("f2", "x_MaSc_b.raw", now - 5 * gradient),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            issues = QueueEndAlert()._get_issues([])
        assert issues[0][1].messenger_user_id == "U_MASC"

    @patch(
        "monitoring.alerts.queue_stop_alert.INSTRUMENT_USER_SLACK_IDS",
        {"MaSc": "U_MASC", "JoeB": "U_JOEB"},
    )
    @patch("monitoring.alerts.queue_stop_alert.MAX_GRADIENT_LENGTH_HOURS", 2)
    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_handoff_recipient_is_prior_file_user_not_new_operator(
        self, mock_rawfile: Mock
    ) -> None:
        """Handoff recipient is the prior user only; the new operator is not notified."""
        now = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
        _install_rawfile_mock(
            mock_rawfile,
            {
                "inst1": [
                    _make_file("f1", "x_JoeB_n.raw", now - timedelta(minutes=5)),
                    _make_file("f2", "x_MaSc_a.raw", now - timedelta(hours=1)),
                    _make_file("f3", "x_MaSc_b.raw", now - timedelta(hours=2)),
                ]
            },
        )
        with patch("monitoring.alerts.queue_stop_alert.datetime") as mock_dt:
            mock_dt.now.return_value = now
            issues = QueueEndAlert()._get_issues([])
        assert len(issues) == 1
        assert issues[0][1].messenger_user_id == "U_MASC"  # prior, not JoeB

    @patch("monitoring.alerts.queue_stop_alert.SPECIAL_ALERT_SLACK_ID", "U_SUP")
    @patch("monitoring.alerts.queue_stop_alert.send_dm")
    def test_special_id_cc_when_configured(self, mock_send_dm: Mock) -> None:
        """When SPECIAL_ALERT_SLACK_ID is set, it is CC'd on every DM."""
        # given
        issue = _build_stall_issue(messenger_user_id="U_MASC")
        # when
        QueueEndAlert().dispatch([("inst1:f1", issue)])
        # then - two DMs: user, then special ID
        recipients = [call.args[1] for call in mock_send_dm.call_args_list]
        assert recipients == ["U_MASC", "U_SUP"]

    @patch("monitoring.alerts.queue_stop_alert.SPECIAL_ALERT_SLACK_ID", None)
    @patch("monitoring.alerts.queue_stop_alert.send_dm")
    def test_no_special_id_cc_when_not_configured(self, mock_send_dm: Mock) -> None:
        """When SPECIAL_ALERT_SLACK_ID is None, only the user receives the DM."""
        # given
        issue = _build_stall_issue()
        # when
        QueueEndAlert().dispatch([("inst1:f1", issue)])
        # then
        assert mock_send_dm.call_count == 1
        assert mock_send_dm.call_args.args[1] == "U_MASC"

    @patch("monitoring.alerts.queue_stop_alert.SPECIAL_ALERT_SLACK_ID", "U_MASC")
    @patch("monitoring.alerts.queue_stop_alert.send_dm")
    def test_recipients_deduplicated_when_user_id_equals_special_id(
        self, mock_send_dm: Mock
    ) -> None:
        """If the alerted user IS the special ID, the recipient list dedups to one entry."""
        # given - user IS the special ID
        issue = _build_stall_issue(messenger_user_id="U_MASC")
        # when
        QueueEndAlert().dispatch([("inst1:f1", issue)])
        # then - only one DM
        assert mock_send_dm.call_count == 1

    @patch("monitoring.alerts.queue_stop_alert.SPECIAL_ALERT_SLACK_ID", "U_SUP")
    @patch("monitoring.alerts.queue_stop_alert.send_dm")
    def test_dispatch_sends_separate_dm_per_recipient_per_issue(
        self, mock_send_dm: Mock
    ) -> None:
        """Each (issue x recipient) pair produces its own DM call; messages aren't bundled."""
        # given - two issues, each goes to user + special ID = 4 DMs total
        issue1 = _build_stall_issue(messenger_user_id="U_MASC")
        issue2 = _build_handoff_issue(messenger_user_id="U_JOEB")
        # when
        QueueEndAlert().dispatch([("inst1:f1", issue1), ("inst2:f2", issue2)])
        # then
        assert mock_send_dm.call_count == 4
        recipients = [call.args[1] for call in mock_send_dm.call_args_list]
        # messages are not bundled across users
        assert recipients == ["U_MASC", "U_SUP", "U_JOEB", "U_SUP"]
        # messages differ per kind
        messages = [call.args[0] for call in mock_send_dm.call_args_list]
        assert "stall" in messages[0].lower() or "Queue stall" in messages[0]
        assert "handoff" in messages[2].lower() or "Queue handoff" in messages[2]

    @patch("monitoring.alerts.queue_stop_alert.SPECIAL_ALERT_SLACK_ID", "U_SUP")
    @patch("monitoring.alerts.queue_stop_alert.send_dm")
    def test_dispatch_continues_after_failed_send_dm(
        self, mock_send_dm: Mock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A failed send to one recipient must not abort delivery to the next recipient."""
        # given - first recipient raises; second still receives the DM
        import requests

        def _side_effect(_message: str, recipient_id: str, **_kwargs: object) -> None:
            if recipient_id == "U_MASC":
                raise requests.HTTPError("Slack returned 500")

        mock_send_dm.side_effect = _side_effect
        issue = _build_stall_issue(messenger_user_id="U_MASC")
        # when
        with caplog.at_level("WARNING"):
            QueueEndAlert().dispatch([("inst1:f1", issue)])
        # then - both recipients attempted; failure logged with context
        assert mock_send_dm.call_count == 2
        assert any(
            "U_MASC" in rec.message
            and "stall" in rec.message
            and "inst1" in rec.message
            for rec in caplog.records
        )
