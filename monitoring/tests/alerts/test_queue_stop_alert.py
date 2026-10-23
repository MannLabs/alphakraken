"""Unit tests for the queue-stop alert."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
import pytz

from monitoring.alerts.base_alert import Issue
from monitoring.alerts.queue_stop_alert import (
    KIND_HANDOFF,
    KIND_STOP,
    QueueStopAlert,
    QueueStopIssue,
    RecentFile,
    detect,
    load_recent_files,
    render_issue,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
MAX_GRADIENT = timedelta(hours=2)
MULTIPLIER = 3
GRADIENT = timedelta(minutes=30)


def _file(name: str, created_at: datetime, initials: str | None) -> RecentFile:
    return RecentFile(name, created_at, initials)


def _detect(files: list[RecentFile], now: datetime = NOW) -> QueueStopIssue | None:
    return detect("inst1", files, now, MAX_GRADIENT, MULTIPLIER)


class TestStopRule:
    """Rule A: the newest two files share initials and the pause exceeds 3 x gradient."""

    def test_fires_when_pause_exceeds_threshold(self) -> None:
        """A pause of 3.5 x gradient is a stop for the newest file."""
        # given
        files = [
            _file("x_MaSc_a.raw", NOW - 3.5 * GRADIENT, "MaSc"),
            _file("x_MaSc_b.raw", NOW - 4.5 * GRADIENT, "MaSc"),
        ]

        # when
        result = _detect(files)

        # then
        assert result == QueueStopIssue(
            KIND_STOP, "inst1", "x_MaSc_a.raw", GRADIENT, 3.5 * GRADIENT, tuple(files)
        )

    def test_no_stop_when_pause_is_below_threshold(self) -> None:
        """A pause of 2 x gradient is not a stop yet."""
        files = [
            _file("x_MaSc_a.raw", NOW - 2 * GRADIENT, "MaSc"),
            _file("x_MaSc_b.raw", NOW - 3 * GRADIENT, "MaSc"),
        ]
        assert _detect(files) is None

    def test_no_stop_when_gradient_exceeds_max(self) -> None:
        """A gap above the max gradient is a new queue, not a stop."""
        files = [
            _file("x_MaSc_a.raw", NOW - timedelta(hours=10), "MaSc"),
            _file("x_MaSc_b.raw", NOW - timedelta(hours=13), "MaSc"),
        ]
        assert _detect(files) is None

    def test_no_stop_when_gradient_is_not_positive(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Equal timestamps (clock skew, duplicates) are logged and skipped."""
        # given
        same = NOW - timedelta(hours=5)
        files = [
            _file("x_MaSc_a.raw", same, "MaSc"),
            _file("x_MaSc_b.raw", same, "MaSc"),
        ]

        # when
        with caplog.at_level("WARNING"):
            result = _detect(files)

        # then
        assert result is None
        assert any("non-positive gradient" in rec.message for rec in caplog.records)

    def test_no_detection_with_fewer_than_two_files(self) -> None:
        """One file is not a queue."""
        assert _detect([_file("x_MaSc_a.raw", NOW - timedelta(days=1), "MaSc")]) is None


PRIOR = [
    _file("x_MaSc_a.raw", NOW - timedelta(hours=1), "MaSc"),
    _file("x_MaSc_b.raw", NOW - timedelta(hours=1, minutes=30), "MaSc"),
]


class TestHandoffRule:
    """Rule B: the prior pair shares initials, the newest file differs."""

    @pytest.mark.parametrize("newest_initials", ["JoeB", None])
    def test_fires_for_the_prior_users_last_file(
        self, newest_initials: str | None
    ) -> None:
        """A known other user or an unattributable file on top ends the prior queue."""
        # given
        files = [
            _file("x_n.raw", NOW - timedelta(minutes=5), newest_initials),
            *PRIOR,
        ]

        # when
        result = _detect(files)

        # then
        assert result == QueueStopIssue(
            KIND_HANDOFF, "inst1", "x_MaSc_a.raw", GRADIENT, None, tuple(files)
        )

    def test_no_handoff_with_only_two_files(self) -> None:
        """Without a third file the prior user's queue cannot be identified."""
        files = [
            _file("x_JoeB_n.raw", NOW - timedelta(minutes=5), "JoeB"),
            PRIOR[0],
        ]
        assert _detect(files) is None

    @pytest.mark.parametrize(
        "prior_initials", [("MaSc", "JoeB"), (None, None), ("MaSc", None)]
    )
    def test_no_handoff_when_prior_pair_does_not_share_known_initials(
        self, prior_initials: tuple[str | None, str | None]
    ) -> None:
        """A single-file prior run or unknown initials are not a queue to end."""
        files = [
            _file("x_JoeB_n.raw", NOW - timedelta(minutes=5), "JoeB"),
            _file("x_a.raw", NOW - timedelta(hours=1), prior_initials[0]),
            _file("x_b.raw", NOW - timedelta(hours=2), prior_initials[1]),
        ]
        assert _detect(files) is None

    def test_no_handoff_when_prior_gap_exceeds_max_gradient(self) -> None:
        """A prior pair further apart than the max gradient was not one queue."""
        files = [
            _file("x_JoeB_n.raw", NOW - timedelta(minutes=5), "JoeB"),
            _file("x_MaSc_a.raw", NOW - timedelta(hours=1), "MaSc"),
            _file("x_MaSc_b.raw", NOW - timedelta(hours=4), "MaSc"),
        ]
        assert _detect(files) is None


def test_rules_are_mutually_exclusive() -> None:
    """For any initials triplet with a long pause, at most one rule fires."""
    # given
    long_pause_ago = NOW - timedelta(hours=10)
    for t1 in ("MaSc", "JoeB", None):
        for t2 in ("MaSc", "JoeB", None):
            for t3 in ("MaSc", "JoeB", None):
                files = [
                    _file("f1", long_pause_ago, t1),
                    _file("f2", long_pause_ago - GRADIENT, t2),
                    _file("f3", long_pause_ago - 2 * GRADIENT, t3),
                ]

                # when
                result = _detect(files)

                # then - the stop rule takes precedence, the handoff needs a different top
                if t1 == t2 and t1 is not None:
                    expected_kind = KIND_STOP
                elif t2 == t3 and t2 is not None:
                    expected_kind = KIND_HANDOFF
                else:
                    expected_kind = None
                assert (result.kind if result else None) == expected_kind, (t1, t2, t3)


class TestLoadRecentFiles:
    """Loading of the newest files per instrument with the operator's initials."""

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_loads_three_newest_files_with_known_initials(
        self, mock_rawfile: Mock
    ) -> None:
        """Files are loaded newest first; a token behind another one is still found."""
        # given
        mock_rawfile.objects.distinct.return_value = ["inst1"]
        raw_files = [
            MagicMock(
                id="20260819_OA5_SA_AnSc_01.raw", created_at=NOW.replace(tzinfo=None)
            ),
            MagicMock(
                id="20260819_OA5_AnSc_02.raw", created_at=NOW - timedelta(minutes=4)
            ),
            MagicMock(id="QC_03.raw", created_at=NOW - timedelta(minutes=8)),
        ]
        mock_rawfile.objects.filter.return_value.only.return_value.order_by.return_value.limit.return_value = raw_files

        # when
        result = load_recent_files({"AnSc"})

        # then
        assert result == {
            "inst1": [
                RecentFile("20260819_OA5_SA_AnSc_01.raw", NOW, "AnSc"),
                RecentFile(
                    "20260819_OA5_AnSc_02.raw", NOW - timedelta(minutes=4), "AnSc"
                ),
                RecentFile("QC_03.raw", NOW - timedelta(minutes=8), None),
            ]
        }
        mock_rawfile.objects.filter.assert_called_once_with(instrument_id="inst1")
        mock_rawfile.objects.filter.return_value.only.return_value.order_by.return_value.limit.assert_called_once_with(
            3
        )


class TestQueueStopAlert:
    """Issues of the alert."""

    def test_is_reported_once_per_subject_file(self) -> None:
        """Stop and handoff share the key, so the same user-last-file notifies once."""
        assert QueueStopAlert().cooldown_minutes is None

    @patch("monitoring.alerts.queue_stop_alert.load_recent_files")
    @patch(
        "monitoring.alerts.queue_stop_alert.get_messenger_ids_by_initials",
        return_value={},
    )
    def test_no_issues_without_users_with_a_messenger_id(
        self, mock_known: Mock, mock_load: Mock
    ) -> None:
        """Without users that can be notified nothing is loaded."""
        # when / then
        assert QueueStopAlert()._get_issues([], NOW) == []
        mock_known.assert_called_once_with()
        mock_load.assert_not_called()

    @patch("monitoring.alerts.queue_stop_alert.MAX_GRADIENT_LENGTH_HOURS", 2)
    @patch("monitoring.alerts.queue_stop_alert.QUEUE_STOP_THRESHOLD_MULTIPLIER", 3)
    @patch("monitoring.alerts.queue_stop_alert.load_recent_files")
    @patch(
        "monitoring.alerts.queue_stop_alert.get_messenger_ids_by_initials",
        return_value={"MaSc": "U_MASC"},
    )
    def test_one_issue_per_instrument_with_a_queue_stop(
        self,
        mock_known: Mock,  # noqa: ARG002
        mock_load: Mock,
    ) -> None:
        """A detected stop becomes an issue about the subject file."""
        # given
        mock_load.return_value = {
            "inst1": [
                _file("x_MaSc_a.raw", NOW - 4 * GRADIENT, "MaSc"),
                _file("x_MaSc_b.raw", NOW - 5 * GRADIENT, "MaSc"),
            ],
            "inst2": [_file("x_MaSc_c.raw", NOW, "MaSc")],
        }

        # when
        result = QueueStopAlert()._get_issues([], NOW)

        # then
        mock_load.assert_called_once_with({"MaSc": "U_MASC"})
        assert len(result) == 1
        issue = result[0]
        assert issue.identifier == "inst1:x_MaSc_a.raw"
        assert issue.relevant_file == "x_MaSc_a.raw"
        assert issue.instrument_id == "inst1"
        assert issue.details.kind == KIND_STOP


def _build_stop_issue() -> QueueStopIssue:
    return QueueStopIssue(
        KIND_STOP,
        "inst1",
        "x_MaSc_a.raw",
        timedelta(minutes=30),
        timedelta(minutes=120),
        (
            _file("x_MaSc_a.raw", NOW, "MaSc"),
            _file("x_MaSc_b.raw", NOW - timedelta(minutes=30), "MaSc"),
        ),
    )


def _build_handoff_issue() -> QueueStopIssue:
    return QueueStopIssue(
        KIND_HANDOFF,
        "inst1",
        "x_MaSc_a.raw",
        timedelta(minutes=30),
        None,
        (
            _file("x_JoeB_n.raw", NOW, "JoeB"),
            _file("x_MaSc_a.raw", NOW - timedelta(hours=1), "MaSc"),
        ),
    )


class TestRendering:
    """Rendering of issues to messages."""

    def test_render_issue_distinguishes_stop_and_handoff(self) -> None:
        """Stop and handoff messages have different headers."""
        # when
        stop_msg = render_issue(_build_stop_issue(), {})
        handoff_msg = render_issue(_build_handoff_issue(), {})

        # then
        assert stop_msg.startswith(
            "Queue stopped on `inst1`: gradient ~30 min, pause 120 min."
        )
        assert handoff_msg.startswith("Queue handoff on `inst1`")

    def test_render_issue_lists_files_with_size_and_timestamp(self) -> None:
        """Each `Recent files:` row carries the file id, size and creation timestamp."""
        # when
        msg = render_issue(_build_stop_issue(), {"x_MaSc_a.raw": 1024**3})

        # then
        assert msg.endswith(
            "Recent files:\n"
            "- `x_MaSc_a.raw` (1.00 GB) [2026-01-01 12:00 UTC]\n"
            "- `x_MaSc_b.raw` (n/a) [2026-01-01 11:30 UTC]"
        )

    @patch("monitoring.alerts.queue_stop_alert.RawFile")
    def test_format_message_renders_each_issue_with_current_sizes(
        self, mock_rawfile: Mock
    ) -> None:
        """Sizes are read once for all listed files, right before rendering."""
        # given
        stop, handoff = _build_stop_issue(), _build_handoff_issue()
        mock_rawfile.objects.filter.return_value.only.return_value = [
            MagicMock(id="x_MaSc_a.raw", size=42 * 1024**3)
        ]

        # when
        result = QueueStopAlert().format_message(
            [Issue("inst1:x_MaSc_a.raw", stop), Issue("inst1:x_MaSc_a.raw", handoff)]
        )

        # then
        mock_rawfile.objects.filter.assert_called_once_with(
            id__in=["x_JoeB_n.raw", "x_MaSc_a.raw", "x_MaSc_b.raw"]
        )
        sizes = {"x_MaSc_a.raw": 42 * 1024**3}
        assert result == render_issue(stop, sizes) + "\n\n" + render_issue(
            handoff, sizes
        )
        assert "(42.00 GB)" in result
