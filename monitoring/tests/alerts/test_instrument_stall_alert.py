"""Unit tests for InstrumentStallAlert."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
import pytz

from monitoring.alerts.instrument_stall_alert import InstrumentStallAlert

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=pytz.UTC)


def _latest(mock_rawfile: Mock, latest: Mock | None) -> None:
    mock_rawfile.objects.filter.return_value.order_by.return_value.only.return_value.first.return_value = latest


class TestInstrumentStallAlert:
    """Issues of the stall alert."""

    def test_is_reported_once_per_stall(self) -> None:
        """A stall is reported once per last file."""
        assert InstrumentStallAlert().cooldown_minutes is None

    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_INSTRUMENT_IDS",
        set(),
    )
    @patch("monitoring.alerts.instrument_stall_alert.RawFile")
    def test_disabled_without_configured_instruments(self, mock_rawfile: Mock) -> None:
        """Without configured instruments nothing is queried."""
        assert InstrumentStallAlert()._get_issues([], NOW) == []
        mock_rawfile.objects.filter.assert_not_called()

    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_THRESHOLD_HOURS", 2
    )
    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_INSTRUMENT_IDS",
        {"inst1"},
    )
    @patch("monitoring.alerts.instrument_stall_alert.RawFile")
    def test_stalled_instrument_yields_an_issue_keyed_by_last_file(
        self, mock_rawfile: Mock
    ) -> None:
        """An instrument without a new file for longer than the threshold is stalled."""
        # given - naive timestamps from the DB are UTC
        created_at = (NOW - timedelta(hours=3)).replace(tzinfo=None)
        _latest(mock_rawfile, Mock(original_name="last.raw", created_at=created_at))

        # when
        result = InstrumentStallAlert()._get_issues([], NOW)

        # then
        mock_rawfile.objects.filter.assert_called_once_with(instrument_id="inst1")
        assert len(result) == 1
        issue = result[0]
        assert issue.identifier == "inst1"
        assert issue.details == NOW - timedelta(hours=3)
        assert issue.relevant_file == "last.raw"
        assert issue.instrument_id == "inst1"
        assert issue.key == "inst1:last.raw"

    @pytest.mark.parametrize(
        "latest",
        [None, Mock(original_name="fresh.raw", created_at=NOW - timedelta(hours=1))],
    )
    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_THRESHOLD_HOURS", 2
    )
    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_INSTRUMENT_IDS",
        {"inst1"},
    )
    @patch("monitoring.alerts.instrument_stall_alert.RawFile")
    def test_no_issue_without_files_or_with_a_recent_file(
        self, mock_rawfile: Mock, latest: Mock | None
    ) -> None:
        """Instruments without files or with a recent file are not stalled."""
        # given
        _latest(mock_rawfile, latest)

        # when / then
        assert InstrumentStallAlert()._get_issues([], NOW) == []

    @patch("monitoring.alerts.instrument_stall_alert.datetime")
    def test_format_message_lists_last_file_and_age(self, mock_datetime: Mock) -> None:
        """The body names the last file, its acquisition time and age."""
        # given
        mock_datetime.now.return_value = NOW
        from monitoring.alerts.base_alert import Issue

        issue = Issue("inst1", NOW - timedelta(hours=3), relevant_file="last.raw")

        # when / then
        assert InstrumentStallAlert().format_message([issue]) == (
            '- `inst1`: last file "last.raw" acquired at 2024-01-15 09:00:00 UTC (3.0 hours ago)'
        )
