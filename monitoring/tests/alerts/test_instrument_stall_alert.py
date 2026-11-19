"""Unit tests for InstrumentStallAlert."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
import pytz

from monitoring.alerts.instrument_stall_alert import InstrumentStallAlert
from shared.db.raw_files import RawFileFilter
from shared.domain.raw_files import RawFile
from shared.tests.factories import raw_file

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=pytz.UTC)


class TestInstrumentStallAlert:
    """Issues of the stall alert."""

    def test_is_reported_once_per_stall(self) -> None:
        """A stall is reported once per last file."""
        assert InstrumentStallAlert().cooldown_minutes is None

    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_INSTRUMENT_IDS",
        set(),
    )
    @patch("shared.db.raw_files.find")
    def test_disabled_without_configured_instruments(self, mock_find: Mock) -> None:
        """Without configured instruments nothing is queried."""
        assert InstrumentStallAlert()._get_issues([], NOW) == []
        mock_find.assert_not_called()

    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_THRESHOLD_HOURS", 2
    )
    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_INSTRUMENT_IDS",
        {"inst1"},
    )
    @patch("shared.db.raw_files.find")
    def test_stalled_instrument_yields_an_issue_keyed_by_last_file(
        self, mock_find: Mock
    ) -> None:
        """An instrument without a new file for longer than the threshold is stalled."""
        # given
        mock_find.return_value = [
            raw_file(original_name="last.raw", created_at=NOW - timedelta(hours=3))
        ]

        # when
        result = InstrumentStallAlert()._get_issues([], NOW)

        # then
        mock_find.assert_called_once_with(RawFileFilter(instrument_id="inst1"), limit=1)
        assert len(result) == 1
        issue = result[0]
        assert issue.identifier == "inst1"
        assert issue.details == (
            'last file "last.raw" acquired at 2024-01-15 09:00:00 UTC (3.0 hours ago)'
        )
        assert issue.relevant_file == "last.raw"
        assert issue.instrument_id == "inst1"
        assert issue.key == "inst1:last.raw"

    @pytest.mark.parametrize(
        "latest",
        [
            [],
            [raw_file(original_name="fresh.raw", created_at=NOW - timedelta(hours=1))],
        ],
    )
    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_THRESHOLD_HOURS", 2
    )
    @patch(
        "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_INSTRUMENT_IDS",
        {"inst1"},
    )
    @patch("shared.db.raw_files.find")
    def test_no_issue_without_files_or_with_a_recent_file(
        self, mock_find: Mock, latest: list[RawFile]
    ) -> None:
        """Instruments without files or with a recent file are not stalled."""
        # given
        mock_find.return_value = latest

        # when / then
        assert InstrumentStallAlert()._get_issues([], NOW) == []
