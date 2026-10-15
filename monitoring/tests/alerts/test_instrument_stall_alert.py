"""Unit tests for InstrumentStallAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.instrument_stall_alert import InstrumentStallAlert


def _install_latest_file(
    mock_rawfile: Mock, original_name: str, created_at: datetime
) -> None:
    latest = Mock(original_name=original_name, created_at=created_at)
    mock_rawfile.objects.filter.return_value.order_by.return_value.only.return_value.first.return_value = latest


@patch(
    "monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_INSTRUMENT_IDS",
    {"inst1"},
)
@patch("monitoring.alerts.instrument_stall_alert.INSTRUMENT_STALL_THRESHOLD_HOURS", 2)
@patch("monitoring.alerts.instrument_stall_alert.RawFile")
def test_stall_should_be_raised_until_reported(mock_rawfile: Mock) -> None:
    """A stall is raised again until marked as reported, then not for the same file."""
    # given
    alert = InstrumentStallAlert()
    _install_latest_file(
        mock_rawfile, "x_MaSc_a.raw", datetime.now(pytz.UTC) - timedelta(hours=3)
    )

    # when
    first = alert._get_issues([])
    unreported = alert._get_issues([])
    alert.mark_reported(first)
    reported = alert._get_issues([])

    # then
    assert len(first) == 1
    assert first[0].identifier == "inst1"
    assert first[0].relevant_file == "x_MaSc_a.raw"
    assert unreported == first
    assert reported == []
