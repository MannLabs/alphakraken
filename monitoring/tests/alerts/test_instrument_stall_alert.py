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
def test_stall_should_be_unique_per_last_file(mock_rawfile: Mock) -> None:
    """A stall is reported once per last file of the instrument."""
    # given
    alert = InstrumentStallAlert()
    _install_latest_file(
        mock_rawfile, "x_MaSc_a.raw", datetime.now(pytz.UTC) - timedelta(hours=3)
    )

    # when
    result = alert._get_issues([])

    # then
    assert len(result) == 1
    assert result[0].identifier == "inst1"
    assert result[0].relevant_file == "x_MaSc_a.raw"
    assert result[0].unique_key == ("inst1", "x_MaSc_a.raw")
