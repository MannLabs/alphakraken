"""Unit tests for PumpPressureAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.config import Cases
from monitoring.alerts.pump_pressure_alert import PumpPressureAlert
from shared.db.models import KrakenStatusEntities


class TestPumpPressureAlert:
    """Test suite for PumpPressureAlert class."""

    def test_name_should_return_pump_pressure_increase_case(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = PumpPressureAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.PUMP_PRESSURE_INCREASE

    @patch("monitoring.alerts.pump_pressure_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.pump_pressure_alert.RawFile")
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_LOOKBACK_DAYS", 7)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_WINDOW_SIZE", 5)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_THRESHOLD_BAR", 20)
    def test_get_issues_should_return_empty_when_no_instruments(
        self,
        mock_rawfile: Mock,
        _mock_augment: Mock,  # noqa: PT019
    ) -> None:
        """Test that get_issues returns empty list when no instruments found."""
        # given
        alert = PumpPressureAlert()
        mock_job = Mock()
        mock_job.id = "file_remover"
        mock_job.entity_type = KrakenStatusEntities.JOB

        status_objects = [mock_job]

        # when
        result = alert._get_issues(status_objects)

        # then
        assert result == []
        mock_rawfile.objects.filter.assert_not_called()

    @patch("monitoring.alerts.pump_pressure_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.pump_pressure_alert.RawFile")
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_LOOKBACK_DAYS", 7)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_WINDOW_SIZE", 5)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_THRESHOLD_BAR", 20)
    def test_get_issues_should_return_empty_when_no_raw_files(
        self,
        mock_rawfile: Mock,
        _mock_augment: Mock,  # noqa: PT019
    ) -> None:
        """Test that get_issues returns empty list when no raw files found in lookback window."""
        # given
        alert = PumpPressureAlert()
        mock_instrument = Mock()
        mock_instrument.id = "instrument1"
        mock_instrument.entity_type = KrakenStatusEntities.INSTRUMENT

        status_objects = [mock_instrument]

        mock_query = Mock()
        mock_query.only.return_value.order_by.return_value = []
        mock_rawfile.objects.filter.return_value = mock_query

        # when
        result = alert._get_issues(status_objects)

        # then
        assert result == []
        _mock_augment.assert_not_called()

    def test_get_pressure_data_by_instrument_should_group_metrics_correctly(
        self,
    ) -> None:
        """Test that get_pressure_data_by_instrument groups metrics by instrument."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        raw_files_with_metrics = {
            "file1": {
                "instrument_id": "instrument1",
                "created_at": now,
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 100.0},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 110.0},
            },
            "file3": {
                "instrument_id": "instrument2",
                "created_at": now - timedelta(hours=2),
                "metrics_alphadia": {"raw:gradient_length_m": 0.6},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 95.0},
            },
        }

        # when
        result = alert._get_pressure_data_by_instrument(raw_files_with_metrics)

        # then
        expected = {
            "instrument1": [
                (100.0, 0.5, now),
                (110.0, 0.5, now - timedelta(hours=1)),
            ],
            "instrument2": [
                (95.0, 0.6, now - timedelta(hours=2)),
            ],
        }
        assert result == expected

    def test_get_pressure_data_by_instrument_should_skip_entries_with_missing_gradient_length(
        self,
    ) -> None:
        """Test that get_pressure_data_by_instrument skips entries with missing gradient length."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        raw_files_with_metrics = {
            "file1": {
                "instrument_id": "instrument1",
                "created_at": now,
                "metrics_alphadia": {},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 100.0},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 110.0},
            },
        }

        # when
        result = alert._get_pressure_data_by_instrument(raw_files_with_metrics)

        # then
        expected = {
            "instrument1": [
                (110.0, 0.5, now - timedelta(hours=1)),
            ],
        }
        assert result == expected

    def test_get_pressure_data_by_instrument_should_skip_entries_with_missing_pressure(
        self,
    ) -> None:
        """Test that get_pressure_data_by_instrument skips entries with missing pressure."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        raw_files_with_metrics = {
            "file1": {
                "instrument_id": "instrument1",
                "created_at": now,
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 110.0},
            },
        }

        # when
        result = alert._get_pressure_data_by_instrument(raw_files_with_metrics)

        # then
        expected = {
            "instrument1": [
                (110.0, 0.5, now - timedelta(hours=1)),
            ],
        }
        assert result == expected

    def test_get_pressure_data_by_instrument_should_return_empty_when_all_metrics_missing(
        self,
    ) -> None:
        """Test that get_pressure_data_by_instrument returns empty dict when all metrics are missing."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        raw_files_with_metrics = {
            "file1": {
                "instrument_id": "instrument1",
                "created_at": now,
                "metrics_alphadia": {},
                "metrics_msqc": {},
            }
        }

        # when
        result = alert._get_pressure_data_by_instrument(raw_files_with_metrics)

        # then
        assert result == {}

    def test_detect_pressure_increase_should_detect_pressure_increase_above_threshold(
        self,
    ) -> None:
        """Test that detect_pressure_increase detects pressure increase when exceeding threshold."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            (125.0, 0.5, now),
            (124.0, 0.5, now - timedelta(hours=1)),
            (123.0, 0.5, now - timedelta(hours=2)),
            (122.0, 0.5, now - timedelta(hours=3)),
            (121.0, 0.5, now - timedelta(hours=4)),
            (120.0, 0.5, now - timedelta(hours=5)),
            (100.0, 0.5, now - timedelta(hours=6)),
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        assert is_alert is True
        assert len(pressure_changes) > 0
        assert any(change[0] > 20 for change in pressure_changes)

    def test_detect_pressure_increase_should_not_detect_when_below_threshold(
        self,
    ) -> None:
        """Test that detect_pressure_increase does not detect when pressure increase is below threshold."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            (110.0, 0.5, now),
            (109.0, 0.5, now - timedelta(hours=1)),
            (108.0, 0.5, now - timedelta(hours=2)),
            (107.0, 0.5, now - timedelta(hours=3)),
            (106.0, 0.5, now - timedelta(hours=4)),
            (105.0, 0.5, now - timedelta(hours=5)),
            (100.0, 0.5, now - timedelta(hours=6)),
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        assert is_alert is False

    def test_detect_pressure_increase_should_filter_by_gradient_length_tolerance(
        self,
    ) -> None:
        """Test that detect_pressure_increase filters out data points outside gradient length tolerance."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            (
                125.0,
                0.5,
                now,
            ),
            (
                124.0,
                0.7,
                now - timedelta(hours=1),
            ),
            (
                123.0,
                0.5,
                now - timedelta(hours=2),
            ),
            (122.0, 0.5, now - timedelta(hours=3)),
            (121.0, 0.5, now - timedelta(hours=4)),
            (120.0, 0.5, now - timedelta(hours=5)),
            (100.0, 0.5, now - timedelta(hours=6)),
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        assert len(pressure_changes) < 2

    def test_detect_pressure_increase_should_handle_insufficient_data_points(
        self,
    ) -> None:
        """Test that detect_pressure_increase handles insufficient data points by returning no alerts."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            (125.0, 0.5, now),
            (124.0, 0.5, now - timedelta(hours=1)),
            (123.0, 0.5, now - timedelta(hours=2)),
            (122.0, 0.5, now - timedelta(hours=3)),
            (121.0, 0.5, now - timedelta(hours=4)),
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        assert is_alert is False
        assert pressure_changes == []

    def test_detect_pressure_increase_should_not_detect_at_exact_threshold(
        self,
    ) -> None:
        """Test that detect_pressure_increase does not detect when pressure increase equals threshold (uses strict greater than)."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            (120.0, 0.5, now),
            (115.0, 0.5, now - timedelta(hours=1)),
            (110.0, 0.5, now - timedelta(hours=2)),
            (105.0, 0.5, now - timedelta(hours=3)),
            (100.0, 0.5, now - timedelta(hours=4)),
            (100.0, 0.5, now - timedelta(hours=5)),
            (100.0, 0.5, now - timedelta(hours=6)),
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        assert is_alert is False
        assert pressure_changes == []

    def test_detect_pressure_increase_should_handle_pressure_decrease(self) -> None:
        """Test that detect_pressure_increase does not alert when pressure decreases."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            (80.0, 0.5, now),
            (85.0, 0.5, now - timedelta(hours=1)),
            (90.0, 0.5, now - timedelta(hours=2)),
            (95.0, 0.5, now - timedelta(hours=3)),
            (100.0, 0.5, now - timedelta(hours=4)),
            (105.0, 0.5, now - timedelta(hours=5)),
            (110.0, 0.5, now - timedelta(hours=6)),
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        assert is_alert is False
        assert all(change[0] <= 0 for change in pressure_changes)

    def test_format_message_should_format_single_issue(self) -> None:
        """Test that format_message formats single issue correctly."""
        # given
        alert = PumpPressureAlert()
        issues = [("instrument1", "Pressure changes: [21.5, 22.3, 23.1]")]

        # when
        result = alert.format_message(issues)

        # then
        expected = "Pump pressure increase detected:\n- `instrument1`: Pressure changes: [21.5, 22.3, 23.1]"
        assert result == expected

    def test_format_message_should_format_multiple_issues(self) -> None:
        """Test that format_message formats multiple issues correctly."""
        # given
        alert = PumpPressureAlert()
        issues = [
            ("instrument1", "Pressure changes: [21.5, 22.3]"),
            ("instrument2", "Pressure changes: [25.0]"),
            ("instrument3", "Pressure changes: [30.2, 31.5, 32.8]"),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Pump pressure increase detected:\n"
            "- `instrument1`: Pressure changes: [21.5, 22.3]\n"
            "- `instrument2`: Pressure changes: [25.0]\n"
            "- `instrument3`: Pressure changes: [30.2, 31.5, 32.8]"
        )
        assert result == expected

    @patch("monitoring.alerts.pump_pressure_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.pump_pressure_alert.RawFile")
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_LOOKBACK_DAYS", 7)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_WINDOW_SIZE", 5)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_THRESHOLD_BAR", 20)
    def test_memory_should_suppress_duplicate_issues(
        self,
        mock_rawfile: Mock,
        mock_augment: Mock,
    ) -> None:
        """Test that memory suppresses duplicate issues with same instrument_id and pressure changes."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        mock_instrument = Mock()
        mock_instrument.id = "instrument1"
        mock_instrument.entity_type = KrakenStatusEntities.INSTRUMENT
        status_objects = [mock_instrument]

        mock_raw_file = Mock()
        mock_raw_file.id = "file1"
        mock_raw_file.instrument_id = "instrument1"
        mock_raw_file.created_at = now

        mock_query = Mock()
        mock_query.only.return_value.order_by.return_value = [mock_raw_file]
        mock_rawfile.objects.filter.return_value = mock_query

        # Mock augment to return pressure data that triggers an alert
        mock_augment.return_value = {
            "file1": {
                "instrument_id": "instrument1",
                "created_at": now,
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 125.0},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 124.0},
            },
            "file3": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=2),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 123.0},
            },
            "file4": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=3),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 122.0},
            },
            "file5": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=4),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 121.0},
            },
            "file6": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=5),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 120.0},
            },
            "file7": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=6),
                "metrics_alphadia": {"raw:gradient_length_m": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 100.0},
            },
        }

        # when - first call should return the issue
        result_first = alert._get_issues(status_objects)

        # then - first call returns the issue
        assert len(result_first) == 1
        assert result_first[0][0] == "instrument1"
        assert "Pressure changes:" in result_first[0][1]

        # when - second call with same data should suppress the issue
        result_second = alert._get_issues(status_objects)

        # then - second call suppresses the duplicate
        assert result_second == []
