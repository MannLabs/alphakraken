"""Unit tests for PumpPressureAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.config import Cases
from monitoring.alerts.pump_pressure_alert import PressureDataPoint, PumpPressureAlert
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
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 100.0},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 110.0},
            },
            "file3": {
                "instrument_id": "instrument2",
                "created_at": now - timedelta(hours=2),
                "metrics_alphadia": {"gradient_length": 0.6},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 95.0},
            },
        }

        # when
        result = alert._get_pressure_data_by_instrument(raw_files_with_metrics)

        # then
        expected = {
            "instrument1": [
                PressureDataPoint(100.0, 0.5, now, "file1"),
                PressureDataPoint(110.0, 0.5, now - timedelta(hours=1), "file2"),
            ],
            "instrument2": [
                PressureDataPoint(95.0, 0.6, now - timedelta(hours=2), "file3"),
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
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 110.0},
            },
        }

        # when
        result = alert._get_pressure_data_by_instrument(raw_files_with_metrics)

        # then
        expected = {
            "instrument1": [
                PressureDataPoint(110.0, 0.5, now - timedelta(hours=1), "file2"),
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
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 110.0},
            },
        }

        # when
        result = alert._get_pressure_data_by_instrument(raw_files_with_metrics)

        # then
        expected = {
            "instrument1": [
                PressureDataPoint(110.0, 0.5, now - timedelta(hours=1), "file2"),
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
            PressureDataPoint(125.0, 0.5, now, "file1"),
            PressureDataPoint(124.0, 0.5, now - timedelta(hours=1), "file2"),
            PressureDataPoint(123.0, 0.5, now - timedelta(hours=2), "file3"),
            PressureDataPoint(122.0, 0.5, now - timedelta(hours=3), "file4"),
            PressureDataPoint(121.0, 0.5, now - timedelta(hours=4), "file5"),
            PressureDataPoint(120.0, 0.5, now - timedelta(hours=5), "file6"),
            PressureDataPoint(100.0, 0.5, now - timedelta(hours=6), "file7"),
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
            PressureDataPoint(110.0, 0.5, now, "file1"),
            PressureDataPoint(109.0, 0.5, now - timedelta(hours=1), "file2"),
            PressureDataPoint(108.0, 0.5, now - timedelta(hours=2), "file3"),
            PressureDataPoint(107.0, 0.5, now - timedelta(hours=3), "file4"),
            PressureDataPoint(106.0, 0.5, now - timedelta(hours=4), "file5"),
            PressureDataPoint(105.0, 0.5, now - timedelta(hours=5), "file6"),
            PressureDataPoint(100.0, 0.5, now - timedelta(hours=6), "file7"),
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
            PressureDataPoint(125.0, 0.5, now, "file1"),
            PressureDataPoint(124.0, 0.7, now - timedelta(hours=1), "file2"),
            PressureDataPoint(123.0, 0.5, now - timedelta(hours=2), "file3"),
            PressureDataPoint(122.0, 0.5, now - timedelta(hours=3), "file4"),
            PressureDataPoint(121.0, 0.5, now - timedelta(hours=4), "file5"),
            PressureDataPoint(120.0, 0.5, now - timedelta(hours=5), "file6"),
            PressureDataPoint(100.0, 0.5, now - timedelta(hours=6), "file7"),
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
            PressureDataPoint(125.0, 0.5, now, "file1"),
            PressureDataPoint(124.0, 0.5, now - timedelta(hours=1), "file2"),
            PressureDataPoint(123.0, 0.5, now - timedelta(hours=2), "file3"),
            PressureDataPoint(122.0, 0.5, now - timedelta(hours=3), "file4"),
            PressureDataPoint(121.0, 0.5, now - timedelta(hours=4), "file5"),
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        assert is_alert is False
        assert pressure_changes == []

    def test_detect_pressure_increase_should_not_compare_significantly_different_gradient_lengths(
        self,
    ) -> None:
        """Test that detect_pressure_increase does not compare files with significantly different gradient lengths (e.g., 2 min vs 16 min)."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        # Files with very different gradient lengths (2 min vs 16 min)
        # Even though there's a large pressure increase between different gradient files, they should not be compared
        pressure_data = [
            PressureDataPoint(150.0, 16.0, now, "file1"),  # 16 min, high pressure
            PressureDataPoint(
                148.0, 16.0, now - timedelta(hours=1), "file2"
            ),  # 16 min, small change
            PressureDataPoint(
                146.0, 16.0, now - timedelta(hours=2), "file3"
            ),  # 16 min, small change
            PressureDataPoint(
                144.0, 16.0, now - timedelta(hours=3), "file4"
            ),  # 16 min, small change
            PressureDataPoint(
                142.0, 16.0, now - timedelta(hours=4), "file5"
            ),  # 16 min, small change
            PressureDataPoint(
                140.0, 16.0, now - timedelta(hours=5), "file6"
            ),  # 16 min, small change
            PressureDataPoint(
                110.0, 2.0, now - timedelta(hours=6), "file7"
            ),  # 2 min, low pressure
        ]

        # when
        is_alert, pressure_changes = alert._detect_pressure_increase(
            pressure_data, window_size=5, threshold=20
        )

        # then
        # Should not detect alert because:
        # - file1 (150 bar, 16 min) vs file6 (140 bar, 16 min) = 10 bar change (below threshold)
        # - file2 (148 bar, 16 min) vs file7 (110 bar, 2 min) = 38 bar change but should be filtered out
        #   because gradient lengths differ by 8x (2 min vs 16 min), which is > 10% tolerance
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
            PressureDataPoint(120.0, 0.5, now, "file1"),
            PressureDataPoint(115.0, 0.5, now - timedelta(hours=1), "file2"),
            PressureDataPoint(110.0, 0.5, now - timedelta(hours=2), "file3"),
            PressureDataPoint(105.0, 0.5, now - timedelta(hours=3), "file4"),
            PressureDataPoint(100.0, 0.5, now - timedelta(hours=4), "file5"),
            PressureDataPoint(100.0, 0.5, now - timedelta(hours=5), "file6"),
            PressureDataPoint(100.0, 0.5, now - timedelta(hours=6), "file7"),
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
            PressureDataPoint(80.0, 0.5, now, "file1"),
            PressureDataPoint(85.0, 0.5, now - timedelta(hours=1), "file2"),
            PressureDataPoint(90.0, 0.5, now - timedelta(hours=2), "file3"),
            PressureDataPoint(95.0, 0.5, now - timedelta(hours=3), "file4"),
            PressureDataPoint(100.0, 0.5, now - timedelta(hours=4), "file5"),
            PressureDataPoint(105.0, 0.5, now - timedelta(hours=5), "file6"),
            PressureDataPoint(110.0, 0.5, now - timedelta(hours=6), "file7"),
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
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 125.0},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 124.0},
            },
            "file3": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=2),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 123.0},
            },
            "file4": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=3),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 122.0},
            },
            "file5": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=4),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 121.0},
            },
            "file6": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=5),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 120.0},
            },
            "file7": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=6),
                "metrics_alphadia": {"gradient_length": 0.5},
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

    @patch(
        "monitoring.alerts.pump_pressure_alert.BUSINESS_ALERTS_WEBHOOK_URL",
        "https://hooks.slack.com/custom",
    )
    def test_get_webhook_url_should_return_custom_url_when_configured(self) -> None:
        """Test that get_webhook_url returns custom URL when env var is set."""
        # given
        alert = PumpPressureAlert()

        # when
        result = alert.get_webhook_url()

        # then
        assert result == "https://hooks.slack.com/custom"

    @patch(
        "monitoring.alerts.pump_pressure_alert.BUSINESS_ALERTS_WEBHOOK_URL",
        "http://test-webhook.example.com",
    )
    def test_get_webhook_url_should_return_default_when_not_configured(self) -> None:
        """Test that get_webhook_url returns default webhook when env var is not set."""
        # given
        alert = PumpPressureAlert()

        # when
        result = alert.get_webhook_url()

        # then
        assert result == "http://test-webhook.example.com"

    def test_detect_high_absolute_pressure_should_detect_pressure_above_threshold(
        self,
    ) -> None:
        """Test that detect_high_absolute_pressure detects pressure >= 480 bar."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            PressureDataPoint(485.0, 0.5, now, "file1"),
            PressureDataPoint(480.0, 0.5, now - timedelta(hours=1), "file2"),
            PressureDataPoint(475.0, 0.5, now - timedelta(hours=2), "file3"),
        ]

        # when
        result = alert._detect_high_absolute_pressure(pressure_data, threshold=480)

        # then
        assert len(result) == 2
        assert result[0] == (485.0, "file1", now)
        assert result[1] == (480.0, "file2", now - timedelta(hours=1))

    def test_detect_high_absolute_pressure_should_not_detect_pressure_below_threshold(
        self,
    ) -> None:
        """Test that detect_high_absolute_pressure does not detect pressure below 480 bar."""
        # given
        alert = PumpPressureAlert()
        now = datetime.now(tz=pytz.utc)

        pressure_data = [
            PressureDataPoint(479.0, 0.5, now, "file1"),
            PressureDataPoint(450.0, 0.5, now - timedelta(hours=1), "file2"),
            PressureDataPoint(400.0, 0.5, now - timedelta(hours=2), "file3"),
        ]

        # when
        result = alert._detect_high_absolute_pressure(pressure_data, threshold=480)

        # then
        assert result == []

    @patch("monitoring.alerts.pump_pressure_alert.augment_raw_files_with_metrics")
    @patch("monitoring.alerts.pump_pressure_alert.RawFile")
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_LOOKBACK_DAYS", 1)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_WINDOW_SIZE", 5)
    @patch("monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_THRESHOLD_BAR", 20)
    @patch(
        "monitoring.alerts.pump_pressure_alert.PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR",
        480,
    )
    def test_get_issues_should_detect_both_increase_and_absolute_threshold(
        self,
        mock_rawfile: Mock,
        mock_augment: Mock,
    ) -> None:
        """Test that get_issues can detect both pressure increase and absolute threshold violations."""
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

        # Mock data with both high absolute pressure AND pressure increase
        mock_augment.return_value = {
            "file1": {
                "instrument_id": "instrument1",
                "created_at": now,
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 485.0},
            },
            "file2": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=1),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 484.0},
            },
            "file3": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=2),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 483.0},
            },
            "file4": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=3),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 482.0},
            },
            "file5": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=4),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 481.0},
            },
            "file6": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=5),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 480.0},
            },
            "file7": {
                "instrument_id": "instrument1",
                "created_at": now - timedelta(hours=6),
                "metrics_alphadia": {"gradient_length": 0.5},
                "metrics_msqc": {"msqc_evosep_pump_hp_pressure_max": 460.0},
            },
        }

        # when
        result = alert._get_issues(status_objects)

        # then
        # Should detect BOTH:
        # 1. Pressure increase (485 - 460 = 25 bar over window of 5)
        # 2. Multiple absolute threshold violations (485, 484, 483, 482, 481, 480 all >= 480)
        assert len(result) > 1
        instrument_ids = [issue[0] for issue in result]
        details = [issue[1] for issue in result]

        assert all(inst_id == "instrument1" for inst_id in instrument_ids)
        assert any("Pressure changes:" in detail for detail in details)
        assert any("High pressure:" in detail for detail in details)
