"""Unit tests for StaleStatusAlert class."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.config import Cases
from monitoring.alerts.stale_status_alert import StaleStatusAlert
from shared.db.models import KrakenStatusEntities


class TestStaleStatusAlert:
    """Test suite for StaleStatusAlert class."""

    def test_case_name_should_return_correct_value(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = StaleStatusAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.STALE

    @patch("monitoring.alerts.stale_status_alert.datetime")
    def test_check_should_handle_comprehensive_stale_scenarios(
        self, mock_datetime: Mock
    ) -> None:
        """Test that check handles complex mixed scenarios with different entity types, thresholds, and time ranges."""
        # given
        alert = StaleStatusAlert()
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = now

        # Create comprehensive mix of status objects with different scenarios

        # Fresh instruments (within 15-minute threshold)
        mock_instrument_fresh = Mock()
        mock_instrument_fresh.id = "instrument_active"
        mock_instrument_fresh.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_instrument_fresh.updated_at_ = datetime(
            2024, 1, 15, 11, 55, 0, tzinfo=pytz.UTC
        )  # 5 minutes ago

        # Stale instruments (beyond 15-minute threshold)
        mock_instrument_stale = Mock()
        mock_instrument_stale.id = "instrument_stale"
        mock_instrument_stale.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_instrument_stale.updated_at_ = datetime(
            2024, 1, 15, 11, 30, 0, tzinfo=pytz.UTC
        )  # 30 minutes ago

        mock_instrument_very_stale = Mock()
        mock_instrument_very_stale.id = "instrument_very_stale"
        mock_instrument_very_stale.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_instrument_very_stale.updated_at_ = datetime(
            2024, 1, 15, 10, 0, 0, tzinfo=pytz.UTC
        )  # 2 hours ago

        # Boundary case - exactly at threshold
        mock_instrument_boundary = Mock()
        mock_instrument_boundary.id = "instrument_boundary"
        mock_instrument_boundary.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_instrument_boundary.updated_at_ = datetime(
            2024, 1, 15, 11, 45, 0, tzinfo=pytz.UTC
        )  # Exactly 15 minutes ago

        # File systems with different staleness levels
        mock_filesystem_fresh = Mock()
        mock_filesystem_fresh.id = "backup_fresh"
        mock_filesystem_fresh.entity_type = KrakenStatusEntities.FILE_SYSTEM
        mock_filesystem_fresh.updated_at_ = datetime(
            2024, 1, 15, 11, 58, 0, tzinfo=pytz.UTC
        )  # 2 minutes ago

        mock_filesystem_stale = Mock()
        mock_filesystem_stale.id = "storage_stale"
        mock_filesystem_stale.entity_type = KrakenStatusEntities.FILE_SYSTEM
        mock_filesystem_stale.updated_at_ = datetime(
            2024, 1, 15, 11, 20, 0, tzinfo=pytz.UTC
        )  # 40 minutes ago

        # Regular job (15-minute threshold)
        mock_job_fresh = Mock()
        mock_job_fresh.id = "processor_job"
        mock_job_fresh.entity_type = KrakenStatusEntities.JOB
        mock_job_fresh.updated_at_ = datetime(
            2024, 1, 15, 11, 50, 0, tzinfo=pytz.UTC
        )  # 10 minutes ago

        mock_job_stale = Mock()
        mock_job_stale.id = "monitor_job"
        mock_job_stale.entity_type = KrakenStatusEntities.JOB
        mock_job_stale.updated_at_ = datetime(
            2024, 1, 15, 11, 25, 0, tzinfo=pytz.UTC
        )  # 35 minutes ago

        # File remover job (24-hour threshold)
        mock_file_remover_fresh = Mock()
        mock_file_remover_fresh.id = "file_remover"
        mock_file_remover_fresh.entity_type = KrakenStatusEntities.JOB
        mock_file_remover_fresh.updated_at_ = datetime(
            2024, 1, 14, 13, 30, 0, tzinfo=pytz.UTC
        )  # 22.5 hours ago (fresh for file_remover)

        mock_file_remover_stale = Mock()
        mock_file_remover_stale.id = "file_remover"
        mock_file_remover_stale.entity_type = KrakenStatusEntities.JOB
        mock_file_remover_stale.updated_at_ = datetime(
            2024, 1, 14, 10, 0, 0, tzinfo=pytz.UTC
        )  # 26 hours ago (stale for file_remover)

        status_objects = [
            mock_instrument_fresh,
            mock_instrument_stale,
            mock_instrument_very_stale,
            mock_instrument_boundary,
            mock_filesystem_fresh,
            mock_filesystem_stale,
            mock_job_fresh,
            mock_job_stale,
            mock_file_remover_stale,  # Only testing stale file_remover for comprehensive scenario
        ]

        # when
        result = alert.get_issues(status_objects)

        # then
        # Should return only stale entities, with proper timezone handling
        expected = [
            ("instrument_stale", datetime(2024, 1, 15, 11, 30, 0, tzinfo=pytz.UTC)),
            (
                "instrument_very_stale",
                datetime(2024, 1, 15, 10, 0, 0, tzinfo=pytz.UTC),
            ),
            ("storage_stale", datetime(2024, 1, 15, 11, 20, 0, tzinfo=pytz.UTC)),
            ("monitor_job", datetime(2024, 1, 15, 11, 25, 0, tzinfo=pytz.UTC)),
            ("file_remover", datetime(2024, 1, 14, 10, 0, 0, tzinfo=pytz.UTC)),
        ]
        assert result == expected

    @patch("monitoring.alerts.stale_status_alert.datetime")
    def test_format_message_should_handle_comprehensive_scenarios(
        self, mock_datetime: Mock
    ) -> None:
        """Test that format_message handles multiple issues with various entity types and time differences."""
        # given
        alert = StaleStatusAlert()
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = now

        # Complex mix of issues with different staleness levels
        issues = [
            (
                "instrument_main",
                datetime(2024, 1, 15, 10, 0, 0, tzinfo=pytz.UTC),
            ),  # 2.0 hours ago
            (
                "backup_system",
                datetime(2024, 1, 15, 9, 30, 0, tzinfo=pytz.UTC),
            ),  # 2.5 hours ago
            (
                "file_remover",
                datetime(2024, 1, 14, 10, 0, 0, tzinfo=pytz.UTC),
            ),  # 26.0 hours ago
            (
                "processor_job",
                datetime(2024, 1, 15, 11, 45, 0, tzinfo=pytz.UTC),
            ),  # 0.25 hours ago
            (
                "storage_server",
                datetime(2024, 1, 15, 11, 40, 0, tzinfo=pytz.UTC),
            ),  # 0.33 hours ago
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Health check status is stale:\n"
            "- `instrument_main`: 2024-01-15 10:00:00 UTC (2.0 hours ago)\n"
            "- `backup_system`: 2024-01-15 09:30:00 UTC (2.5 hours ago)\n"
            "- `file_remover`: 2024-01-14 10:00:00 UTC (26.0 hours ago)\n"
            "- `processor_job`: 2024-01-15 11:45:00 UTC (0.2 hours ago)\n"
            "- `storage_server`: 2024-01-15 11:40:00 UTC (0.3 hours ago)."
        )
        assert result == expected

    @patch("monitoring.alerts.stale_status_alert.datetime")
    def test_check_should_return_empty_when_all_fresh(
        self, mock_datetime: Mock
    ) -> None:
        """Test that check returns empty result when all status objects are fresh."""
        # given
        alert = StaleStatusAlert()
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = now

        # All fresh status objects across different entity types
        status_objects = []
        entity_configs = [
            (
                "instrument1",
                KrakenStatusEntities.INSTRUMENT,
                datetime(2024, 1, 15, 11, 50, 0, tzinfo=pytz.UTC),
            ),  # 10 min ago
            (
                "instrument2",
                KrakenStatusEntities.INSTRUMENT,
                datetime(2024, 1, 15, 11, 55, 0, tzinfo=pytz.UTC),
            ),  # 5 min ago
            (
                "backup",
                KrakenStatusEntities.FILE_SYSTEM,
                datetime(2024, 1, 15, 11, 58, 0, tzinfo=pytz.UTC),
            ),  # 2 min ago
            (
                "output",
                KrakenStatusEntities.FILE_SYSTEM,
                datetime(2024, 1, 15, 11, 52, 0, tzinfo=pytz.UTC),
            ),  # 8 min ago
            (
                "processor",
                KrakenStatusEntities.JOB,
                datetime(2024, 1, 15, 11, 47, 0, tzinfo=pytz.UTC),
            ),  # 13 min ago
            (
                "file_remover",
                KrakenStatusEntities.JOB,
                datetime(2024, 1, 14, 14, 0, 0, tzinfo=pytz.UTC),
            ),  # 22 hours ago (fresh for file_remover)
        ]

        for entity_id, entity_type, update_time in entity_configs:
            mock_status = Mock()
            mock_status.id = entity_id
            mock_status.entity_type = entity_type
            mock_status.updated_at_ = update_time
            status_objects.append(mock_status)

        # when
        result = alert.get_issues(status_objects)

        # then
        assert result == []

    @patch("monitoring.alerts.stale_status_alert.datetime")
    def test_file_remover_threshold_behavior(self, mock_datetime: Mock) -> None:
        """Test that file_remover job uses 24-hour threshold while other entities use 15-minute threshold."""
        # given
        alert = StaleStatusAlert()
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = now

        # File remover at different staleness levels
        mock_file_remover_boundary = Mock()
        mock_file_remover_boundary.id = "file_remover"
        mock_file_remover_boundary.entity_type = KrakenStatusEntities.JOB
        mock_file_remover_boundary.updated_at_ = datetime(
            2024, 1, 14, 12, 0, 1, tzinfo=pytz.UTC
        )  # 23 hours 59 minutes 59 seconds ago (fresh)

        # Regular job at same time (would be very stale for regular threshold)
        mock_regular_job = Mock()
        mock_regular_job.id = "regular_job"
        mock_regular_job.entity_type = KrakenStatusEntities.JOB
        mock_regular_job.updated_at_ = datetime(
            2024, 1, 14, 12, 0, 1, tzinfo=pytz.UTC
        )  # 23 hours 59 minutes 59 seconds ago (stale)

        status_objects = [mock_file_remover_boundary, mock_regular_job]

        # when
        result = alert.get_issues(status_objects)

        # then
        # Only regular job should be stale
        expected = [("regular_job", datetime(2024, 1, 14, 12, 0, 1, tzinfo=pytz.UTC))]
        assert result == expected
