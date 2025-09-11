"""Unit tests for InstrumentFilePileUpAlert class."""

from unittest.mock import Mock, patch

from monitoring.alerts.config import Cases
from monitoring.alerts.instrument_file_pile_up_alert import InstrumentFilePileUpAlert
from shared.db.models import InstrumentFileStatus, KrakenStatusEntities


class TestInstrumentFilePileUpAlert:
    """Test suite for InstrumentFilePileUpAlert class."""

    def test_case_name_should_return_correct_value(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = InstrumentFilePileUpAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.INSTRUMENT_FILE_PILE_UP

    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.INSTRUMENT_FILE_MIN_AGE_HOURS",
        6,
    )
    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.INSTRUMENT_FILE_PILE_UP_THRESHOLDS"
    )
    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.get_raw_files_by_instrument_file_status"
    )
    def test_check_should_handle_comprehensive_file_pile_up_scenarios(
        self, mock_get_files: Mock, mock_thresholds: Mock
    ) -> None:
        """Test that check handles complex mixed scenarios with various instruments and file pile-up conditions."""
        # given
        alert = InstrumentFilePileUpAlert()
        mock_thresholds.items.return_value = [
            (InstrumentFileStatus.NEW, 20),
            (InstrumentFileStatus.MOVED, 50),
            (InstrumentFileStatus.PURGED, 5),
        ]

        # Create comprehensive mix of status objects
        mock_instrument_no_pileup = Mock()
        mock_instrument_no_pileup.id = "instrument_healthy"
        mock_instrument_no_pileup.entity_type = KrakenStatusEntities.INSTRUMENT

        mock_instrument_single_pileup = Mock()
        mock_instrument_single_pileup.id = "instrument_new_files_issue"
        mock_instrument_single_pileup.entity_type = KrakenStatusEntities.INSTRUMENT

        mock_instrument_multiple_pileup = Mock()
        mock_instrument_multiple_pileup.id = "instrument_multiple_issues"
        mock_instrument_multiple_pileup.entity_type = KrakenStatusEntities.INSTRUMENT

        mock_instrument_boundary = Mock()
        mock_instrument_boundary.id = "instrument_boundary_case"
        mock_instrument_boundary.entity_type = KrakenStatusEntities.INSTRUMENT

        mock_instrument_mixed = Mock()
        mock_instrument_mixed.id = "instrument_mixed_status"
        mock_instrument_mixed.entity_type = KrakenStatusEntities.INSTRUMENT

        # Should be ignored - not instrument entity types
        mock_job = Mock()
        mock_job.id = "some_job"
        mock_job.entity_type = KrakenStatusEntities.JOB

        mock_filesystem = Mock()
        mock_filesystem.id = "backup_system"
        mock_filesystem.entity_type = KrakenStatusEntities.FILE_SYSTEM

        status_objects = [
            mock_instrument_no_pileup,
            mock_instrument_single_pileup,
            mock_instrument_multiple_pileup,
            mock_instrument_boundary,
            mock_instrument_mixed,
            mock_job,
            mock_filesystem,
        ]

        # Mock get_raw_files_by_instrument_file_status with different scenarios
        def mock_get_files_side_effect(
            status: InstrumentFileStatus, instrument_id: str, min_age_hours: int
        ) -> list[Mock]:
            # Verify correct min_age_hours is passed
            assert min_age_hours == 6

            # Define file counts for each scenario
            scenarios = {
                "instrument_healthy": {
                    InstrumentFileStatus.NEW: 15,  # Below 20
                    InstrumentFileStatus.MOVED: 30,  # Below 50
                    InstrumentFileStatus.PURGED: 3,  # Below 5
                },
                "instrument_new_files_issue": {
                    InstrumentFileStatus.NEW: 25,  # Above 20
                    InstrumentFileStatus.MOVED: 20,  # Below 50
                    InstrumentFileStatus.PURGED: 2,  # Below 5
                },
                "instrument_multiple_issues": {
                    InstrumentFileStatus.NEW: 35,  # Above 20
                    InstrumentFileStatus.MOVED: 75,  # Above 50
                    InstrumentFileStatus.PURGED: 8,  # Above 5
                },
                "instrument_boundary_case": {
                    InstrumentFileStatus.NEW: 20,  # Exactly 20 (should not trigger)
                    InstrumentFileStatus.MOVED: 55,  # Above 50
                    InstrumentFileStatus.PURGED: 5,  # Exactly 5 (should not trigger)
                },
                "instrument_mixed_status": {
                    InstrumentFileStatus.NEW: 25,  # Above 20
                    InstrumentFileStatus.MOVED: 40,  # Below 50
                    InstrumentFileStatus.PURGED: 7,  # Above 5
                },
            }

            count = scenarios.get(instrument_id, {}).get(status, 0)
            return [Mock() for _ in range(count)]

        mock_get_files.side_effect = mock_get_files_side_effect

        # when
        result = alert._get_issues(status_objects)

        # then
        # Should only check instruments and return those with pile-ups
        expected = [
            ("instrument_new_files_issue", "new: 25"),
            ("instrument_multiple_issues", "new: 35; moved: 75; purged: 8"),
            ("instrument_boundary_case", "moved: 55"),
            ("instrument_mixed_status", "new: 25; purged: 7"),
        ]
        assert result == expected

    def test_format_message_should_handle_comprehensive_scenarios(self) -> None:
        """Test that format_message handles multiple issues with various file pile-up patterns."""
        # given
        alert = InstrumentFilePileUpAlert()

        # Complex mix of issues with different pile-up patterns
        issues = [
            ("instrument_critical", "new: 45; moved: 120"),
            ("instrument_single_new", "new: 30"),
            ("instrument_complex", "new: 25; moved: 80; purged: 15"),
            ("instrument_minor", "purged: 8"),
            ("instrument_moved_only", "moved: 200"),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Instrument file pile up detected (files not moved/purged):\n"
            "- `instrument_critical`: new: 45; moved: 120\n"
            "- `instrument_single_new`: new: 30\n"
            "- `instrument_complex`: new: 25; moved: 80; purged: 15\n"
            "- `instrument_minor`: purged: 8\n"
            "- `instrument_moved_only`: moved: 200"
        )
        assert result == expected

    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.INSTRUMENT_FILE_MIN_AGE_HOURS",
        12,
    )
    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.INSTRUMENT_FILE_PILE_UP_THRESHOLDS"
    )
    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.get_raw_files_by_instrument_file_status"
    )
    def test_check_should_return_empty_when_no_pile_ups(
        self, mock_get_files: Mock, mock_thresholds: Mock
    ) -> None:
        """Test that check returns empty result when no instruments have file pile-ups."""
        # given
        alert = InstrumentFilePileUpAlert()
        mock_thresholds.items.return_value = [
            (InstrumentFileStatus.NEW, 25),
            (InstrumentFileStatus.MOVED, 60),
        ]

        # Multiple instruments with no pile-ups
        status_objects = []
        for instrument_id in ["instrument1", "instrument2", "instrument3"]:
            mock_instrument = Mock()
            mock_instrument.id = instrument_id
            mock_instrument.entity_type = KrakenStatusEntities.INSTRUMENT
            status_objects.append(mock_instrument)

        # Add non-instrument entities (should be ignored)
        mock_job = Mock()
        mock_job.id = "job1"
        mock_job.entity_type = KrakenStatusEntities.JOB

        mock_filesystem = Mock()
        mock_filesystem.id = "filesystem1"
        mock_filesystem.entity_type = KrakenStatusEntities.FILE_SYSTEM

        status_objects.extend([mock_job, mock_filesystem])

        # Mock get_raw_files_by_instrument_file_status - all counts below thresholds
        def mock_get_files_side_effect(
            status: InstrumentFileStatus, instrument_id: str, min_age_hours: int
        ) -> list[Mock]:
            # Verify correct min_age_hours is passed
            assert min_age_hours == 12

            scenarios = {
                "instrument1": {
                    InstrumentFileStatus.NEW: 15,  # Below 25
                    InstrumentFileStatus.MOVED: 45,  # Below 60
                },
                "instrument2": {
                    InstrumentFileStatus.NEW: 10,  # Below 25
                    InstrumentFileStatus.MOVED: 20,  # Below 60
                },
                "instrument3": {},  # No files
            }

            count = scenarios.get(instrument_id, {}).get(status, 0)
            return [Mock() for _ in range(count)]

        mock_get_files.side_effect = mock_get_files_side_effect

        # when
        result = alert._get_issues(status_objects)

        # then
        assert result == []

    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.INSTRUMENT_FILE_MIN_AGE_HOURS",
        8,
    )
    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.INSTRUMENT_FILE_PILE_UP_THRESHOLDS"
    )
    @patch(
        "monitoring.alerts.instrument_file_pile_up_alert.get_raw_files_by_instrument_file_status"
    )
    def test_entity_type_filtering_and_parameter_validation(
        self, mock_get_files: Mock, mock_thresholds: Mock
    ) -> None:
        """Test that check only processes INSTRUMENT entity types and passes correct parameters."""
        # given
        alert = InstrumentFilePileUpAlert()
        mock_thresholds.items.return_value = [
            (InstrumentFileStatus.NEW, 15),
            (InstrumentFileStatus.MOVED, 30),
        ]

        mock_instrument = Mock()
        mock_instrument.id = "test_instrument"
        mock_instrument.entity_type = KrakenStatusEntities.INSTRUMENT

        mock_job = Mock()
        mock_job.id = "job_with_files"
        mock_job.entity_type = KrakenStatusEntities.JOB

        mock_filesystem = Mock()
        mock_filesystem.id = "filesystem_with_files"
        mock_filesystem.entity_type = KrakenStatusEntities.FILE_SYSTEM

        status_objects = [mock_instrument, mock_job, mock_filesystem]

        # Mock get_raw_files_by_instrument_file_status - above thresholds for instrument
        def mock_get_files_side_effect(
            status: InstrumentFileStatus, instrument_id: str, min_age_hours: int
        ) -> list[Mock]:
            assert (
                instrument_id == "test_instrument"
            )  # Should only be called for instrument
            assert min_age_hours == 8

            counts = {
                InstrumentFileStatus.NEW: 20,  # Above 15
                InstrumentFileStatus.MOVED: 35,  # Above 30
            }

            count = counts.get(status, 0)
            return [Mock() for _ in range(count)]

        mock_get_files.side_effect = mock_get_files_side_effect

        # when
        result = alert._get_issues(status_objects)

        # then
        # Should only be called for each status type for the instrument
        expected_calls = 2  # NEW and MOVED status for one instrument
        assert mock_get_files.call_count == expected_calls

        # Verify all calls were made with correct instrument
        for call in mock_get_files.call_args_list:
            args, kwargs = call
            assert kwargs["instrument_id"] == "test_instrument"
            assert kwargs["min_age_hours"] == 8

        assert result == [("test_instrument", "new: 20; moved: 35")]
