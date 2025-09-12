"""Unit tests for StatusPileUpAlert class."""

from unittest.mock import Mock, patch

from monitoring.alerts.config import Cases
from monitoring.alerts.status_pile_up_alert import StatusPileUpAlert
from shared.db.models import KrakenStatusEntities


class TestStatusPileUpAlert:
    """Test suite for StatusPileUpAlert class."""

    def test_case_name_should_return_correct_value(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = StatusPileUpAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.STATUS_PILE_UP

    @patch("monitoring.alerts.status_pile_up_alert.STATUS_PILE_UP_THRESHOLDS")
    @patch("monitoring.alerts.status_pile_up_alert.RawFile")
    def test_check_should_handle_comprehensive_pile_up_scenarios(
        self, mock_rawfile: Mock, mock_thresholds: Mock
    ) -> None:
        """Test that check handles complex mixed scenarios with various instruments and pile-up conditions."""
        # given
        alert = StatusPileUpAlert()
        mock_thresholds.__getitem__.side_effect = lambda status: {
            "processing": 5,
            "quanting": 10,
            "copying": 3,
            "monitoring": 8,
            "validating": 15,
        }.get(status, 5)  # Default threshold of 5

        # Create comprehensive mix of status objects
        mock_instrument_no_pileup = Mock()
        mock_instrument_no_pileup.id = "instrument_healthy"
        mock_instrument_no_pileup.entity_type = KrakenStatusEntities.INSTRUMENT

        mock_instrument_single_pileup = Mock()
        mock_instrument_single_pileup.id = "instrument_processing_issues"
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

        # Mock RawFile queries with different pile-up scenarios
        def mock_rawfile_side_effect(**kwargs) -> Mock:
            instrument_id = kwargs.get("instrument_id")
            mock_query = Mock()

            # Define status counts for each scenario
            scenarios = {
                "instrument_healthy": {
                    "processing": 2,
                    "quanting": 5,
                    "copying": 1,
                },
                "instrument_processing_issues": {
                    "processing": 8,
                    "quanting": 3,
                    "copying": 2,
                },
                "instrument_multiple_issues": {
                    "processing": 12,
                    "quanting": 25,
                    "copying": 6,
                    "monitoring": 2,
                },
                "instrument_boundary_case": {
                    "processing": 5,
                    "quanting": 11,
                    "copying": 3,
                    "validating": 16,
                },
                "instrument_mixed_status": {
                    "processing": 7,
                    "quanting": 8,
                    "copying": 4,
                    "monitoring": 12,
                    "unknown_status": 10,
                },
            }

            mock_query.item_frequencies.return_value = scenarios.get(instrument_id, {})
            return mock_query

        mock_rawfile.objects.side_effect = mock_rawfile_side_effect

        # when
        result = alert.get_issues(status_objects)

        # then
        # Should only check instruments and return those with pile-ups
        expected = [
            ("instrument_processing_issues", "processing: 8"),
            ("instrument_multiple_issues", "processing: 12; quanting: 25; copying: 6"),
            ("instrument_boundary_case", "quanting: 11; validating: 16"),
            (
                "instrument_mixed_status",
                "processing: 7; copying: 4; monitoring: 12; unknown_status: 10",
            ),
        ]
        assert result == expected

    def test_format_message_should_handle_comprehensive_scenarios(self) -> None:
        """Test that format_message handles multiple issues with various pile-up patterns."""
        # given
        alert = StatusPileUpAlert()

        # Complex mix of issues with different pile-up patterns
        issues = [
            ("instrument_critical", "processing: 25; quanting: 18"),
            ("instrument_single", "copying: 12"),
            (
                "instrument_complex",
                "processing: 8; quanting: 15; copying: 7; monitoring: 20",
            ),
            ("instrument_minor", "validating: 6"),
            ("instrument_edge_case", "unknown_status: 50; processing: 10"),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Status pile up detected:\n"
            "- `instrument_critical`: processing: 25; quanting: 18\n"
            "- `instrument_single`: copying: 12\n"
            "- `instrument_complex`: processing: 8; quanting: 15; copying: 7; monitoring: 20\n"
            "- `instrument_minor`: validating: 6\n"
            "- `instrument_edge_case`: unknown_status: 50; processing: 10"
        )
        assert result == expected

    @patch("monitoring.alerts.status_pile_up_alert.STATUS_PILE_UP_THRESHOLDS")
    @patch("monitoring.alerts.status_pile_up_alert.RawFile")
    def test_check_should_return_empty_when_no_pile_ups(
        self, mock_rawfile: Mock, mock_thresholds: Mock
    ) -> None:
        """Test that check returns empty result when no instruments have status pile-ups."""
        # given
        alert = StatusPileUpAlert()
        mock_thresholds.__getitem__.side_effect = lambda status: {
            "processing": 10,
            "quanting": 15,
            "copying": 5,
        }[status]

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

        # Mock RawFile queries - all counts below thresholds or empty
        def mock_rawfile_side_effect(**kwargs) -> Mock:
            instrument_id = kwargs.get("instrument_id")
            mock_query = Mock()

            scenarios = {
                "instrument1": {
                    "processing": 5,
                    "quanting": 8,
                },
                "instrument2": {
                    "copying": 3,
                    "processing": 2,
                },
                "instrument3": {},  # No files
            }

            mock_query.item_frequencies.return_value = scenarios.get(instrument_id, {})
            return mock_query

        mock_rawfile.objects.side_effect = mock_rawfile_side_effect

        # when
        result = alert.get_issues(status_objects)

        # then
        assert result == []

    @patch("monitoring.alerts.status_pile_up_alert.STATUS_PILE_UP_THRESHOLDS")
    @patch("monitoring.alerts.status_pile_up_alert.RawFile")
    def test_entity_type_filtering_behavior(
        self, mock_rawfile: Mock, mock_thresholds: Mock
    ) -> None:
        """Test that check only processes INSTRUMENT entity types and ignores JOB and FILE_SYSTEM."""
        # given
        alert = StatusPileUpAlert()
        mock_thresholds.__getitem__.side_effect = lambda status: {"processing": 5}[
            status
        ]

        mock_instrument = Mock()
        mock_instrument.id = "test_instrument"
        mock_instrument.entity_type = KrakenStatusEntities.INSTRUMENT

        mock_job1 = Mock()
        mock_job1.id = "job_with_pileup"
        mock_job1.entity_type = KrakenStatusEntities.JOB

        mock_job2 = Mock()
        mock_job2.id = "another_job"
        mock_job2.entity_type = KrakenStatusEntities.JOB

        mock_filesystem = Mock()
        mock_filesystem.id = "filesystem_with_pileup"
        mock_filesystem.entity_type = KrakenStatusEntities.FILE_SYSTEM

        status_objects = [mock_instrument, mock_job1, mock_job2, mock_filesystem]

        # Mock RawFile query - only for instrument
        mock_query = Mock()
        mock_query.item_frequencies.return_value = {"processing": 8}  # Above threshold
        mock_rawfile.objects.return_value = mock_query

        # when
        result = alert.get_issues(status_objects)

        # then
        # Should only call RawFile.objects once for the instrument
        mock_rawfile.objects.assert_called_once()
        call_args = mock_rawfile.objects.call_args
        assert call_args[1]["instrument_id"] == "test_instrument"
        assert "status__nin" in call_args[1]
        assert result == [("test_instrument", "processing: 8")]
