"""Tests for the file_based_job_handler module."""

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.keys import JobStates, QuantingEnv
from jobs._experimental.file_based_job_handler import FileBasedJobHandler

from shared.keys import InternalPaths


@pytest.fixture
def mock_raw_file() -> MagicMock:
    """Create a mock RawFile for testing."""
    raw_file = MagicMock()
    raw_file.id = "test_raw_file_123"
    raw_file.project_id = "test_project"
    raw_file.instrument_id = "test_instrument"
    return raw_file


@pytest.fixture
def sample_environment() -> dict:
    """Create sample environment variables for testing."""
    return {
        QuantingEnv.RAW_FILE_ID: "test_raw_file_123",
        QuantingEnv.OUTPUT_PATH: "/test/output/path",
        QuantingEnv.RELATIVE_OUTPUT_PATH: "test/relative/path",
        QuantingEnv.CUSTOM_COMMAND: "test_command.exe",
        "OTHER_VAR": "other_value",
    }


class TestFileBasedJobHandler:
    """Test cases for FileBasedJobHandler class."""

    def test_init_should_set_job_submit_directory(self) -> None:
        """Test that FileBasedJobHandler initializes with correct job submit directory."""
        # given

        # when
        handler = FileBasedJobHandler()

        # then
        expected_path = (
            Path(InternalPaths.MOUNTS_PATH) / InternalPaths.OUTPUT / "job_queue"
        )
        assert handler._job_submit_dir == expected_path

    @patch(
        "jobs._experimental.file_based_job_handler.Path.open", new_callable=mock_open
    )
    @patch("jobs._experimental.file_based_job_handler.Path.mkdir")
    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    def test_start_job_should_create_job_file_when_directory_creation_succeeds(
        self,
        mock_exists: MagicMock,
        mock_mkdir: MagicMock,
        mock_file_open: MagicMock,
        sample_environment: dict,
    ) -> None:
        """Test that start_job creates a .job file with correct content when directory creation succeeds."""
        # given
        handler = FileBasedJobHandler()
        mock_exists.return_value = False
        mock_mkdir.return_value = None

        # when
        job_id = handler.start_job(
            "ignored_script", sample_environment, "ignored_folder"
        )

        # then
        assert job_id == "test_raw_file_123"
        mock_mkdir.assert_called_once_with(exist_ok=True)
        mock_exists.assert_called_once()

        # Verify file content
        expected_content = [
            f"{QuantingEnv.RAW_FILE_ID}=test_raw_file_123\n",
            f"{QuantingEnv.OUTPUT_PATH}=/test/output/path\n",
            f"{QuantingEnv.RELATIVE_OUTPUT_PATH}=test/relative/path\n",
            f"{QuantingEnv.CUSTOM_COMMAND}=test_command.exe\n",
        ]
        handle = mock_file_open.return_value.__enter__.return_value
        for expected_line in expected_content:
            handle.write.assert_any_call(expected_line)

    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    def test_start_job_should_raise_exception_when_job_file_already_exists(
        self, mock_exists: MagicMock, sample_environment: dict
    ) -> None:
        """Test that start_job raises AirflowFailException when job file already exists."""
        # given
        handler = FileBasedJobHandler()
        mock_exists.return_value = True

        # when/then
        with pytest.raises(AirflowFailException, match="Job file .* already exists"):
            handler.start_job("ignored_script", sample_environment, "ignored_folder")

    @patch("jobs._experimental.file_based_job_handler.get_raw_file_by_id")
    @patch("jobs._experimental.file_based_job_handler._get_project_id_or_fallback")
    @patch(
        "jobs._experimental.file_based_job_handler.get_internal_output_path_for_raw_file"
    )
    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    def test_get_job_status_should_return_pending_when_status_file_does_not_exist(
        self,
        mock_exists: MagicMock,
        mock_get_path: MagicMock,
        mock_get_project: MagicMock,
        mock_get_raw_file: MagicMock,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test that get_job_status returns PENDING when status file does not exist."""
        # given
        handler = FileBasedJobHandler()
        mock_get_raw_file.return_value = mock_raw_file
        mock_get_project.return_value = "test_project"
        mock_get_path.return_value = Path("/test/output")
        mock_exists.return_value = False

        # when
        status = handler.get_job_status("test_raw_file_123")

        # then
        assert status == JobStates.PENDING

    @patch("jobs._experimental.file_based_job_handler.get_raw_file_by_id")
    @patch("jobs._experimental.file_based_job_handler._get_project_id_or_fallback")
    @patch(
        "jobs._experimental.file_based_job_handler.get_internal_output_path_for_raw_file"
    )
    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    @patch(
        "jobs._experimental.file_based_job_handler.Path.open",
        new_callable=mock_open,
        read_data="",
    )
    def test_get_job_status_should_return_running_when_status_file_is_empty(  # noqa: PLR0913
        self,
        mock_file_open: MagicMock,  # noqa: ARG002
        mock_exists: MagicMock,
        mock_get_path: MagicMock,
        mock_get_project: MagicMock,
        mock_get_raw_file: MagicMock,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test that get_job_status returns RUNNING when status file is empty."""
        # given
        handler = FileBasedJobHandler()
        mock_get_raw_file.return_value = mock_raw_file
        mock_get_project.return_value = "test_project"
        mock_get_path.return_value = Path("/test/output")
        mock_exists.return_value = True

        # when
        status = handler.get_job_status("test_raw_file_123")

        # then
        assert status == JobStates.RUNNING

    @patch("jobs._experimental.file_based_job_handler.get_raw_file_by_id")
    @patch("jobs._experimental.file_based_job_handler._get_project_id_or_fallback")
    @patch(
        "jobs._experimental.file_based_job_handler.get_internal_output_path_for_raw_file"
    )
    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    @patch(
        "jobs._experimental.file_based_job_handler.Path.open",
        new_callable=mock_open,
        read_data="Starting job\nCOMPLETED\n",
    )
    def test_get_job_status_should_return_completed_when_last_line_is_completed(  # noqa: PLR0913
        self,
        mock_file_open: MagicMock,  # noqa: ARG002
        mock_exists: MagicMock,
        mock_get_path: MagicMock,
        mock_get_project: MagicMock,
        mock_get_raw_file: MagicMock,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test that get_job_status returns COMPLETED when last non-empty line is COMPLETED."""
        # given
        handler = FileBasedJobHandler()
        mock_get_raw_file.return_value = mock_raw_file
        mock_get_project.return_value = "test_project"
        mock_get_path.return_value = Path("/test/output")
        mock_exists.return_value = True

        # when
        status = handler.get_job_status("test_raw_file_123")

        # then
        assert status == JobStates.COMPLETED

    @patch("jobs._experimental.file_based_job_handler.get_raw_file_by_id")
    @patch("jobs._experimental.file_based_job_handler._get_project_id_or_fallback")
    @patch(
        "jobs._experimental.file_based_job_handler.get_internal_output_path_for_raw_file"
    )
    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    @patch(
        "jobs._experimental.file_based_job_handler.Path.open",
        new_callable=mock_open,
        read_data="Starting job\nFAILED\n",
    )
    def test_get_job_status_should_return_failed_when_last_line_is_failed(  # noqa: PLR0913
        self,
        mock_file_open: MagicMock,  # noqa: ARG002
        mock_exists: MagicMock,
        mock_get_path: MagicMock,
        mock_get_project: MagicMock,
        mock_get_raw_file: MagicMock,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test that get_job_status returns FAILED when last non-empty line is FAILED."""
        # given
        handler = FileBasedJobHandler()
        mock_get_raw_file.return_value = mock_raw_file
        mock_get_project.return_value = "test_project"
        mock_get_path.return_value = Path("/test/output")
        mock_exists.return_value = True

        # when
        status = handler.get_job_status("test_raw_file_123")

        # then
        assert status == JobStates.FAILED

    @patch("jobs._experimental.file_based_job_handler.get_raw_file_by_id")
    @patch("jobs._experimental.file_based_job_handler._get_project_id_or_fallback")
    @patch(
        "jobs._experimental.file_based_job_handler.get_internal_output_path_for_raw_file"
    )
    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    @patch(
        "jobs._experimental.file_based_job_handler.Path.open",
        new_callable=mock_open,
        read_data="Starting job\nSome log output\n\n",
    )
    def test_get_job_status_should_return_running_when_last_line_is_not_status(  # noqa: PLR0913
        self,
        mock_file_open: MagicMock,  # noqa: ARG002
        mock_exists: MagicMock,
        mock_get_path: MagicMock,
        mock_get_project: MagicMock,
        mock_get_raw_file: MagicMock,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test that get_job_status returns RUNNING when last non-empty line is not a status."""
        # given
        handler = FileBasedJobHandler()
        mock_get_raw_file.return_value = mock_raw_file
        mock_get_project.return_value = "test_project"
        mock_get_path.return_value = Path("/test/output")
        mock_exists.return_value = True

        # when
        status = handler.get_job_status("test_raw_file_123")

        # then
        assert status == JobStates.RUNNING
