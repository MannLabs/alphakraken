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

    @pytest.mark.parametrize(
        ("read_data", "expected_status"),
        [
            ("", JobStates.RUNNING),
            ("Starting job\nCOMPLETED\n", JobStates.COMPLETED),
            ("Starting job\nFAILED\n", JobStates.FAILED),
            ("Starting job\nSome log output\n\n", JobStates.RUNNING),
        ],
    )
    @patch("jobs._experimental.file_based_job_handler.get_raw_file_by_id")
    @patch("jobs._experimental.file_based_job_handler._get_project_id_or_fallback")
    @patch(
        "jobs._experimental.file_based_job_handler.get_internal_output_path_for_raw_file"
    )
    @patch("jobs._experimental.file_based_job_handler.Path.exists")
    def test_get_job_status_should_return_correct_status_based_on_file_content(  # noqa: PLR0913
        self,
        mock_exists: MagicMock,
        mock_get_path: MagicMock,
        mock_get_project: MagicMock,
        mock_get_raw_file: MagicMock,
        mock_raw_file: MagicMock,
        read_data: str,
        expected_status: str,
    ) -> None:
        """Test that get_job_status returns correct status based on file content."""
        # given
        handler = FileBasedJobHandler()
        mock_get_raw_file.return_value = mock_raw_file
        mock_get_project.return_value = "test_project"
        mock_get_path.return_value = Path("/test/output")
        mock_exists.return_value = True

        # when
        with patch(
            "jobs._experimental.file_based_job_handler.Path.open",
            mock_open(read_data=read_data),
        ):
            status = handler.get_job_status("test_raw_file_123")

        # then
        assert status == expected_status
