"""Tests for the docker_job_handler module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.keys import JobStates, QuantingEnv
from docker.errors import ImageNotFound, NotFound

from shared.keys import SoftwareTypes

MODULE = "jobs.docker_job_handler"

HOST_MOUNTS_PATH = Path("/host/mounts")

INTERNAL_RAW_FILE_PATH = "/opt/airflow/mounts/backup/test1/2024_07/raw_file_1.raw"
INTERNAL_OUTPUT_PATH = "/opt/airflow/mounts/output/P1/out_raw_file_1.raw/msqc"

DEFAULT_STATE = {
    "ExitCode": 0,
    "OOMKilled": False,
    "StartedAt": "2024-07-01T12:00:00.123456789Z",
    "FinishedAt": "2024-07-01T12:08:42.123456789Z",
}


@pytest.fixture
def handler() -> MagicMock:
    """Create a DockerJobHandler with a mocked docker client."""
    with (
        patch(f"{MODULE}.docker.from_env") as mock_from_env,
        patch(f"{MODULE}.get_host_mounts_path", return_value=HOST_MOUNTS_PATH),
    ):
        from jobs.docker_job_handler import DockerJobHandler

        handler_ = DockerJobHandler()
        handler_._client = mock_from_env.return_value
        return handler_


@pytest.fixture
def sample_environment() -> dict:
    """Create sample environment variables for testing."""
    return {
        QuantingEnv.RAW_FILE_ID: "raw_file_1.raw",
        QuantingEnv.SOFTWARE_TYPE: SoftwareTypes.MSQC,
        QuantingEnv.NUM_THREADS: 2,
        QuantingEnv.SLURM_MEM: "31G",
        QuantingEnv.SLURM_CPUS_PER_TASK: 2,
        QuantingEnv.INTERNAL_RAW_FILE_PATH: INTERNAL_RAW_FILE_PATH,
        QuantingEnv.INTERNAL_OUTPUT_PATH: INTERNAL_OUTPUT_PATH,
    }


def _container(
    *,
    status: str = "exited",
    state: dict | None = None,
    output_path: str = INTERNAL_OUTPUT_PATH,
) -> MagicMock:
    """Create a mock docker container."""
    container = MagicMock()
    container.id = "0123456789abcdef"
    container.status = status
    container.attrs = {"State": DEFAULT_STATE | (state or {})}
    container.labels = {"alphakraken.internal_output_path": output_path}
    container.logs.return_value = b"some log output"
    return container


@patch(f"{MODULE}.Path.exists")
class TestStartJob:
    """Test cases for DockerJobHandler.start_job()."""

    def test_start_job_should_run_container_with_translated_host_paths(
        self,
        mock_exists: MagicMock,
        handler: MagicMock,
        sample_environment: dict,
    ) -> None:
        """Test that bind sources are host paths and binds are the internal paths."""
        # given
        mock_exists.return_value = True
        handler._client.containers.get.side_effect = NotFound("not found")
        handler._client.containers.run.return_value = _container()

        # when
        job_id = handler.start_job("ignored.sh", sample_environment, "2024_07")

        # then
        assert job_id == "0123456789ab"

        _, kwargs = handler._client.containers.run.call_args
        assert kwargs["volumes"] == {
            "/host/mounts/backup/test1/2024_07/raw_file_1.raw": {
                "bind": INTERNAL_RAW_FILE_PATH,
                "mode": "ro",
            },
            "/host/mounts/output/P1/out_raw_file_1.raw/msqc": {
                "bind": INTERNAL_OUTPUT_PATH,
                "mode": "rw",
            },
        }
        assert kwargs["environment"] == {
            QuantingEnv.RAW_FILE_PATH: INTERNAL_RAW_FILE_PATH,
            QuantingEnv.OUTPUT_PATH: INTERNAL_OUTPUT_PATH,
            QuantingEnv.NUM_THREADS: "2",
        }
        assert kwargs["name"] == "kraken-msqc-raw_file_1.raw"
        assert kwargs["mem_limit"] == "31g"
        assert kwargs["nano_cpus"] == 2_000_000_000
        assert kwargs["network_mode"] == "none"
        assert kwargs["detach"] is True

    def test_start_job_should_remove_leftover_container(
        self,
        mock_exists: MagicMock,
        handler: MagicMock,
        sample_environment: dict,
    ) -> None:
        """Test that a container left over from a previous run of the same job is removed."""
        # given
        mock_exists.return_value = True
        leftover = _container()
        handler._client.containers.get.return_value = leftover
        handler._client.containers.run.return_value = _container()

        # when
        handler.start_job("ignored.sh", sample_environment, "2024_07")

        # then
        handler._client.containers.get.assert_called_once_with(
            "kraken-msqc-raw_file_1.raw"
        )
        leftover.remove.assert_called_once_with(force=True)

    def test_start_job_should_raise_on_unknown_software_type(
        self,
        mock_exists: MagicMock,
        handler: MagicMock,
        sample_environment: dict,
    ) -> None:
        """Test that a software type without an image mapping fails the task."""
        # given
        mock_exists.return_value = True
        sample_environment[QuantingEnv.SOFTWARE_TYPE] = SoftwareTypes.ALPHADIA

        # when, then
        with pytest.raises(AirflowFailException, match="No docker image defined"):
            handler.start_job("ignored.sh", sample_environment, "2024_07")

    def test_start_job_should_raise_on_missing_path(
        self,
        mock_exists: MagicMock,
        handler: MagicMock,
        sample_environment: dict,
    ) -> None:
        """Test that a bind source that the worker cannot see fails the task."""
        # given
        mock_exists.return_value = False

        # when, then
        with pytest.raises(AirflowFailException, match="does not exist in the worker"):
            handler.start_job("ignored.sh", sample_environment, "2024_07")

    def test_start_job_should_raise_on_missing_image(
        self,
        mock_exists: MagicMock,
        handler: MagicMock,
        sample_environment: dict,
    ) -> None:
        """Test that a missing image gives a hint on how to build it."""
        # given
        mock_exists.return_value = True
        handler._client.containers.get.side_effect = NotFound("not found")
        handler._client.containers.run.side_effect = ImageNotFound("no image")

        # when, then
        with pytest.raises(AirflowFailException, match="not found"):
            handler.start_job("ignored.sh", sample_environment, "2024_07")


class TestGetJobStatus:
    """Test cases for DockerJobHandler.get_job_status()."""

    @pytest.mark.parametrize(
        ("status", "state", "expected"),
        [
            ("created", {}, JobStates.PENDING),
            ("running", {}, JobStates.RUNNING),
            ("exited", {"ExitCode": 0}, JobStates.COMPLETED),
            ("exited", {"ExitCode": 1}, JobStates.FAILED),
            ("exited", {"ExitCode": 137, "OOMKilled": True}, JobStates.OUT_OF_MEMORY),
            ("dead", {"ExitCode": 255}, JobStates.FAILED),
        ],
    )
    def test_get_job_status_should_map_docker_states(
        self,
        handler: MagicMock,
        status: str,
        state: dict,
        expected: str,
    ) -> None:
        """Test that docker states are mapped to job states."""
        # given
        handler._client.containers.get.return_value = _container(
            status=status, state=state
        )

        # when
        job_status = handler.get_job_status("0123456789ab")

        # then
        assert job_status == expected

    def test_get_job_status_should_return_unknown_for_removed_container(
        self, handler: MagicMock
    ) -> None:
        """Test that a container that does not exist anymore yields UNKNOWN."""
        # given
        handler._client.containers.get.side_effect = NotFound("not found")

        # when
        job_status = handler.get_job_status("0123456789ab")

        # then
        assert job_status == JobStates.UNKNOWN


class TestGetJobResult:
    """Test cases for DockerJobHandler.get_job_result()."""

    def test_get_job_result_should_return_status_and_elapsed_time(
        self, handler: MagicMock, tmp_path: Path
    ) -> None:
        """Test that the elapsed time is derived from the container timestamps."""
        # given
        handler._client.containers.get.return_value = _container(
            output_path=str(tmp_path)
        )

        # when
        job_status, time_elapsed = handler.get_job_result("0123456789ab")

        # then
        assert job_status == JobStates.COMPLETED
        assert time_elapsed == 522
        assert (tmp_path / "log.txt").read_bytes() == b"some log output"

    def test_get_job_result_should_return_zero_time_for_running_container(
        self, handler: MagicMock, tmp_path: Path
    ) -> None:
        """Test that a container that has not finished yet reports no elapsed time."""
        # given
        handler._client.containers.get.return_value = _container(
            status="running",
            state={"FinishedAt": "0001-01-01T00:00:00Z"},
            output_path=str(tmp_path),
        )

        # when
        job_status, time_elapsed = handler.get_job_result("0123456789ab")

        # then
        assert job_status == JobStates.RUNNING
        assert time_elapsed == 0

    def test_get_job_result_should_tolerate_removed_container(
        self, handler: MagicMock
    ) -> None:
        """Test that a container that does not exist anymore yields UNKNOWN."""
        # given
        handler._client.containers.get.side_effect = NotFound("not found")

        # when
        job_status, time_elapsed = handler.get_job_result("0123456789ab")

        # then
        assert job_status == JobStates.UNKNOWN
        assert time_elapsed == 0
