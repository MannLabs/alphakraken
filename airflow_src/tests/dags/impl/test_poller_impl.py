"""Tests for the poller_impl module."""

from unittest.mock import MagicMock, call, patch

from dags.impl.poller_impl import _parse_squeue_output, poll_slurm_job_states
from paramiko.ssh_exception import SSHException

SSH_PREFIX = "some_cluster_ssh"


def test_parse_squeue_output() -> None:
    """Test that the squeue output is parsed to a dict job id -> state."""
    assert _parse_squeue_output("123 RUNNING\n456 PENDING\n") == {
        "123": "RUNNING",
        "456": "PENDING",
    }
    assert _parse_squeue_output("") == {}


@patch("dags.impl.poller_impl.get_airflow_variable", return_value="true")
@patch("dags.impl.poller_impl.get_cluster_ssh_hook")
def test_poll_slurm_job_states_skips_in_debug_mode(
    mock_get_cluster_ssh_hook: MagicMock,
    mock_get_airflow_variable: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the cluster is not polled if the debug variable is set."""
    poll_slurm_job_states(SSH_PREFIX)

    mock_get_cluster_ssh_hook.assert_not_called()


@patch("dags.impl.poller_impl.get_airflow_variable", return_value="false")
@patch("dags.impl.poller_impl.monotonic")
@patch("dags.impl.poller_impl.sleep")
@patch("dags.impl.poller_impl.update_slurm_job_states")
@patch("dags.impl.poller_impl.get_cluster_ssh_hook")
def test_poll_slurm_job_states_writes_states_and_reuses_hook(
    mock_get_cluster_ssh_hook: MagicMock,
    mock_update_slurm_job_states: MagicMock,
    mock_sleep: MagicMock,  # noqa: ARG001
    mock_monotonic: MagicMock,
    mock_get_airflow_variable: MagicMock,  # noqa: ARG001
) -> None:
    """Test that every iteration writes the parsed states, using the same hook."""
    # start time, then two iterations within and one after the loop duration
    mock_monotonic.side_effect = [0, 1, 2, 10_000]
    mock_hook = mock_get_cluster_ssh_hook.return_value
    mock_hook.exec_ssh_client_command.side_effect = [
        (0, b"123 RUNNING\n", b""),
        (1, b"", b"some error"),
    ]

    # when
    poll_slurm_job_states(SSH_PREFIX)

    # then
    mock_get_cluster_ssh_hook.assert_called_once_with(SSH_PREFIX, 0)
    mock_update_slurm_job_states.assert_called_once_with(SSH_PREFIX, {"123": "RUNNING"})
    mock_hook.client.close.assert_called_once()


@patch("dags.impl.poller_impl.get_airflow_variable", return_value="false")
@patch("dags.impl.poller_impl.monotonic")
@patch("dags.impl.poller_impl.sleep")
@patch("dags.impl.poller_impl.update_slurm_job_states")
@patch("dags.impl.poller_impl.get_cluster_ssh_hook")
def test_poll_slurm_job_states_reconnects_after_error(
    mock_get_cluster_ssh_hook: MagicMock,
    mock_update_slurm_job_states: MagicMock,
    mock_sleep: MagicMock,  # noqa: ARG001
    mock_monotonic: MagicMock,
    mock_get_airflow_variable: MagicMock,  # noqa: ARG001
) -> None:
    """Test that after an SSH error, a new hook for the next connection is used."""
    mock_monotonic.side_effect = [0, 1, 2, 10_000]
    mock_hook = mock_get_cluster_ssh_hook.return_value
    mock_hook.exec_ssh_client_command.side_effect = [
        SSHException("Timeout opening channel."),
        (0, b"123 PENDING", b""),
    ]

    # when
    poll_slurm_job_states(SSH_PREFIX)

    # then
    assert mock_get_cluster_ssh_hook.call_args_list == [
        call(SSH_PREFIX, 0),
        call(SSH_PREFIX, 1),
    ]
    mock_update_slurm_job_states.assert_called_once_with(SSH_PREFIX, {"123": "PENDING"})
