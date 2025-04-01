"""Tests for the SSH sensor plugin."""

from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from plugins.common.keys import JobStates
from plugins.sensors.ssh_sensor import WaitForJobFinishSensor
from plugins.sensors.ssh_utils import ssh_execute


@patch("plugins.sensors.ssh_sensor.get_job_handler")
@patch("plugins.sensors.ssh_sensor.get_xcom")
def test_poke_executes_ssh_command_and_checks_returned_state(
    mock_get_xcom: MagicMock,
    mock_get_job_handler: MagicMock,
) -> None:
    """Test that the poke function returns False when the returned state is in the running states."""
    # given
    mock_get_xcom.return_value = "12345"
    mock_get_job_handler.return_value.get_job_status.return_value = JobStates.RUNNING
    context = {"ti": MagicMock()}
    # with patch.dict("os.environ", AIRFLOW_CONN_CLUSTER_SSH_CONNECTION=fixture_cluster_ssh_connection_uri):
    operator = WaitForJobFinishSensor(task_id="my_task")

    operator.pre_execute(context)

    # when
    result = operator.poke(context)

    # then
    assert not result


@patch("plugins.sensors.ssh_sensor.get_job_handler")
@patch("plugins.sensors.ssh_sensor.get_xcom")
def test_poke_returns_true_when_state_not_in_running_states(
    mock_get_xcom: MagicMock,
    mock_get_job_handler: MagicMock,
) -> None:
    """Test that the poke function returns True when the returned state is not in the running states."""
    # given
    mock_get_xcom.return_value = "12345"

    mock_get_job_handler.return_value.get_job_status.return_value = JobStates.COMPLETED
    context = {"ti": MagicMock()}
    operator = WaitForJobFinishSensor(task_id="my_task")

    operator.pre_execute(context)

    # when
    result = operator.poke(context)

    assert result


# TODO: move test to test_ssh_utils.py
def test_ssh_execute_returns_decoded_output() -> None:
    """Test that the ssh_execute function returns the decoded output of the command."""
    # given
    ssh_hook = MagicMock()
    ssh_hook.exec_ssh_client_command.return_value = (0, b"command output", b"")

    # when
    result = ssh_execute("my_command", ssh_hook)

    # then
    assert result == "command output"
    ssh_hook.exec_ssh_client_command.assert_called_once_with(
        ssh_hook.get_conn.return_value,
        "my_command",
        timeout=60,
        get_pty=False,
        environment={},
    )


# TODO: move test to test_ssh_utils.py
@patch("plugins.sensors.ssh_utils.sleep")
def test_ssh_execute_multiple_tries(mock_sleep: MagicMock) -> None:
    """ssh_execute returns the decoded output of the command SSH command returns 254 for some time."""
    # given
    ssh_hook = MagicMock()

    bad_return1 = (254, b"command output", b"")
    bad_return2 = (0, b"", b"")
    ssh_hook.exec_ssh_client_command.side_effect = (
        [bad_return1] * 2 + [bad_return2] * 2 + [(0, b"command output", b"")]
    )

    # when
    result = ssh_execute("my_command", ssh_hook)

    # then
    assert result == "command output"
    assert mock_sleep.call_count == 5  # noqa: PLR2004
    assert ssh_hook.exec_ssh_client_command.call_count == 5  # noqa: PLR2004

    expected_call = call(
        ssh_hook.get_conn.return_value,
        "my_command",
        timeout=60,
        get_pty=False,
        environment={},
    )
    ssh_hook.exec_ssh_client_command.assert_has_calls(
        [expected_call, expected_call, expected_call, expected_call]
    )


# TODO: move test to test_ssh_utils.py
@patch("plugins.sensors.ssh_utils.sleep")
def test_ssh_execute_too_many_tries(mock_sleep: MagicMock) -> None:
    """ssh_execute returns the decoded output of the command SSH command returns 254 for some time."""
    # given
    ssh_hook = MagicMock()
    bad_return = (254, b"command output", b"")
    ssh_hook.exec_ssh_client_command.side_effect = [bad_return] * 11

    # when
    with pytest.raises(AirflowFailException):
        ssh_execute("my_command", ssh_hook)

    assert mock_sleep.call_count == 10  # noqa: PLR2004
    assert ssh_hook.exec_ssh_client_command.call_count == 10  # noqa: PLR2004
