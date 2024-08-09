"""Tests for the SSH sensor plugin."""

from unittest.mock import MagicMock, patch

from plugins.sensors.ssh_sensor import QuantingSSHSensor, SSHSensorOperator


@patch.object(SSHSensorOperator, "ssh_execute")
@patch("plugins.sensors.ssh_sensor.get_xcom")
def test_poke_executes_ssh_command_and_checks_returned_state(
    mock_get_xcom: MagicMock, mock_ssh_execute: MagicMock
) -> None:
    """Test that the poke function returns False when the returned state is in the running states."""
    # given
    mock_get_xcom.return_value = "12345"
    mock_ssh_execute.return_value = "RUNNING"
    context = {"ti": MagicMock()}
    ssh_hook = MagicMock()
    operator = QuantingSSHSensor(task_id="my_task", ssh_hook=ssh_hook)

    operator.pre_execute(context)

    # when
    result = operator.poke(context)

    # then
    assert not result


@patch.object(SSHSensorOperator, "ssh_execute")
@patch("plugins.sensors.ssh_sensor.get_xcom")
def test_poke_returns_true_when_state_not_in_running_states(
    mock_get_xcom: MagicMock, mock_ssh_execute: MagicMock
) -> None:
    """Test that the poke function returns True when the returned state is not in the running states."""
    # given
    mock_get_xcom.return_value = "12345"
    mock_ssh_execute.return_value = "COMPLETED"
    context = {"ti": MagicMock()}
    ssh_hook = MagicMock()
    operator = QuantingSSHSensor(task_id="my_task", ssh_hook=ssh_hook)

    operator.pre_execute(context)

    # when
    result = operator.poke(context)

    # then
    assert result


def test_ssh_execute_returns_decoded_output() -> None:
    """Test that the ssh_execute function returns the decoded output of the command."""
    # given
    ssh_hook = MagicMock()
    ssh_hook.exec_ssh_client_command.return_value = (0, b"command output", b"")

    # when
    result = SSHSensorOperator.ssh_execute("my_command", ssh_hook)

    # then
    assert result == "command output"
    ssh_hook.exec_ssh_client_command.assert_called_once_with(
        ssh_hook.get_conn.return_value,
        "my_command",
        timeout=60,
        get_pty=False,
        environment={},
    )
