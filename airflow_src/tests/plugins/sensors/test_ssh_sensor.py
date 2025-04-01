"""Tests for the SSH sensor plugin."""

from unittest.mock import MagicMock, patch

from plugins.common.keys import JobStates
from plugins.sensors.ssh_sensor import WaitForJobFinishSensor


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
