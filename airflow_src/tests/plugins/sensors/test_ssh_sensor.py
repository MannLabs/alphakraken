"""Tests for the SSH sensor plugin."""

from unittest.mock import MagicMock, patch

from airflow.utils.xcom import XCOM_RETURN_KEY
from plugins.common.keys import JobStates
from plugins.sensors.ssh_sensor import WaitForJobFinishSensor

SOURCE_TASK_ID = "processing.submit_job"


@patch("plugins.sensors.ssh_sensor.get_job_status")
def test_poke_executes_ssh_command_and_checks_returned_state(
    mock_get_job_status: MagicMock,
) -> None:
    """Test that the poke function returns False when the returned state is in the running states."""
    # given
    mock_ti = MagicMock()
    mock_ti.map_index = 0
    mock_ti.xcom_pull.return_value = "12345"
    mock_get_job_status.return_value = JobStates.RUNNING
    context = {"ti": mock_ti}
    operator = WaitForJobFinishSensor(
        task_id="my_task", xcom_source_task_id=SOURCE_TASK_ID
    )

    operator.pre_execute(context)

    # then
    mock_ti.xcom_pull.assert_called_once_with(
        key=XCOM_RETURN_KEY, task_ids=SOURCE_TASK_ID, map_indexes=0
    )
    assert not operator.poke(context)


@patch("plugins.sensors.ssh_sensor.get_job_status")
def test_poke_returns_true_when_state_not_in_running_states(
    mock_get_job_status: MagicMock,
) -> None:
    """Test that the poke function returns True when the returned state is not in the running states."""
    # given
    mock_ti = MagicMock()
    mock_ti.map_index = 2
    mock_ti.xcom_pull.return_value = "12345"
    mock_get_job_status.return_value = JobStates.COMPLETED
    context = {"ti": mock_ti}
    operator = WaitForJobFinishSensor(
        task_id="my_task", xcom_source_task_id=SOURCE_TASK_ID
    )

    operator.pre_execute(context)

    # then
    mock_ti.xcom_pull.assert_called_once_with(
        key=XCOM_RETURN_KEY, task_ids=SOURCE_TASK_ID, map_indexes=2
    )
    assert operator.poke(context)
