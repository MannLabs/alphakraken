"""Tests for the SSH sensor plugin."""

from unittest.mock import MagicMock, patch

from airflow.utils.xcom import XCOM_RETURN_KEY
from plugins.common.keys import JobStates
from plugins.sensors.ssh_sensor import WaitForJobFinishSensor

SOURCE_TASK_ID = "quanting_pipeline.run_quanting"


@patch("plugins.sensors.ssh_sensor.get_job_status")
@patch("plugins.sensors.ssh_sensor.get_xcom")
def test_poke_executes_ssh_command_and_checks_returned_state(
    mock_get_xcom: MagicMock,
    mock_get_job_status: MagicMock,
) -> None:
    """Test that the poke function returns False when the returned state is in the running states."""
    # given
    mock_get_xcom.return_value = "12345"
    mock_get_job_status.return_value = JobStates.RUNNING
    context = {"ti": MagicMock()}
    operator = WaitForJobFinishSensor(
        task_id="my_task", xcom_source_task_id=SOURCE_TASK_ID
    )

    operator.pre_execute(context)

    # then
    mock_get_xcom.assert_called_once_with(
        context["ti"], XCOM_RETURN_KEY, task_ids=SOURCE_TASK_ID
    )
    assert not operator.poke(context)


@patch("plugins.sensors.ssh_sensor.get_job_status")
@patch("plugins.sensors.ssh_sensor.get_xcom")
def test_poke_returns_true_when_state_not_in_running_states(
    mock_get_xcom: MagicMock,
    mock_get_job_status: MagicMock,
) -> None:
    """Test that the poke function returns True when the returned state is not in the running states."""
    # given
    mock_get_xcom.return_value = "12345"
    mock_get_job_status.return_value = JobStates.COMPLETED
    context = {"ti": MagicMock()}
    operator = WaitForJobFinishSensor(
        task_id="my_task", xcom_source_task_id=SOURCE_TASK_ID
    )

    operator.pre_execute(context)

    # then
    mock_get_xcom.assert_called_once_with(
        context["ti"], XCOM_RETURN_KEY, task_ids=SOURCE_TASK_ID
    )
    assert operator.poke(context)
