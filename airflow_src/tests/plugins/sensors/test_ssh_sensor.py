"""Tests for the SSH sensor plugin."""

from collections.abc import Callable
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.utils.xcom import XCOM_RETURN_KEY
from plugins.common.keys import JobStates
from plugins.common.quanting_env import QuantingEnv
from plugins.sensors.ssh_sensor import WaitForJobFinishSensor

JOB_ID_SOURCE_TASK_ID = "processing.submit_job"
QUANTING_ENV_SOURCE_TASK_ID = "processing.prepare_job"
RUNNER_NAME = "file_based"


@patch("plugins.sensors.ssh_sensor.get_job_status")
def test_poke_executes_ssh_command_and_checks_returned_state(
    mock_get_job_status: MagicMock,
    make_quanting_env: Callable[..., QuantingEnv],
) -> None:
    """Test that the poke function returns False when the returned state is in the running states."""
    # given
    mock_ti = MagicMock()
    mock_ti.map_index = 0
    mock_ti.xcom_pull.side_effect = [
        "12345",
        make_quanting_env(runner=RUNNER_NAME).to_dict(),
    ]
    mock_get_job_status.return_value = JobStates.RUNNING
    context = {"ti": mock_ti}
    operator = WaitForJobFinishSensor(
        task_id="my_task",
        xcom_source_task_id=JOB_ID_SOURCE_TASK_ID,
        quanting_env_source_task_id=QUANTING_ENV_SOURCE_TASK_ID,
    )

    operator.pre_execute(context)

    # then
    mock_ti.xcom_pull.assert_has_calls(
        [
            call(key=XCOM_RETURN_KEY, task_ids=JOB_ID_SOURCE_TASK_ID, map_indexes=0),
            call(
                key=XCOM_RETURN_KEY,
                task_ids=QUANTING_ENV_SOURCE_TASK_ID,
                map_indexes=0,
            ),
        ]
    )
    assert not operator.poke(context)
    mock_get_job_status.assert_called_once_with("12345", RUNNER_NAME)


@pytest.mark.parametrize(
    ("job_status", "expected_poke"),
    [
        (JobStates.COMPLETED, True),
        (JobStates.FAILED, True),
        (JobStates.TIMEOUT, True),
        (JobStates.OUT_OF_MEMORY, True),
        ("some_other_state", True),
        (JobStates.PENDING, False),
        (JobStates.RUNNING, False),
        (JobStates.COMPLETING, False),
    ],
)
@patch("plugins.sensors.ssh_sensor.get_job_status")
def test_poke_returns_true_when_state_not_in_running_states(
    mock_get_job_status: MagicMock,
    make_quanting_env: Callable[..., QuantingEnv],
    job_status: str,
    *,
    expected_poke: bool,
) -> None:
    """Test that the poke function stops only on terminal states, not on transient COMPLETING."""
    # given
    mock_ti = MagicMock()
    mock_ti.map_index = 2
    mock_ti.xcom_pull.side_effect = [
        "12345",
        make_quanting_env(runner=RUNNER_NAME).to_dict(),
    ]
    mock_get_job_status.return_value = job_status
    context = {"ti": mock_ti}
    operator = WaitForJobFinishSensor(
        task_id="my_task",
        xcom_source_task_id=JOB_ID_SOURCE_TASK_ID,
        quanting_env_source_task_id=QUANTING_ENV_SOURCE_TASK_ID,
    )

    operator.pre_execute(context)

    # then
    mock_ti.xcom_pull.assert_has_calls(
        [
            call(key=XCOM_RETURN_KEY, task_ids=JOB_ID_SOURCE_TASK_ID, map_indexes=2),
            call(
                key=XCOM_RETURN_KEY,
                task_ids=QUANTING_ENV_SOURCE_TASK_ID,
                map_indexes=2,
            ),
        ]
    )
    assert operator.poke(context) is expected_poke
    mock_get_job_status.assert_called_once_with("12345", RUNNER_NAME)
