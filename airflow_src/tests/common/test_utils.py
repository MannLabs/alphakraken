"""Unit tests for the 'utils' module."""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
import pytz
from airflow.exceptions import DagNotFound
from airflow.sdk import Variable
from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowRuntimeError,
    ErrorType,
)
from airflow.sdk.execution_time.comms import ErrorResponse, OKResponse, TriggerDagRun
from plugins.common.utils import (
    get_airflow_variable,
    get_env_variable,
    get_xcom,
    put_xcom,
    trigger_dag_run,
    truncate_string,
)


def test_xcom_push_successful() -> None:
    """Test that put_xcom successfully pushes key-value pairs to XCom."""
    ti = Mock()
    ti.xcom_push = Mock()
    # when
    put_xcom(ti, "key1", "value1")
    ti.xcom_push.assert_called_once_with("key1", "value1")


def test_xcom_push_with_none_value_raises_error() -> None:
    """Test that put_xcom raises a ValueError when trying to push a None value to XCom."""
    ti = Mock()
    with pytest.raises(ValueError):
        # when
        put_xcom(ti, "key1", None)


def test_xcom_pull_successful() -> None:
    """Test that get_xcom successfully pulls values from XCom for given keys."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value="value1")
    # when
    result = get_xcom(ti, "key1", task_ids="task1")
    assert result == "value1"

    ti.xcom_pull.assert_called_once_with(key="key1", task_ids="task1")


def test_xcom_pull_with_missing_key_raises_error() -> None:
    """Test that get_xcom raises a KeyError when trying to pull a value for a missing key from XCom."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)
    # when
    with pytest.raises(KeyError):
        get_xcom(ti, "missing_key", task_ids="task1")


def test_xcom_pull_forwards_default_to_airflow() -> None:
    """Test that get_xcom forwards `default` and returns airflow's answer, not its own default."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value="value_from_airflow")
    # when
    result = get_xcom(ti, "some_key", task_ids="task1", default="my_default")

    assert result == "value_from_airflow"
    ti.xcom_pull.assert_called_once_with(
        key="some_key", task_ids="task1", default="my_default"
    )


def test_xcom_pull_requires_task_ids() -> None:
    """Test that task_ids is mandatory and keyword-only.

    Omitting it silently changes meaning between airflow 2 (any task) and airflow 3 (calling task),
    and passing it positionally would bind to `default`.
    """
    ti = Mock()

    with pytest.raises(TypeError):
        get_xcom(ti, "key1")  # type: ignore[call-arg]

    with pytest.raises(TypeError):
        get_xcom(ti, "key1", "task1")  # type: ignore[misc]


def test_xcom_pull_with_none_default() -> None:
    """Test that get_xcom returns None when default=None and key is missing."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)

    result = get_xcom(ti, "missing_key", task_ids="task1", default=None)

    assert result is None
    ti.xcom_pull.assert_called_once_with(
        key="missing_key", task_ids="task1", default=None
    )


def test_xcom_pull_returns_default_when_airflow_ignores_it() -> None:
    """Test that the default is applied even when xcom_pull returns None despite being given one.

    Airflow 3 ignores `default` on the code path taken when `map_indexes` is not passed.
    """
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)

    # when
    assert get_xcom(ti, "key1", task_ids="task1", default=[]) == []


def test_xcom_pull_raises_when_no_value_and_no_default() -> None:
    """Test that a missing value without a default still raises."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)

    # when
    with pytest.raises(KeyError):
        get_xcom(ti, "key1", task_ids="task1")


def test_xcom_pull_with_task_ids() -> None:
    """Test that get_xcom passes task_ids to xcom_pull."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value="job_123")

    result = get_xcom(ti, "return_value", task_ids="processing.submit_job")

    assert result == "job_123"
    ti.xcom_pull.assert_called_once_with(
        key="return_value", task_ids="processing.submit_job"
    )


def test_xcom_pull_with_map_indexes() -> None:
    """Test that get_xcom passes map_indexes to xcom_pull."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value="branch_error")

    result = get_xcom(
        ti, "branch_errors", task_ids="some_task", map_indexes=2, default=None
    )

    assert result == "branch_error"
    ti.xcom_pull.assert_called_once_with(
        key="branch_errors", task_ids="some_task", default=None, map_indexes=2
    )


@patch.object(Variable, "get")
def test_get_airflow_variable_returns_value_when_default_not_set(
    mock_get: MagicMock,
) -> None:
    """Test that get_airflow_variable returns the value of an Airflow Variable with a given key."""
    mock_get.return_value = "value"

    # when
    result = get_airflow_variable("my_key")

    assert result == "value"
    mock_get.assert_called_once_with("my_key")


def test_get_airflow_variable_returns_default_when_value_not_found() -> None:
    """Test that get_airflow_variable returns the default value when the value of an Airflow Variable with a given key is not found."""
    # when
    result = get_airflow_variable("not_existing_var", "default_value")

    assert result == "default_value"


@patch("os.getenv")
def test_get_env_variable_returns_value_when_default_not_set(
    mock_getenv: MagicMock,
) -> None:
    """Test that get_env_variable returns the value of an environment variable with a given key."""
    mock_getenv.return_value = "value"

    # when
    result = get_env_variable("my_key")

    assert result == "value"
    mock_getenv.assert_called_once_with("my_key", default=None)


def test_get_env_variable_returns_default_when_value_not_found() -> None:
    """Test that get_env_variable returns the default value when the value of an environment variable with a given key is not found."""
    # when
    result = get_env_variable("not_existing_env_var", "default_value")

    assert result == "default_value"


def test_get_env_variable_raises_when_value_not_found() -> None:
    """Test that get_env_variable returns the default value when the value of an environment variable with a given key is not found."""
    # when
    with pytest.raises(KeyError):
        get_env_variable("not_existing_env_var")


def _sent_trigger_message(mock_task_runner: MagicMock) -> TriggerDagRun:
    mock_task_runner.SUPERVISOR_COMMS.send.assert_called_once()
    return mock_task_runner.SUPERVISOR_COMMS.send.call_args.args[0]


@patch("plugins.common.utils.datetime")
@patch("plugins.common.utils.task_runner")
def test_trigger_dag_run(mock_task_runner: MagicMock, mock_datetime: MagicMock) -> None:
    """Test that trigger_dag_run sends a TriggerDagRun message with the given configuration."""
    mock_datetime.now.return_value = datetime.fromtimestamp(0, tz=pytz.utc)
    mock_task_runner.SUPERVISOR_COMMS.send.return_value = OKResponse(ok=True)

    # when
    trigger_dag_run("dag_id", {"key": "value"})

    msg = _sent_trigger_message(mock_task_runner)
    assert isinstance(msg, TriggerDagRun)
    assert msg.dag_id == "dag_id"
    assert msg.run_id.startswith("manual__1970-01-01T00:00:00+00:00_")
    assert msg.conf == {"key": "value"}
    assert msg.logical_date is None
    assert msg.run_after == datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc)


@patch("plugins.common.utils.datetime")
@patch("plugins.common.utils.task_runner")
def test_trigger_dag_run_with_delay(
    mock_task_runner: MagicMock, mock_datetime: MagicMock
) -> None:
    """Test that the time delay is passed as run_after."""
    mock_datetime.now.return_value = datetime.fromtimestamp(0, tz=pytz.utc)
    mock_task_runner.SUPERVISOR_COMMS.send.return_value = OKResponse(ok=True)

    # when
    trigger_dag_run("dag_id", {"key": "value"}, 10)

    msg = _sent_trigger_message(mock_task_runner)
    assert msg.run_after == datetime(1970, 1, 1, 0, 10, tzinfo=pytz.utc)
    assert msg.run_id.startswith("manual__1970-01-01T00:10:00+00:00_")


@patch("plugins.common.utils.task_runner")
def test_trigger_dag_run_raises_dag_not_found_on_404(
    mock_task_runner: MagicMock,
) -> None:
    """Test that a 404 of the api-server surfaces as DagNotFound."""
    mock_task_runner.SUPERVISOR_COMMS.send.side_effect = AirflowRuntimeError(
        ErrorResponse(error=ErrorType.API_SERVER_ERROR, detail={"status_code": 404})
    )

    with pytest.raises(DagNotFound):
        # when
        trigger_dag_run("dag_id", {"key": "value"})


@patch("plugins.common.utils.task_runner")
def test_trigger_dag_run_reraises_other_runtime_errors(
    mock_task_runner: MagicMock,
) -> None:
    """Test that api-server errors other than 404 are re-raised unchanged."""
    mock_task_runner.SUPERVISOR_COMMS.send.side_effect = AirflowRuntimeError(
        ErrorResponse(error=ErrorType.API_SERVER_ERROR, detail={"status_code": 500})
    )

    with pytest.raises(AirflowRuntimeError):
        # when
        trigger_dag_run("dag_id", {"key": "value"})


@patch("plugins.common.utils.task_runner")
def test_trigger_dag_run_fails_when_run_already_exists(
    mock_task_runner: MagicMock,
) -> None:
    """Test that an ErrorResponse (e.g. run id collision) fails the task."""
    mock_task_runner.SUPERVISOR_COMMS.send.return_value = ErrorResponse(
        error=ErrorType.DAGRUN_ALREADY_EXISTS
    )

    with pytest.raises(AirflowFailException, match="DAGRUN_ALREADY_EXISTS"):
        # when
        trigger_dag_run("dag_id", {"key": "value"})


def test_truncate_string_returns_none_if_input_is_none() -> None:
    """Test that truncate_string returns None if the input string is None."""
    assert truncate_string(None) is None


def test_truncate_string_handles_edge_case_of_empty_string() -> None:
    """Test that truncate_string handles the edge case of an empty string."""
    assert truncate_string("", 200) == ""


def test_truncate_string_returns_input_if_length_is_less_than_n() -> None:
    """Test that truncate_string returns the input string if its length is less than n."""
    input_string = "short string"
    assert truncate_string(input_string, 20) == input_string


def test_truncate_string_truncates_correctly_if_length_is_greater_than_n() -> None:
    """Test that truncate_string truncates the input string correctly if its length is greater than n."""
    input_string = "a" * 300
    expected_output = "a" * 100 + " ... " + "a" * 100
    assert truncate_string(input_string, 200) == expected_output


def test_truncate_string_handles_edge_case_of_exactly_n_characters() -> None:
    """Test that truncate_string handles the edge case of exactly n characters."""
    input_string = "a" * 200
    assert truncate_string(input_string, 200) == input_string
