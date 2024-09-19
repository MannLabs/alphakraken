"""Unit tests for the 'utils' module."""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
import pytz
from airflow.models import Variable
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
    ti.xcom_push.assert_any_call("key1", "value1")


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
    result = get_xcom(ti, "key1")
    assert result == "value1"

    ti.xcom_pull.assert_called_once_with(key="key1", default=None)


def test_xcom_pull_with_missing_key_raises_error() -> None:
    """Test that get_xcom raises a ValueError when trying to pull a value for a missing key from XCom."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)
    # when
    with pytest.raises(KeyError):
        get_xcom(ti, "missing_key")


def test_xcom_pull_with_missing_key_gives_default() -> None:
    """Test that get_xcom raises a ValueError when trying to pull a value for a missing key from XCom."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value="some_default")
    # when
    assert get_xcom(ti, "missing_key", "some_default") == "some_default"

    ti.xcom_pull.assert_called_once_with(key="missing_key", default="some_default")


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


@patch("plugins.common.utils.datetime")
@patch("plugins.common.utils.trigger_dag")
def test_trigger_dag_run(mock_trigger_dag: MagicMock, mock_datetime: MagicMock) -> None:
    """Test that trigger_dag_run triggers a DAG run with the given configuration."""
    mock_datetime.now.return_value = datetime.fromtimestamp(0, tz=pytz.utc)

    # when
    trigger_dag_run("dag_id", {"key": "value"})

    mock_trigger_dag.assert_called_once_with(
        dag_id="dag_id",
        run_id="manual__1970-01-01T01:00:00",
        conf={"key": "value"},
        execution_date=None,
        replace_microseconds=False,
    )


@patch("plugins.common.utils.datetime")
@patch("plugins.common.utils.trigger_dag")
def test_trigger_dag_run_with_delay(
    mock_trigger_dag: MagicMock, mock_datetime: MagicMock
) -> None:
    """Test that trigger_dag_run triggers a DAG run with the given configuration and time delay."""
    mock_datetime.now.return_value = datetime.fromtimestamp(0, tz=pytz.utc)

    # when
    trigger_dag_run("dag_id", {"key": "value"}, 10)

    mock_trigger_dag.assert_called_once_with(
        dag_id="dag_id",
        run_id="manual__1970-01-01T01:00:00",
        conf={"key": "value"},
        execution_date=datetime(1970, 1, 1, 11, 0, tzinfo=pytz.utc),
        replace_microseconds=False,
    )


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
