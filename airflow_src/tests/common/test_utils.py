"""Unit tests for the 'utils' module."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.models import Variable
from plugins.common.utils import get_env_variable, get_variable, get_xcom, put_xcom


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


def test_xcom_pull_with_missing_key_raises_error() -> None:
    """Test that get_xcom raises a ValueError when trying to pull a value for a missing key from XCom."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)
    # when
    with pytest.raises(ValueError):
        get_xcom(ti, "missing_key")


@patch.object(Variable, "get")
def test_get_variable_returns_value_when_default_not_set(mock_get: MagicMock) -> None:
    """Test that get_variable returns the value of an Airflow Variable with a given key."""
    mock_get.return_value = "value"

    # when
    result = get_variable("my_key")

    assert result == "value"
    mock_get.assert_called_once_with("my_key")


def test_get_variable_returns_default_when_value_not_found() -> None:
    """Test that get_variable returns the default value when the value of an Airflow Variable with a given key is not found."""
    # when
    result = get_variable("not_existing_var", "default_value")

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
    with pytest.raises(ValueError):
        get_env_variable("not_existing_env_var")
