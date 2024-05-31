"""Unit tests for the 'utils' module."""

# ruff: noqa: ANN201 Missing return type annotation
from unittest.mock import Mock

import pytest

from shared.utils import get_xcom, put_xcom


def test_xcom_push_successful():
    """Test that put_xcom successfully pushes key-value pairs to XCom."""
    ti = Mock()
    ti.xcom_push = Mock()
    # when
    put_xcom(ti, {"key1": "value1", "key2": "value2"})
    ti.xcom_push.assert_any_call("key1", "value1")
    ti.xcom_push.assert_any_call("key2", "value2")


def test_xcom_push_with_none_value_raises_error():
    """Test that put_xcom raises a ValueError when trying to push a None value to XCom."""
    ti = Mock()
    with pytest.raises(ValueError):
        # when
        put_xcom(ti, {"key1": None})


def test_xcom_push_with_empty_dict_does_nothing():
    """Test that put_xcom does not push anything to XCom when given an empty dictionary."""
    ti = Mock()
    ti.xcom_push = Mock()
    # when
    put_xcom(ti, {})
    ti.xcom_push.assert_not_called()


def test_xcom_pull_successful():
    """Test that get_xcom successfully pulls values from XCom for given keys."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value="value")
    # when
    result = get_xcom(ti, ["key1", "key2"])
    assert result == {"key1": "value", "key2": "value"}


def test_xcom_pull_with_missing_key_raises_error():
    """Test that get_xcom raises a ValueError when trying to pull a value for a missing key from XCom."""
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)
    # when
    with pytest.raises(ValueError):
        get_xcom(ti, ["missing_key"])


def test_xcom_pull_with_empty_keys_returns_empty_dict():
    """Test that get_xcom returns an empty dictionary when given an empty list of keys."""
    ti = Mock()
    # when
    result = get_xcom(ti, [])
    assert result == {}
