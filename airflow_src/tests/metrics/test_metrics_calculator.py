"""Tests for metrics calculation."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from plugins.metrics.metrics_calculator import _convert_numpy_types, calc_metrics

from shared.keys import MetricsTypes


@patch("plugins.metrics.metrics_calculator.calc_alphadia_metrics")
def test_calc_metrics_alphadia(mock_alphadia: MagicMock) -> None:
    """Test calc_metrics with alphadia metrics type."""
    output_dir = Path("/test/output")
    expected_metrics = {"test_metric": 1.0, "another.metric": 2.5}
    mock_alphadia.return_value = expected_metrics

    result = calc_metrics(output_dir, metrics_type=MetricsTypes.ALPHADIA)

    mock_alphadia.assert_called_once_with(output_dir)
    assert result == {"test_metric": 1.0, "another:metric": 2.5}


@patch("plugins.metrics.metrics_calculator.calc_custom_metrics")
def test_calc_metrics_custom(mock_custom: MagicMock) -> None:
    """Test calc_metrics with custom metrics type."""
    output_dir = Path("/test/output")
    expected_metrics = {"custom_metric": 3.0, "custom.metric": 4.5}
    mock_custom.return_value = expected_metrics

    result = calc_metrics(output_dir, metrics_type=MetricsTypes.CUSTOM)

    mock_custom.assert_called_once_with(output_dir)
    assert result == {"custom_metric": 3.0, "custom:metric": 4.5}


def test_calc_metrics_invalid_type() -> None:
    """Test calc_metrics with invalid metrics type raises KeyError."""
    output_dir = Path("/test/output")

    with pytest.raises(KeyError):
        calc_metrics(output_dir, metrics_type="invalid_type")


def test_convert_numpy_types_comprehensive() -> None:
    """Test _convert_numpy_types with complex nested data covering all branches."""
    input_data = {
        "numpy_int32": np.int32(42),
        "numpy_int64": np.int64(100),
        "numpy_float32": np.float32(3.14),
        "numpy_float64": np.float64(2.71828),
        "numpy_bool": np.bool_(True),  # noqa: FBT003
        "numpy_array_1d": np.array([1, 2, 3]),
        "numpy_array_2d": np.array([[1, 2], [3, 4]]),
        "nested_dict": {
            "inner_numpy_int": np.int16(7),
            "inner_list": [np.float32(1.5), np.int8(10), "string"],
            "inner_array": np.array([5, 6]),
        },
        "list_with_numpy": [
            np.int32(1),
            np.float64(2.5),
            {"nested_numpy": np.int64(99)},
            [np.bool_(False), np.array([7, 8, 9])],  # noqa: FBT003
        ],
        "python_int": 123,
        "python_float": 45.67,
        "python_str": "hello",
        "python_bool": False,
        "python_none": None,
    }

    result = _convert_numpy_types(input_data)

    expected = {
        "numpy_int32": 42,
        "numpy_int64": 100,
        "numpy_float32": pytest.approx(3.14, rel=1e-5),
        "numpy_float64": pytest.approx(2.71828, rel=1e-5),
        "numpy_bool": True,
        "numpy_array_1d": [1, 2, 3],
        "numpy_array_2d": [[1, 2], [3, 4]],
        "nested_dict": {
            "inner_numpy_int": 7,
            "inner_list": [pytest.approx(1.5, rel=1e-5), 10, "string"],
            "inner_array": [5, 6],
        },
        "list_with_numpy": [
            1,
            pytest.approx(2.5, rel=1e-5),
            {"nested_numpy": 99},
            [False, [7, 8, 9]],
        ],
        "python_int": 123,
        "python_float": 45.67,
        "python_str": "hello",
        "python_bool": False,
        "python_none": None,
    }

    assert result == expected

    assert isinstance(result["numpy_int32"], int)
    assert isinstance(result["numpy_float32"], float)
    assert isinstance(result["numpy_bool"], bool)
    assert isinstance(result["numpy_array_1d"], list)
    assert isinstance(result["numpy_array_2d"], list)
    assert isinstance(result["nested_dict"]["inner_numpy_int"], int)


def test_convert_numpy_types_raises_not_implemented_for_set() -> None:
    """Test _convert_numpy_types raises NotImplementedError for sets."""
    input_data = {"some_set": {1, 2, 3}}

    with pytest.raises(NotImplementedError, match="Tuples and sets are not supported"):
        _convert_numpy_types(input_data)
