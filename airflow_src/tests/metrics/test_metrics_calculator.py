"""Tests for metrics calculation."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from plugins.metrics.metrics_calculator import (
    REPORTED_METRICS_FILE_NAME,
    _convert_numpy_types,
    _get_reported_metrics,
    calc_metrics,
)

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


def test_calc_metrics_custom() -> None:
    """Test calc_metrics calculates nothing for the custom metrics type."""
    output_dir = Path("/test/output")

    result = calc_metrics(output_dir, metrics_type=MetricsTypes.CUSTOM)

    assert result == {}


@patch("plugins.metrics.metrics_calculator.calc_diann_metrics")
def test_calc_metrics_diann(mock_diann: MagicMock) -> None:
    """Test calc_metrics with diann metrics type."""
    output_dir = Path("/test/output")
    expected_metrics = {"proteins": 500, "peptides": 3000}
    mock_diann.return_value = expected_metrics

    result = calc_metrics(output_dir, metrics_type=MetricsTypes.DIANN)

    mock_diann.assert_called_once_with(output_dir)
    assert result == {"proteins": 500, "peptides": 3000}


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


def test_convert_numpy_types_nan_python_float() -> None:
    """Test that Python float NaN is converted to None."""
    result = _convert_numpy_types({"val": float("nan")})

    assert result == {"val": None}


def test_convert_numpy_types_nan_numpy_float() -> None:
    """Test that numpy float NaN is converted to None."""
    result = _convert_numpy_types(
        {
            "f32": np.float32("nan"),
            "f64": np.float64("nan"),
        }
    )

    assert result == {"f32": None, "f64": None}


def test_convert_numpy_types_nan_in_nested_structure() -> None:
    """Test that NaN is converted to None in nested dicts and lists."""
    result = _convert_numpy_types(
        {
            "nested": {"a": float("nan"), "b": 1.0},
            "list": [np.float32("nan"), 2.0, float("nan")],
        }
    )

    assert result == {
        "nested": {"a": None, "b": 1.0},
        "list": [None, 2.0, None],
    }


def test_convert_numpy_types_raises_not_implemented_for_set() -> None:
    """Test _convert_numpy_types raises NotImplementedError for sets."""
    input_data = {"some_set": {1, 2, 3}}

    with pytest.raises(NotImplementedError, match="Tuples and sets are not supported"):
        _convert_numpy_types(input_data)


def test__get_reported_metrics_no_file(tmp_path: Path) -> None:
    """Test _get_reported_metrics returns empty dict if the file does not exist."""
    assert _get_reported_metrics(tmp_path) == {}


def test__get_reported_metrics_single_row(tmp_path: Path) -> None:
    """Test _get_reported_metrics reads the metrics of a single-row file."""
    (tmp_path / REPORTED_METRICS_FILE_NAME).write_text(
        "proteins,fwhm.rt,name\n8123,4.2,some_name\n"
    )

    result = _get_reported_metrics(tmp_path)

    assert result == {"proteins": 8123, "fwhm:rt": 4.2, "name": "some_name"}
    assert isinstance(result["proteins"], int)


def test__get_reported_metrics_multiple_rows(tmp_path: Path) -> None:
    """Test _get_reported_metrics uses the first row of a multi-row file."""
    (tmp_path / REPORTED_METRICS_FILE_NAME).write_text(
        "proteins,precursors\n8123,95012\n8090,94500\n"
    )

    assert _get_reported_metrics(tmp_path) == {"proteins": 8123, "precursors": 95012}


def test__get_reported_metrics_header_only(tmp_path: Path) -> None:
    """Test _get_reported_metrics returns empty dict for a file without data rows."""
    (tmp_path / REPORTED_METRICS_FILE_NAME).write_text("proteins,precursors\n")

    assert _get_reported_metrics(tmp_path) == {}


def test__get_reported_metrics_nan_value(tmp_path: Path) -> None:
    """Test _get_reported_metrics converts missing values to None."""
    (tmp_path / REPORTED_METRICS_FILE_NAME).write_text("proteins,precursors\n8123,\n")

    assert _get_reported_metrics(tmp_path) == {"proteins": 8123, "precursors": None}


@patch("plugins.metrics.metrics_calculator.calc_msqc_metrics")
def test_calc_metrics_merges_reported_metrics(
    mock_msqc: MagicMock, tmp_path: Path
) -> None:
    """Test calc_metrics adds the metrics the quanting software reported itself."""
    mock_msqc.return_value = {"calculated": 1}
    (tmp_path / REPORTED_METRICS_FILE_NAME).write_text("reported\n2\n")

    result = calc_metrics(tmp_path, metrics_type=MetricsTypes.MSQC)

    assert result == {"calculated": 1, "reported": 2}


def test_calc_metrics_custom_only_reported_metrics(tmp_path: Path) -> None:
    """Test calc_metrics returns only reported metrics for the custom metrics type."""
    (tmp_path / REPORTED_METRICS_FILE_NAME).write_text("reported\n2\n")

    result = calc_metrics(tmp_path, metrics_type=MetricsTypes.CUSTOM)

    assert result == {"reported": 2}


@patch("plugins.metrics.metrics_calculator.calc_msqc_metrics")
def test_calc_metrics_reported_metrics_win_on_name_clash(
    mock_msqc: MagicMock, tmp_path: Path
) -> None:
    """Test calc_metrics lets the reported metrics override the calculated ones."""
    mock_msqc.return_value = {"proteins": 8000, "other": 1}
    (tmp_path / REPORTED_METRICS_FILE_NAME).write_text("proteins\n8123\n")

    result = calc_metrics(tmp_path, metrics_type=MetricsTypes.MSQC)

    assert result == {"proteins": 8123, "other": 1}
