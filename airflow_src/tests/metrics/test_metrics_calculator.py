"""Tests for metrics calculation."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from plugins.metrics.metrics_calculator import calc_metrics

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
