"""Tests for the response shapes shared by the read APIs."""

from shared.api_responses import to_metrics_dict
from shared.tests import factories


def test_to_metrics_dict() -> None:
    """Test that the dict holds the type, the values and the output folder as users see it."""
    metrics = factories.metrics(
        type="alphadia",
        relative_output_path="P1/out_f.raw/alphadia",
        output_display_base_path="/fs/pool/output",
        values={"proteins": 5000},
    )

    assert to_metrics_dict(metrics) == {
        "type": "alphadia",
        "proteins": 5000,
        "output_path": "/fs/pool/output/P1/out_f.raw/alphadia",
    }


def test_to_metrics_dict_without_recorded_output_path() -> None:
    """Test that metrics without a recorded output path carry no output folder."""
    metrics = factories.metrics(
        type="msqc", relative_output_path=None, values={"pressure": 100.0}
    )

    assert to_metrics_dict(metrics) == {"type": "msqc", "pressure": 100.0}
