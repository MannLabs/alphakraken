"""Tests for the MCP server."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.server import (
    _format,
    _matches_gradient_length,
    get_available_instruments,
    get_raw_files_by_names,
    get_raw_files_for_instrument,
)
from shared.db.raw_files import RawFileFilter
from shared.keys import MetricsTypes
from shared.tests.factories import metrics, raw_file

CREATED_AT = datetime(2025, 1, 2, 3, 4, 5, 678, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("some text", "some text"),
        (None, None),
        (0, 0),
        (0.0, 0),
        (5000, 5000),
        (123456789, 123460000),
        (21.123456, 21.123),
        (0.000123456789, 0.00012346),
        (-21.123456, -21.123),
        (CREATED_AT, "2025-01-02T03:04:00+00:00"),
    ],
)
def test_format(value: Any, expected: Any) -> None:
    """Test that numbers are rounded to five significant digits and datetimes to the minute."""
    assert _format(value) == expected


def test_format_returns_unroundable_number_unchanged() -> None:
    """Test that a number that cannot be rounded is passed through."""
    assert _format(float("inf")) == float("inf")


@pytest.mark.parametrize(
    ("gradient_length", "expected"),
    [
        (21.0, True),
        # 5% tolerance around the gradient length
        (20.0, True),
        (22.1, True),
        (19.9, False),
        (22.2, False),
        ("21.0", True),
        (None, False),
    ],
)
def test_matches_gradient_length(gradient_length: Any, *, expected: bool) -> None:
    """Test that the target has to be within the tolerance of the gradient length."""
    assert _matches_gradient_length(gradient_length, 21.0) is expected


@patch("shared.db.status.instrument_ids", return_value=["inst1", "inst2"])
def test_get_available_instruments(mock_instrument_ids: MagicMock) -> None:  # noqa: ARG001
    """Test that each instrument is returned as a dict."""
    assert get_available_instruments() == [
        {"instrument_id": "inst1"},
        {"instrument_id": "inst2"},
    ]


@patch("shared.db.status.instrument_ids", side_effect=Exception("db down"))
def test_get_available_instruments_reports_error(
    mock_instrument_ids: MagicMock,  # noqa: ARG001
) -> None:
    """Test that a failing query is returned as an error entry, not raised."""
    assert get_available_instruments() == [
        {"error": "Failed to retrieve instrument data: db down"}
    ]


@pytest.fixture
def mock_db() -> Iterator[dict[str, MagicMock]]:
    """The two queries the raw file tools use: `f1` with two metrics types, `f2` without metrics."""
    with (
        patch("shared.db.raw_files.find") as find,
        patch("shared.db.metrics.latest_by_raw_file") as latest,
    ):
        find.return_value = [
            raw_file(id="f1", status_details=None, size=1024, created_at=CREATED_AT),
            raw_file(id="f2", status_details="oops", created_at=CREATED_AT),
        ]
        latest.return_value = {
            "f1": {
                MetricsTypes.ALPHADIA: metrics(
                    relative_output_path="P1/out_f1/fallback_v1",
                    output_display_base_path="/output",
                    values={
                        "proteins": 5000,
                        "gradient_length": 21.123456,
                        "precursors": 40000,
                    },
                ),
                MetricsTypes.MSQC: metrics(
                    type=MetricsTypes.MSQC,
                    relative_output_path=None,
                    values={"gradient_length": 44.0, "pressure": 100.0},
                ),
            }
        }
        yield {"find": find, "latest": latest}


def test_get_raw_files_by_names(mock_db: dict[str, MagicMock]) -> None:
    """Test that each raw file carries its whitelisted set fields and all metrics, one dict per type."""
    # when
    results = get_raw_files_by_names(["f1", "f2"])

    assert results == [
        {
            "raw_file": {
                "instrument_id": "inst1",
                "status": "done",
                "size": 1024,
                "created_at": "2025-01-02T03:04:00+00:00",
                "name": "f1",
            },
            "metrics": [
                {
                    "type": "alphadia",
                    "proteins": 5000,
                    "gradient_length": 21.123,
                    "precursors": 40000,
                    "output_path": "/output/P1/out_f1/fallback_v1",
                },
                {"type": "msqc", "gradient_length": 44.0, "pressure": 100.0},
            ],
        },
        {
            "raw_file": {
                "instrument_id": "inst1",
                "status": "done",
                "status_details": "oops",
                "size": 1024,
                "created_at": "2025-01-02T03:04:00+00:00",
                "name": "f2",
            },
            "metrics": [],
        },
    ]
    mock_db["find"].assert_called_once_with(RawFileFilter(ids=["f1", "f2"]))
    mock_db["latest"].assert_called_once_with(["f1", "f2"])


def test_get_raw_files_by_names_reports_error(mock_db: dict[str, MagicMock]) -> None:
    """Test that a failing query is returned as an error entry, not raised."""
    mock_db["find"].side_effect = Exception("db down")

    assert get_raw_files_by_names(["f1"]) == [
        {"error": "Failed to retrieve raw file data: db down"}
    ]


def _assert_filter(
    mock_find: MagicMock, *, name_contains: str | None, max_age_in_days: int
) -> None:
    """Assert the one query was for `inst1`, the name and files created within the last days."""
    mock_find.assert_called_once()
    where = mock_find.call_args.args[0]
    assert where == RawFileFilter(
        instrument_id="inst1",
        name_contains=name_contains,
        created_after=where.created_after,
    )
    # the cutoff is taken from the clock, so compare with a margin
    age = datetime.now(tz=UTC) - where.created_after
    assert timedelta(0) <= age - timedelta(days=max_age_in_days) < timedelta(minutes=1)


def test_get_raw_files_for_instrument_returns_basic_metrics_by_default(
    mock_db: dict[str, MagicMock],
) -> None:
    """Test that the defaults query the last 30 days by instrument and keep only the basic metrics."""
    # when
    results = get_raw_files_for_instrument("inst1")

    assert [result["metrics"] for result in results] == [
        [
            {"type": "alphadia", "proteins": 5000, "gradient_length": 21.123},
            {"type": "msqc", "gradient_length": 44.0},
        ],
        [],
    ]
    _assert_filter(mock_db["find"], name_contains=None, max_age_in_days=30)


def test_get_raw_files_for_instrument_passes_filters(
    mock_db: dict[str, MagicMock],
) -> None:
    """Test that name search string and maximum age are passed on and all metrics can be requested."""
    # when
    results = get_raw_files_for_instrument(
        "inst1", name_search_string="hela", max_age_in_days=7, only_basic_metrics=False
    )

    assert "precursors" in results[0]["metrics"][0]
    _assert_filter(mock_db["find"], name_contains="hela", max_age_in_days=7)


@pytest.mark.parametrize(
    ("gradient_length_filter", "expected_names"),
    [
        (21.0, ["f1"]),
        # the gradient length of any metrics type counts
        (44.0, ["f1"]),
        (99.0, []),
    ],
)
def test_get_raw_files_for_instrument_filters_by_gradient_length(
    mock_db: dict[str, MagicMock],  # noqa: ARG001
    gradient_length_filter: float,
    expected_names: list[str],
) -> None:
    """Test that raw files outside the gradient length range or without metrics are left out."""
    # when
    results = get_raw_files_for_instrument(
        "inst1", gradient_length_filter=gradient_length_filter
    )

    assert [result["raw_file"]["name"] for result in results] == expected_names


def test_get_raw_files_for_instrument_reports_error(
    mock_db: dict[str, MagicMock],
) -> None:
    """Test that a failing query is returned as an error entry, not raised."""
    mock_db["latest"].side_effect = Exception("db down")

    assert get_raw_files_for_instrument("inst1") == [
        {"error": "Failed to retrieve raw file data: db down"}
    ]
