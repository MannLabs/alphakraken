"""Tests for the raw file queries of the REST API service."""

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from rest_api.service import get_raw_files_with_metrics
from shared.db.raw_files import RawFileFilter
from shared.keys import MetricsTypes
from shared.tests.factories import metrics, raw_file

CREATED_AT = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)


@pytest.fixture
def mock_db() -> Iterator[dict[str, MagicMock]]:
    """The three queries the endpoint uses, with one raw file that has two metrics types."""
    with (
        patch("shared.db.raw_files.find") as find,
        patch("shared.db.raw_files.count", return_value=7) as count,
        patch("shared.db.metrics.latest_by_raw_file") as latest,
        patch("rest_api.service.get_display_backup_path", return_value="/backup/f1"),
        patch(
            "shared.api_responses.get_display_output_path",
            side_effect=lambda m: m.relative_output_path and "/output/f1",
        ),
    ):
        find.return_value = [
            raw_file(
                id="f1",
                collision_flag=None,
                status_details=None,
                created_at=CREATED_AT,
                created_at_=CREATED_AT,
                updated_at_=CREATED_AT,
                file_info={"a.raw": (1.0, "hash")},
            )
        ]
        latest.return_value = {
            "f1": {
                MetricsTypes.ALPHADIA: metrics(values={"proteins": 5000}),
                MetricsTypes.MSQC: metrics(
                    type=MetricsTypes.MSQC,
                    relative_output_path=None,
                    values={"pressure": 100.0},
                ),
            }
        }
        yield {"find": find, "count": count, "latest": latest}


def test_get_raw_files_with_metrics(mock_db: dict[str, MagicMock]) -> None:
    """The filters are passed on; each raw file carries its metrics as a list of dicts."""
    # when
    results, total = get_raw_files_with_metrics(
        instrument_id="inst1",
        name_contains="F",
        project_id="P1",
        date_from=CREATED_AT,
        date_to=CREATED_AT,
        limit=10,
        offset=20,
    )

    # then
    where = RawFileFilter(
        instrument_id="inst1",
        name_contains="F",
        project_id="P1",
        created_after=CREATED_AT,
        created_before=CREATED_AT,
    )
    mock_db["find"].assert_called_once_with(
        where, limit=10, offset=20, include_file_info=False
    )
    mock_db["count"].assert_called_once_with(where)
    mock_db["latest"].assert_called_once_with(["f1"])
    assert total == 7
    assert results == [
        {
            "id": "f1",
            "original_name": "file.raw",
            "instrument_id": "inst1",
            "project_id": "_FALLBACK",
            "status": "done",
            "size": 1024,
            "instrument_file_status": "n/a",
            "created_at": CREATED_AT,
            "updated_at_": CREATED_AT,
            "metrics": [
                {"type": "alphadia", "proteins": 5000, "output_path": "/output/f1"},
                {"type": "msqc", "pressure": 100.0},
            ],
        }
    ]


def test_get_raw_files_without_metrics(mock_db: dict[str, MagicMock]) -> None:
    """Without metrics, no metrics query is made and the dicts have no metrics key."""
    results, _ = get_raw_files_with_metrics(include_metrics=False)

    mock_db["latest"].assert_not_called()
    assert "metrics" not in results[0]


def test_get_raw_files_with_file_info(mock_db: dict[str, MagicMock]) -> None:
    """With file info, the mapping and the folder its paths are relative to are included."""
    results, _ = get_raw_files_with_metrics(include_file_info=True)

    assert mock_db["find"].call_args.kwargs["include_file_info"] is True
    assert results[0]["file_info"] == {"a.raw": (1.0, "hash")}
    assert results[0]["backup_path"] == "/backup/f1"
