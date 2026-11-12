"""Tests for the db module."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz
from service.db import (
    df_from_db_data,
    get_full_raw_file_data,
    get_output_folders,
    get_raw_files_for_status_df,
    get_raw_files_for_throughput_per_day,
)
from service.timezone import DISPLAY_TIMEZONE

from shared.db import status
from shared.db.raw_files import RawFileFilter
from shared.domain.raw_files import TERMINAL_STATUSES
from shared.tests.factories import kraken_status, metrics, raw_file


@patch("shared.db.raw_files.find")
def test_get_raw_files_for_status_df(mock_find: MagicMock) -> None:
    """Recent files are returned with their status columns, in display time, per instrument."""
    # given
    mock_find.return_value = [
        raw_file(
            id="f1",
            instrument_id="inst2",
            status="done",
            status_details=None,
            created_at=datetime(2024, 1, 2, tzinfo=UTC),
            updated_at_=datetime(2024, 1, 3, tzinfo=UTC),
        ),
        raw_file(
            id="f2",
            instrument_id="inst1",
            status="error",
            status_details="boom",
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_=datetime(2024, 1, 4, tzinfo=UTC),
        ),
    ]

    # when
    result = get_raw_files_for_status_df(max_age_in_days=7)

    # then
    where = mock_find.call_args.args[0]
    assert (datetime.now(tz=UTC) - where.created_after).days == 7
    assert list(result.columns) == [
        "instrument_id",
        "created_at",
        "updated_at_",
        "status",
        "status_details",
    ]
    assert list(result["instrument_id"]) == ["inst1", "inst2"]
    assert result["status_details"][0] == "boom"
    assert pd.isna(result["status_details"][1])
    assert result["created_at"].dt.tz == DISPLAY_TIMEZONE
    assert result["created_at"][1] == datetime(2024, 1, 2, tzinfo=UTC)


@patch("shared.db.raw_files.find")
def test_get_raw_files_for_throughput_per_day(mock_find: MagicMock) -> None:
    """Finished files are counted and their sizes summed per day and instrument."""
    # given
    day1, day2 = (
        datetime(2024, 1, 1, 10, tzinfo=UTC),
        datetime(2024, 1, 2, 10, tzinfo=UTC),
    )
    mock_find.return_value = [
        raw_file(instrument_id="inst1", created_at=day1, size=1024**3),
        raw_file(instrument_id="inst1", created_at=day1, size=None),
        raw_file(instrument_id="inst2", created_at=day2, size=2 * 1024**3),
    ]

    # when
    result = get_raw_files_for_throughput_per_day(days=14)

    # then
    where = mock_find.call_args.args[0]
    assert where.statuses == TERMINAL_STATUSES
    assert (datetime.now(tz=UTC) - where.created_after).days == 14
    assert result.to_dict("records") == [
        {"date": day1.date(), "instrument_id": "inst1", "count": 2, "size_gb": 1.0},
        {"date": day2.date(), "instrument_id": "inst2", "count": 1, "size_gb": 2.0},
    ]


def test_get_raw_files_for_throughput_per_day_empty() -> None:
    """Without finished files the DataFrame has the columns but no rows."""
    with patch("shared.db.raw_files.find", return_value=[]):
        result = get_raw_files_for_throughput_per_day()

    assert result.empty
    assert list(result.columns) == ["date", "instrument_id", "count", "size_gb"]


@patch("shared.db.status.find")
def test_df_from_db_data_with_domain_objects(mock_find: MagicMock) -> None:
    """Domain objects become rows with `id` as the identifier column, in display time."""
    mock_find.return_value = [kraken_status(id="inst1"), kraken_status(id="backup")]

    result = df_from_db_data(status.find())

    assert list(result["id"]) == ["inst1", "backup"]
    assert result["updated_at_"].dt.tz == DISPLAY_TIMEZONE


def test_get_raw_files_for_status_df_empty() -> None:
    """Without recent files the DataFrame is empty."""
    with patch("shared.db.raw_files.find", return_value=[]):
        assert get_raw_files_for_status_df(max_age_in_days=7).empty


def test_df_from_db_data() -> None:
    """Test the df_from_db_data function."""
    mock_query = MagicMock()
    mock_query.to_mongo.side_effect = [
        {"a": 1, "b": 2, "created_at_": 3},
        {"a": 4, "b": 5, "created_at_": 6},
    ]

    mock_query_set = [mock_query, mock_query]

    # when
    result = df_from_db_data(mock_query_set)

    expected_data = pd.DataFrame({"a": [4, 1], "b": [5, 2], "created_at_": [6, 3]})
    pd.testing.assert_frame_equal(expected_data, result)


def test_df_from_db_data_all_parameters() -> None:
    """Test the df_from_db_data function with all parameters."""
    mock_query = MagicMock()
    mock_query.to_mongo.side_effect = [
        {"a": 1, "b": 2, "created_at_": 3},
        {"a": 1, "b": 5, "created_at_": 6},
    ]

    mock_query_set = [mock_query, mock_query]

    # when
    result = df_from_db_data(mock_query_set, drop_duplicates=["a"], drop_columns=["b"])

    expected_data = pd.DataFrame({"a": [1], "created_at_": [6]})
    pd.testing.assert_frame_equal(expected_data, result)


@patch("service.timezone.DISPLAY_TIMEZONE", pytz.timezone("Europe/Berlin"))
def test_df_from_db_data_shifts_timestamps_to_display_timezone() -> None:
    """Test that the db timestamps are shifted to the configured timezone."""
    mock_query = MagicMock()
    mock_query.to_mongo.return_value = {
        "created_at": datetime(2024, 7, 1, 12, 0, 0),  # noqa: DTZ001
        "created_at_": datetime(2024, 7, 1, 12, 0, 1),  # noqa: DTZ001
        "updated_at_": datetime(2024, 7, 1, 12, 0, 2),  # noqa: DTZ001
    }

    # when
    result = df_from_db_data([mock_query])

    assert result["created_at"].tolist() == [
        pd.Timestamp("2024-07-01 14:00:00", tz="Europe/Berlin")
    ]
    assert result["created_at_"].tolist() == [
        pd.Timestamp("2024-07-01 14:00:01", tz="Europe/Berlin")
    ]
    assert result["updated_at_"].tolist() == [
        pd.Timestamp("2024-07-01 14:00:02", tz="Europe/Berlin")
    ]


@patch(
    "service.db.get_display_output_path",
    side_effect=lambda p, base: f"{base or '/out'}/{p}",
)
@patch("shared.db.metrics.find")
def test_get_output_folders(
    mock_find: MagicMock,
    mock_get_display_output_path: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_output_folders returns one row per metrics entry, respecting the N:1 relation."""
    mock_find.return_value = [
        metrics(
            raw_file_id="f1",
            settings_name="s1",
            relative_output_path="f1/alphadia",
            output_display_base_path=None,
        ),
        metrics(
            raw_file_id="f2",
            settings_name="s1",
            relative_output_path="f2/alphadia",
            output_display_base_path=None,
        ),
        # same raw file f1 run with different settings -> its own output folder
        metrics(
            raw_file_id="f1",
            settings_name="s2",
            settings_version=3,
            type="custom",
            relative_output_path="f1/custom",
            output_display_base_path="/old_out",
        ),
        # written before output paths were recorded
        metrics(raw_file_id="f2", relative_output_path=None),
    ]

    # when
    result = get_output_folders(["f1", "f2"])

    # then
    mock_find.assert_called_once_with(["f1", "f2"], fields=[])
    assert list(result.columns) == [
        "raw_file_id",
        "settings_name",
        "settings_version",
        "type",
        "output_path",
    ]
    assert result["raw_file_id"].tolist() == ["f1", "f2", "f1", "f2"]
    assert result["output_path"].tolist()[:3] == [
        "/out/f1/alphadia",
        "/out/f2/alphadia",
        "/old_out/f1/custom",
    ]
    assert pd.isna(result["output_path"][3])


@patch("shared.db.metrics.find", return_value=[])
def test_get_output_folders_no_metrics(
    mock_find: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_output_folders returns an empty, well-formed DataFrame when no metrics exist."""
    # when
    result = get_output_folders(["f1"])

    # then
    assert result.empty
    assert list(result.columns) == [
        "raw_file_id",
        "settings_name",
        "settings_version",
        "type",
        "output_path",
    ]


@patch("service.db.get_display_backup_path", side_effect=lambda r: f"/backup/{r.id}")
@patch("shared.db.raw_files.find")
def test_get_full_raw_file_data_adds_the_backup_path(
    mock_find: MagicMock,
    mock_get_display_backup_path: MagicMock,  # noqa: ARG001
) -> None:
    """Test that each row carries the display path of its own backup folder and the file info."""
    # given - the dataframe is sorted by `created_at_`, unlike the query result
    mock_find.return_value = [
        raw_file(
            id="f1",
            created_at_=datetime(2024, 1, 1, tzinfo=UTC),
            file_info={"f1": (1.0, "hash")},
        ),
        raw_file(id="f2", created_at_=datetime(2024, 1, 2, tzinfo=UTC), file_info={}),
    ]

    # when
    result = get_full_raw_file_data(["f1", "f2"])

    # then
    mock_find.assert_called_once_with(
        RawFileFilter(ids=["f1", "f2"]), include_file_info=True
    )
    assert result["id"].tolist() == ["f2", "f1"]
    assert result["backup_path"].tolist() == ["/backup/f2", "/backup/f1"]
    assert result["file_info"].tolist() == [{}, {"f1": (1.0, "hash")}]


def test_get_full_raw_file_data_without_raw_files() -> None:
    """Test that unknown ids yield an empty DataFrame."""
    with patch("shared.db.raw_files.find", return_value=[]):
        assert get_full_raw_file_data(["unknown"]).empty
