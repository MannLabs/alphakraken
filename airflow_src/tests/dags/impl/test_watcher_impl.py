"""Tests for the watcher_impl module."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest
import pytz
from dags.impl.watcher_impl import (
    _add_raw_file_to_db,
    _file_meets_age_criterion,
    _get_collision_flag,
    _is_collision,
    _sort_by_creation_date,
    decide_raw_file_handling,
    get_unknown_raw_files,
    start_acquisition_handler,
)
from plugins.common.keys import OpArgs, XComKeys

from shared.db.models import RawFile

SOME_INSTRUMENT_ID = "some_instrument_id"


@patch("dags.impl.watcher_impl.get_file_creation_timestamp")
@patch("dags.impl.watcher_impl.add_new_raw_file_to_db")
@patch("dags.impl.watcher_impl._get_collision_flag")
def test_add_raw_file_to_db(
    mock_get_collision_flag: MagicMock,
    mock_add_new_raw_file_to_db: MagicMock,
    mock_get_file_creation_timestamp: MagicMock,
) -> None:
    """Test add_to_db makes the expected calls."""
    mock_get_file_creation_timestamp.return_value = 42.0
    mock_get_collision_flag.return_value = "123-"

    # when
    _add_raw_file_to_db(
        "test_file.raw",
        is_collision=True,
        project_id="PID1",
        instrument_id="instrument1",
    )

    mock_get_file_creation_timestamp.assert_called_once_with(
        "test_file.raw", "instrument1"
    )
    mock_add_new_raw_file_to_db.assert_called_once_with(
        "test_file.raw",
        collision_flag="123-",
        project_id="PID1",
        instrument_id="instrument1",
        status="queued_for_monitoring",
        creation_ts=42.0,
    )


@patch("dags.impl.watcher_impl.RawFileWrapperFactory")
@patch("dags.impl.watcher_impl.get_raw_files_by_names_from_db")
@patch("dags.impl.watcher_impl._is_collision")
@patch("dags.impl.watcher_impl._sort_by_creation_date")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_existing_files_in_db(
    mock_put_xcom: MagicMock,
    mock_sort: MagicMock,
    mock_is_collision: MagicMock,
    mock_get_unknown_raw_files_from_db: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test get_unknown_raw_files with existing files in the database."""
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.return_value = {
        "file1.raw",
        "file2.raw",
        "file3.raw",
    }
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.side_effect = [
        Path("/path/to/file1.raw"),
        Path("/path/to/file2.raw"),
    ]

    file1 = MagicMock(wraps=RawFile, original_name="file1.raw", size=123)
    file2 = MagicMock(wraps=RawFile, original_name="file2.raw", size=234)
    mock_get_unknown_raw_files_from_db.return_value = [file1, file2]

    mock_is_collision.side_effect = [False, True]

    ti = Mock()
    mock_sort.return_value = ["file3.raw", "file2.raw"]

    # when
    get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    mock_put_xcom.assert_called_once_with(
        ti, XComKeys.RAW_FILE_NAMES_TO_PROCESS, {"file3.raw": False, "file2.raw": True}
    )
    mock_sort.assert_called_once_with(["file2.raw", "file3.raw"], "some_instrument_id")
    mock_is_collision.assert_has_calls(
        [
            call(Path("/path/to/file1.raw"), [123]),
            call(Path("/path/to/file2.raw"), [234]),
        ]
    )


def test_no_collision_if_files_not_fixed() -> None:
    """Test _is_collision with no collision: one file status is not fixed."""
    mock_path = MagicMock(spec=Path)

    status_and_size_from_db = [100, None]
    assert not _is_collision(mock_path, status_and_size_from_db)


def test_no_collision_if_main_files_no_found() -> None:
    """Test _is_collision with no collision: main file is not found."""
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = False

    status_and_size_from_db = [100, 100]
    assert not _is_collision(mock_path, status_and_size_from_db)


def test_no_collision_if_size_matches() -> None:
    """Test _is_collision with no collision: all file status are fixed, but one size matches."""
    mock_path = MagicMock(spec=Path)
    mock_path.stat.return_value.st_size = 100
    db_files_fixed_with_size_match = [100, 123]
    assert not _is_collision(mock_path, db_files_fixed_with_size_match)


def test_collision_if_size_mismatch() -> None:
    """Test _is_collision with a collision: all file status are fixed, but no size does match."""
    mock_path = MagicMock(spec=Path)
    mock_path.stat.return_value.st_size = 100
    db_files_fixed_with_size_mismatch = [123, 234]

    assert _is_collision(mock_path, db_files_fixed_with_size_mismatch)


@patch("dags.impl.watcher_impl.datetime")
def test_get_collision_flag(mock_datetime: MagicMock) -> None:
    """Test construction of collision flag."""
    mock_datetime.now.return_value = datetime(
        2000, 1, 2, 3, 4, 5, 678901, tzinfo=pytz.utc
    )

    result = _get_collision_flag()
    assert result == "20000102-030405-678901-"


@patch("dags.impl.watcher_impl.get_file_creation_timestamp")
def test_sort_by_creation_date_multiple_files(
    mock_get_file_creation_timestamp: MagicMock,
) -> None:
    """Test _sort_by_creation_date with multiple files."""
    mock_get_file_creation_timestamp.side_effect = [
        (
            datetime(2022, 1, 1, 12, 0, 0, tzinfo=pytz.utc).timestamp(),
            0,
        ),
        (datetime(2022, 1, 1, 11, 0, 0, tzinfo=pytz.utc).timestamp(), 0),
        (datetime(2022, 1, 1, 13, 0, 0, tzinfo=pytz.utc).timestamp(), 0),
    ]
    # when
    result = _sort_by_creation_date(["file1", "file2", "file3"], "instrument1")
    assert result == ["file3", "file1", "file2"]


@patch("dags.impl.watcher_impl.RawFileWrapperFactory")
@patch("dags.impl.watcher_impl.get_raw_files_by_names_from_db")
@patch("dags.impl.watcher_impl._sort_by_creation_date")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_no_existing_files_in_db(
    mock_put_xcom: MagicMock,
    mock_sort: MagicMock,
    mock_get_unknown_raw_files_from_db: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test get_unknown_raw_files with no existing files in the database."""
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.return_value = {
        "file1.raw",
        "file2.raw",
        "file3.raw",
    }

    mock_get_unknown_raw_files_from_db.return_value = []
    ti = Mock()
    mock_sort.return_value = ["file3.raw", "file2.raw", "file1.raw"]

    # when
    get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    mock_put_xcom.assert_called_once_with(
        ti,
        XComKeys.RAW_FILE_NAMES_TO_PROCESS,
        {"file3.raw": False, "file2.raw": False, "file1.raw": False},
    )
    mock_sort.assert_called_once_with(
        ["file1.raw", "file2.raw", "file3.raw"], "some_instrument_id"
    )


@patch("dags.impl.watcher_impl.RawFileWrapperFactory")
@patch("dags.impl.watcher_impl.get_raw_files_by_names_from_db")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_empty_directory(
    mock_put_xcom: MagicMock,
    mock_get_unknown_raw_files_from_db: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test get_unknown_raw_files with an empty directory."""
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.return_value = {}
    ti = Mock()

    # when
    get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    mock_get_unknown_raw_files_from_db.assert_called_once_with([])
    mock_put_xcom.assert_called_once_with(ti, XComKeys.RAW_FILE_NAMES_TO_PROCESS, {})


@patch("dags.impl.watcher_impl.get_all_project_ids")
@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl.get_unique_project_id")
@patch("dags.impl.watcher_impl._file_meets_age_criterion")
@patch("dags.impl.watcher_impl.put_xcom")
def test_decide_raw_file_handling(
    mock_put: MagicMock,
    mock_file_meets: MagicMock,
    mock_get_unique: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_project_ids: MagicMock,
) -> None:
    """A test for the decide_raw_file_handling function."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = {"file1": "", "file2": "", "file3": "123-"}
    mock_get_project_ids.return_value = ["project1", "project2"]
    mock_get_unique.side_effect = [None, "project1", "project2"]
    mock_file_meets.side_effect = [True, True, False]

    # when
    decide_raw_file_handling(mock_ti, instrument_id="instrument1")

    mock_get_xcom.assert_called_once_with(mock_ti, "raw_file_names_to_process")
    mock_get_project_ids.assert_called_once()
    mock_get_unique.assert_any_call("file1", ["project1", "project2"])
    mock_get_unique.assert_any_call("file2", ["project1", "project2"])
    mock_get_unique.assert_any_call("file3", ["project1", "project2"])
    mock_put.assert_called_once_with(
        mock_ti,
        "raw_file_names_with_decisions",
        {
            "file1": (None, True, ""),
            "file2": ("project1", True, ""),
            "file3": ("project2", False, "123-"),
        },
    )
    mock_file_meets.assert_has_calls(
        [
            call("file1", "instrument1"),
            call("file2", "instrument1"),
            call("file3", "instrument1"),
        ]
    )


@patch("dags.impl.watcher_impl.get_airflow_variable")
@patch("dags.impl.watcher_impl.get_file_creation_timestamp")
def test_file_meets_age_criterion_when_file_is_younger(
    mock_get_file_creation_timestamp: MagicMock, mock_get_var: MagicMock
) -> None:
    """Test _file_meets_age_criterion when the file is younger than the max. age."""
    mock_get_var.return_value = "2"
    mock_get_file_creation_timestamp.return_value = (
        datetime.now(tz=pytz.utc) - timedelta(hours=1)
    ).timestamp()

    # when
    assert _file_meets_age_criterion("file", "instrument")


@patch("dags.impl.watcher_impl.get_airflow_variable")
@patch("dags.impl.watcher_impl.get_file_creation_timestamp")
def test_file_meets_age_criterion_when_file_is_older(
    mock_get_file_creation_timestamp: MagicMock, mock_get_var: MagicMock
) -> None:
    """Test _file_meets_age_criterion when the file is older than the max. age."""
    mock_get_var.return_value = "2"
    mock_get_file_creation_timestamp.return_value = (
        datetime.now(tz=pytz.utc) - timedelta(hours=3)
    ).timestamp()

    # when
    assert not _file_meets_age_criterion("file", "instrument")


@patch("dags.impl.watcher_impl.get_airflow_variable")
@patch("dags.impl.watcher_impl.get_file_creation_timestamp")
def test_file_meets_age_criterion_when_no_max_age_defined(
    mock_get_file_creation_timestamp: MagicMock, mock_get_var: MagicMock
) -> None:
    """Test _file_meets_age_criterion when no max. age is defined."""
    mock_get_var.return_value = "-1"
    mock_get_file_creation_timestamp.return_value = (
        (datetime.now(tz=pytz.utc) - timedelta(hours=3)).timestamp(),
        "ignored",
    )

    # when
    assert _file_meets_age_criterion("file", "instrument")


@patch("dags.impl.watcher_impl.get_airflow_variable")
def test_file_meets_age_criterion_invalid_number(mock_get_var: MagicMock) -> None:
    """Test _file_meets_age_criterion when the max. age is not a number."""
    mock_get_var.return_value = "no_number"

    with pytest.raises(ValueError):
        # when
        _file_meets_age_criterion("file", "instrument")


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.trigger_dag_run")
def test_start_acquisition_handler_with_no_files(
    mock_trigger_dag_run: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_acquisition_handler with no files."""
    # given
    mock_get_xcom.return_value = {}
    ti = Mock()

    # when
    start_acquisition_handler(ti, **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    mock_trigger_dag_run.assert_not_called()
    mock_add_raw_file_to_db.assert_not_called()


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.trigger_dag_run")
def test_start_acquisition_handler_with_single_file(
    mock_trigger_dag_run: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_acquisition_handler with a single file."""
    # given
    raw_file_names = {"file1.raw": ("PID1", True, True)}
    mock_get_xcom.return_value = raw_file_names

    mock_add_raw_file_to_db.return_value = "123-file1.raw"
    ti = Mock()

    # when
    start_acquisition_handler(ti, **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    assert mock_trigger_dag_run.call_count == 1  # no magic numbers
    for n, call_ in enumerate(mock_trigger_dag_run.call_args_list):
        assert call_.args[0] == ("acquisition_handler.instrument1")
        assert {
            "raw_file_id": f"123-{list(raw_file_names.keys())[n]}",
        } == call_.args[1]

    mock_add_raw_file_to_db.assert_called_once_with(
        "file1.raw",
        is_collision=True,
        project_id="PID1",
        instrument_id="instrument1",
        status="queued_for_monitoring",
    )


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.trigger_dag_run")
def test_start_acquisition_handler_with_multiple_files(  # Too many arguments
    mock_trigger_dag_run: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_acquisition_handler with multiple files."""
    # given
    raw_file_names = {
        "file1.raw": ("project1", True, False),
        "file2.raw": (None, True, False),
        "file3.raw": ("project2", False, False),
    }
    mock_get_xcom.return_value = raw_file_names
    mock_add_raw_file_to_db.side_effect = raw_file_names

    # when
    start_acquisition_handler(Mock(), **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    assert mock_trigger_dag_run.call_count == 2  # noqa: PLR2004 no magic numbers
    for n, call_ in enumerate(mock_trigger_dag_run.call_args_list):
        assert call_.args[0] == ("acquisition_handler.instrument1")
        assert {
            "raw_file_id": list(raw_file_names.keys())[n],
        } == call_.args[1]

    mock_add_raw_file_to_db.assert_has_calls(
        [
            call(
                "file1.raw",
                is_collision=False,
                project_id="project1",
                instrument_id="instrument1",
                status="queued_for_monitoring",
            ),
            call(
                "file2.raw",
                is_collision=False,
                project_id=None,
                instrument_id="instrument1",
                status="queued_for_monitoring",
            ),
            call(
                "file3.raw",
                is_collision=False,
                project_id="project2",
                instrument_id="instrument1",
                status="ignored",
            ),
        ]
    )
