"""Tests for the watcher_impl module."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest
import pytz
from common.settings import INSTRUMENTS
from dags.impl.watcher_impl import (
    _add_raw_file_to_db,
    _file_meets_age_criterion,
    _get_file_info,
    _sort_by_creation_date,
    decide_raw_file_handling,
    get_unknown_raw_files,
    start_file_handler,
)
from plugins.common.keys import OpArgs, XComKeys

SOME_INSTRUMENT_ID = "some_instrument_id"


@patch.dict(
    INSTRUMENTS, {"instrument1": {"raw_data_path_variable_name": "SOME_VARIABLE_NAME"}}
)
@patch("os.stat")
def test_get_file_info(
    mock_stat: MagicMock,
) -> None:
    """Test _get_file_info returns the expected values."""
    mock_stat.return_value.st_size = 42.0
    mock_stat.return_value.st_ctime = 43.0

    # when
    result = _get_file_info("test_file.raw", "instrument1")

    assert result == (43.0, 42.0)


@patch("dags.impl.watcher_impl.get_internal_instrument_data_path")
@patch("dags.impl.watcher_impl._get_file_info")
@patch("dags.impl.watcher_impl.add_new_raw_file_to_db")
def test_add_raw_file_to_db(
    mock_add_new_raw_file_to_db: MagicMock,
    mock_get_file_info: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test add_to_db makes the expected calls."""
    mock_get_instrument_data_path.return_value = Path("/path/to/data")
    mock_get_file_info.return_value = 42.0, 43.0

    # when
    _add_raw_file_to_db(
        "test_file.raw",
        project_id="PID1",
        instrument_id="instrument1",
    )

    mock_get_file_info.assert_called_once_with("test_file.raw", "instrument1")
    mock_add_new_raw_file_to_db.assert_called_once_with(
        "test_file.raw",
        project_id="PID1",
        instrument_id="instrument1",
        status="new",
        size=43.0,
        creation_ts=42.0,
    )


@patch("dags.impl.watcher_impl.get_internal_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl._sort_by_creation_date")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_existing_files_in_db(
    mock_put_xcom: MagicMock,
    mock_sort: MagicMock,
    mock_get_unknown_raw_files_from_db: MagicMock,
    mock_os_listdir: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test get_unknown_raw_files with existing files in the database."""
    mock_get_instrument_data_path.return_value = Path("path/to")

    mock_os_listdir.return_value = [
        "path/to/file1.raw",
        "path/to/file2.raw",
        "path/to/file3.raw",
    ]
    mock_get_unknown_raw_files_from_db.return_value = ["file1.raw", "file2.raw"]
    ti = Mock()
    mock_sort.return_value = ["file3.raw"]

    # when
    get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    mock_put_xcom.assert_called_once_with(ti, XComKeys.RAW_FILE_NAMES, ["file3.raw"])
    mock_sort.assert_called_once_with(["file3.raw"], "some_instrument_id")


@patch("dags.impl.watcher_impl._get_file_info")
def test_sort_by_creation_date_multiple_files(mock_get_file_info: MagicMock) -> None:
    """Test _sort_by_creation_date with multiple files."""
    mock_get_file_info.side_effect = [
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


@patch("dags.impl.watcher_impl.get_internal_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl._sort_by_creation_date")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_no_existing_files_in_db(
    mock_put_xcom: MagicMock,
    mock_sort: MagicMock,
    mock_get_unknown_raw_files_from_db: MagicMock,
    mock_os_listdir: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test get_unknown_raw_files with no existing files in the database."""
    # Given a list of raw files, some of which are already in the database
    mock_get_instrument_data_path.return_value = Path("path/to")

    mock_os_listdir.return_value = [
        "path/to/file1.raw",
        "path/to/file2.raw",
        "path/to/file3.raw",
    ]
    mock_get_unknown_raw_files_from_db.return_value = []
    ti = Mock()
    mock_sort.return_value = ["file3.raw", "file2.raw", "file1.raw"]

    # Call the function
    get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    # The function should call put_xcom with the correct arguments
    mock_put_xcom.assert_called_once_with(
        ti, XComKeys.RAW_FILE_NAMES, ["file3.raw", "file2.raw", "file1.raw"]
    )
    mock_sort.assert_called_once_with(
        ["file1.raw", "file2.raw", "file3.raw"], "some_instrument_id"
    )


@patch("dags.impl.watcher_impl.get_internal_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_empty_directory(
    mock_put_xcom: MagicMock,
    mock_get_unknown_raw_files_from_db: MagicMock,
    mock_os_listdir: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test get_unknown_raw_files with an empty directory."""
    # Given a list of raw files, some of which are already in the database
    mock_get_instrument_data_path.return_value = Path("path/to")

    mock_os_listdir.return_value = []

    ti = Mock()

    # Call the function
    with pytest.raises(ValueError):
        get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    # The function should call put_xcom with the correct arguments
    mock_put_xcom.assert_not_called()
    mock_get_unknown_raw_files_from_db.assert_not_called()


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
    mock_get_xcom.return_value = ["file1", "file2", "file3"]
    mock_get_project_ids.return_value = ["project1", "project2"]
    mock_get_unique.side_effect = [None, "project1", "project2"]
    mock_file_meets.side_effect = [True, True, False]

    # when
    decide_raw_file_handling(mock_ti, instrument_id="instrument1")

    mock_get_xcom.assert_called_once_with(mock_ti, "raw_file_names")
    mock_get_project_ids.assert_called_once()
    mock_get_unique.assert_any_call("file1", ["project1", "project2"])
    mock_get_unique.assert_any_call("file2", ["project1", "project2"])
    mock_get_unique.assert_any_call("file3", ["project1", "project2"])
    mock_put.assert_called_once_with(
        mock_ti,
        "raw_file_project_ids",
        {
            "file1": (None, True),
            "file2": ("project1", True),
            "file3": ("project2", False),
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
@patch("dags.impl.watcher_impl._get_file_info")
def test_file_meets_age_criterion_when_file_is_younger(
    mock_get_file_info: MagicMock, mock_get_var: MagicMock
) -> None:
    """Test _file_meets_age_criterion when the file is younger than the max. age."""
    mock_get_var.return_value = "2"
    mock_get_file_info.return_value = (
        (datetime.now(tz=pytz.utc) - timedelta(hours=1)).timestamp(),
        "ignored",
    )

    # when
    assert _file_meets_age_criterion("file", "instrument")


@patch("dags.impl.watcher_impl.get_airflow_variable")
@patch("dags.impl.watcher_impl._get_file_info")
def test_file_meets_age_criterion_when_file_is_older(
    mock_get_file_info: MagicMock, mock_get_var: MagicMock
) -> None:
    """Test _file_meets_age_criterion when the file is older than the max. age."""
    mock_get_var.return_value = "2"
    mock_get_file_info.return_value = (
        (datetime.now(tz=pytz.utc) - timedelta(hours=3)).timestamp(),
        "ignored",
    )

    # when
    assert not _file_meets_age_criterion("file", "instrument")


@patch("dags.impl.watcher_impl.get_airflow_variable")
@patch("dags.impl.watcher_impl._get_file_info")
def test_file_meets_age_criterion_when_no_max_age_defined(
    mock_get_file_info: MagicMock, mock_get_var: MagicMock
) -> None:
    """Test _file_meets_age_criterion when no max. age is defined."""
    mock_get_var.return_value = "-1"
    mock_get_file_info.return_value = (
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
@patch("dags.impl.watcher_impl.trigger_dag")
def test_start_file_handler_with_no_files(
    mock_trigger_dag: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_file_handler with no files."""
    # given
    mock_get_xcom.return_value = {}
    ti = Mock()

    # when
    start_file_handler(ti, **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    mock_trigger_dag.assert_not_called()
    mock_add_raw_file_to_db.assert_not_called()


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.DagRun.generate_run_id")
@patch("dags.impl.watcher_impl.trigger_dag")
@patch("dags.impl.watcher_impl.datetime")
def test_start_file_handler_with_single_file(
    mock_datetime: MagicMock,
    mock_trigger_dag: MagicMock,
    mock_generate: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_file_handler with a single file."""
    # given
    raw_file_names = {"file1.raw": ("PID1", True)}
    mock_get_xcom.return_value = raw_file_names
    run_ids = [
        "run_id1",
    ]
    mock_generate.side_effect = run_ids
    mock_datetime.now.return_value = 123
    ti = Mock()

    # when
    start_file_handler(ti, **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    assert mock_trigger_dag.call_count == 1  # no magic numbers
    for n, call_ in enumerate(mock_trigger_dag.call_args_list):
        assert call_[1]["dag_id"].endswith("instrument1")
        assert run_ids[n] == call_[1]["run_id"]
        assert {
            "raw_file_name": list(raw_file_names.keys())[n],
        } == call_[1]["conf"]
        assert not call_[1]["replace_microseconds"]

    mock_add_raw_file_to_db.assert_called_once_with(
        "file1.raw", project_id="PID1", instrument_id="instrument1", status="new"
    )


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.DagRun.generate_run_id")
@patch("dags.impl.watcher_impl.trigger_dag")
@patch("dags.impl.watcher_impl.datetime")
def test_start_file_handler_with_multiple_files(  # Too many arguments
    mock_datetime: MagicMock,
    mock_trigger_dag: MagicMock,
    mock_generate: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_file_handler with multiple files."""
    # given
    raw_file_names = {
        "file1.raw": ("project1", True),
        "file2.raw": (None, True),
        "file3.raw": ("project2", False),
    }
    mock_get_xcom.return_value = raw_file_names
    run_ids = ["run_id1", "run_id2", "run_id3"]
    mock_generate.side_effect = run_ids
    mock_datetime.now.return_value = 123

    # when
    start_file_handler(Mock(), **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    assert mock_trigger_dag.call_count == 2  # noqa: PLR2004 no magic numbers
    for n, call_ in enumerate(mock_trigger_dag.call_args_list):
        assert call_[1]["dag_id"].endswith("instrument1")
        assert run_ids[n] == call_[1]["run_id"]
        assert {
            "raw_file_name": list(raw_file_names.keys())[n],
        } == call_[1]["conf"]
        assert not call_[1]["replace_microseconds"]

    mock_add_raw_file_to_db.assert_has_calls(
        [
            call(
                "file1.raw",
                project_id="project1",
                instrument_id="instrument1",
                status="new",
            ),
            call(
                "file2.raw",
                project_id=None,
                instrument_id="instrument1",
                status="new",
            ),
            call(
                "file3.raw",
                project_id="project2",
                instrument_id="instrument1",
                status="ignored",
            ),
        ]
    )
