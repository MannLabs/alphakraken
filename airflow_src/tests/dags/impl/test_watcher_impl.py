"""Tests for the watcher_impl module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from dags.impl.watcher_impl import (
    _add_raw_file_to_db,
    decide_raw_file_handling,
    get_unknown_raw_files,
    start_acquisition_handler,
)
from plugins.common.keys import OpArgs, XComKeys

SOME_INSTRUMENT_ID = "some_instrument_id"


@patch("dags.impl.watcher_impl.get_internal_instrument_data_path")
@patch("os.stat")
@patch("dags.impl.watcher_impl.add_new_raw_file_to_db")
def test_add_raw_file_to_db(
    mock_add_new_raw_file_to_db: MagicMock,
    mock_stat: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test add_to_db makes the expected calls."""
    # Given

    mock_get_instrument_data_path.return_value = Path("/path/to/data")
    mock_stat.return_value.st_size = 42.0
    mock_stat.return_value.st_ctime = 43.0

    # When
    _add_raw_file_to_db("instrument1", "test_file.raw")

    # Then
    mock_get_instrument_data_path.assert_called_once_with("instrument1")
    mock_add_new_raw_file_to_db.assert_called_once_with(
        "test_file.raw",
        status="new",
        instrument_id="instrument1",
        size=42.0,
        creation_ts=43.0,
    )


@patch("dags.impl.watcher_impl.get_internal_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_existing_files_in_db(
    mock_put_xcom: MagicMock,
    mock_get_unknown_raw_files_from_db: MagicMock,
    mock_os_listdir: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test get_unknown_raw_files with existing files in the database."""
    # Given a list of raw files, some of which are already in the database
    mock_get_instrument_data_path.return_value = Path("path/to")

    mock_os_listdir.return_value = [
        "path/to/file1.raw",
        "path/to/file2.raw",
        "path/to/file3.raw",
    ]
    mock_get_unknown_raw_files_from_db.return_value = ["file1.raw", "file2.raw"]
    ti = Mock()

    # Call the function
    get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    # The function should call put_xcom with the correct arguments
    mock_put_xcom.assert_called_once_with(ti, XComKeys.RAW_FILE_NAMES, ["file3.raw"])


@patch("dags.impl.watcher_impl.get_internal_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_unknown_raw_files_with_no_existing_files_in_db(
    mock_put_xcom: MagicMock,
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

    # Call the function
    get_unknown_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    # The function should call put_xcom with the correct arguments
    mock_put_xcom.assert_called_once_with(
        ti, XComKeys.RAW_FILE_NAMES, ["file1.raw", "file2.raw", "file3.raw"]
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
@patch("dags.impl.watcher_impl.put_xcom")
def test_decide_raw_file_handling(
    mock_put: MagicMock,
    mock_get_unique: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_project_ids: MagicMock,
) -> None:
    """A test for the decide_raw_file_handling function."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = ["file1", "file2", "file3"]
    mock_get_project_ids.return_value = ["project1", "project2"]
    mock_get_unique.side_effect = [None, "project1", "project2"]

    # when
    decide_raw_file_handling(mock_ti, instrument_id="instrument1")

    mock_get_xcom.assert_called_once_with(mock_ti, "raw_file_names")
    mock_get_project_ids.assert_called_once()
    mock_get_unique.assert_any_call("file1", ["project1", "project2"])
    mock_get_unique.assert_any_call("file2", ["project1", "project2"])
    mock_get_unique.assert_any_call("file3", ["project1", "project2"])
    mock_put.assert_called_once_with(
        mock_ti,
        "raw_file_handling_decisions",
        {"file1": True, "file2": True, "file3": True},
    )


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.trigger_dag")
def test_start_acquisition_handler_with_no_files(
    mock_trigger_dag: MagicMock,
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
    mock_trigger_dag.assert_not_called()
    mock_add_raw_file_to_db.assert_not_called()


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.DagRun.generate_run_id")
@patch("dags.impl.watcher_impl.trigger_dag")
@patch("dags.impl.watcher_impl.datetime")
def test_start_acquisition_handler_with_single_file(
    mock_datetime: MagicMock,
    mock_trigger_dag: MagicMock,
    mock_generate: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_acquisition_handler with a single file."""
    # given
    raw_file_names = {"file1.raw": True}
    mock_get_xcom.return_value = raw_file_names
    run_ids = [
        "run_id1",
    ]
    mock_generate.side_effect = run_ids
    mock_datetime.now.return_value = 123
    ti = Mock()

    # when
    start_acquisition_handler(ti, **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    assert mock_trigger_dag.call_count == 1  # no magic numbers
    for n, call in enumerate(mock_trigger_dag.call_args_list):
        assert call[1]["dag_id"].endswith("instrument1")
        assert run_ids[n] == call[1]["run_id"]
        assert {"raw_file_name": list(raw_file_names.keys())[n]} == call[1]["conf"]
        assert not call[1]["replace_microseconds"]

    mock_add_raw_file_to_db.assert_called_once_with(
        "instrument1", "file1.raw", status="new"
    )


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl._add_raw_file_to_db")
@patch("dags.impl.watcher_impl.DagRun.generate_run_id")
@patch("dags.impl.watcher_impl.trigger_dag")
@patch("dags.impl.watcher_impl.datetime")
def test_start_acquisition_handler_with_multiple_files(
    mock_datetime: MagicMock,
    mock_trigger_dag: MagicMock,
    mock_generate: MagicMock,
    mock_add_raw_file_to_db: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_acquisition_handler with multiple files."""
    # given
    raw_file_names = {"file1.raw": True, "file2.raw": True, "file3.raw": False}
    mock_get_xcom.return_value = raw_file_names
    run_ids = ["run_id1", "run_id2", "run_id3"]
    mock_generate.side_effect = run_ids
    mock_datetime.now.return_value = 123

    # when
    start_acquisition_handler(Mock(), **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    assert mock_trigger_dag.call_count == 2  # noqa: PLR2004 no magic numbers
    for n, call in enumerate(mock_trigger_dag.call_args_list):
        assert call[1]["dag_id"].endswith("instrument1")
        assert run_ids[n] == call[1]["run_id"]
        assert {"raw_file_name": list(raw_file_names.keys())[n]} == call[1]["conf"]
        assert not call[1]["replace_microseconds"]

    mock_add_raw_file_to_db.assert_has_calls(
        [
            call("instrument1", "file1.raw", status="new"),
            call("instrument1", "file2.raw", status="new"),
            call("instrument1", "file3.raw", status="ignored"),
        ]
    )
