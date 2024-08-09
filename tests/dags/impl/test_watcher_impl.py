"""Tests for the watcher_impl module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from dags.impl.watcher_impl import get_raw_files, start_acquisition_handler
from shared.keys import OpArgs, XComKeys

SOME_INSTRUMENT_ID = "some_instrument_id"


@patch("dags.impl.watcher_impl.get_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_raw_files_with_existing_files_in_db(
    mock_put_xcom: MagicMock,
    mock_get_raw_files_from_db: MagicMock,
    mock_os_listdir: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test get_raw_files with existing files in the database."""
    # Given a list of raw files, some of which are already in the database
    mock_get_instrument_data_path.return_value = Path("path/to")

    mock_os_listdir.return_value = [
        "path/to/file1.raw",
        "path/to/file2.raw",
        "path/to/file3.raw",
    ]
    mock_get_raw_files_from_db.return_value = ["file1.raw", "file2.raw"]
    ti = Mock()

    # Call the function
    get_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    # The function should call put_xcom with the correct arguments
    mock_put_xcom.assert_called_once_with(ti, XComKeys.RAW_FILE_NAMES, ["file3.raw"])


@patch("dags.impl.watcher_impl.get_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_raw_files_with_no_existing_files_in_db(
    mock_put_xcom: MagicMock,
    mock_get_raw_files_from_db: MagicMock,
    mock_os_listdir: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test get_raw_files with no existing files in the database."""
    # Given a list of raw files, some of which are already in the database
    mock_get_instrument_data_path.return_value = Path("path/to")

    mock_os_listdir.return_value = [
        "path/to/file1.raw",
        "path/to/file2.raw",
        "path/to/file3.raw",
    ]
    mock_get_raw_files_from_db.return_value = []
    ti = Mock()

    # Call the function
    get_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    # The function should call put_xcom with the correct arguments
    mock_put_xcom.assert_called_once_with(
        ti, XComKeys.RAW_FILE_NAMES, ["file1.raw", "file2.raw", "file3.raw"]
    )


@patch("dags.impl.watcher_impl.get_instrument_data_path")
@patch("os.listdir")
@patch("dags.impl.watcher_impl.get_raw_file_names_from_db")
@patch("dags.impl.watcher_impl.put_xcom")
def test_get_raw_files_with_empty_directory(
    mock_put_xcom: MagicMock,
    mock_get_raw_files_from_db: MagicMock,
    mock_os_listdir: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test get_raw_files with an empty directory."""
    # Given a list of raw files, some of which are already in the database
    mock_get_instrument_data_path.return_value = Path("path/to")

    mock_os_listdir.return_value = []

    ti = Mock()

    # Call the function
    with pytest.raises(ValueError):
        get_raw_files(ti, **{OpArgs.INSTRUMENT_ID: SOME_INSTRUMENT_ID})

    # The function should call put_xcom with the correct arguments
    mock_put_xcom.assert_not_called()
    mock_get_raw_files_from_db.assert_not_called()


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl.DagRun.generate_run_id")
@patch("dags.impl.watcher_impl.trigger_dag")
@patch("dags.impl.watcher_impl.datetime")
def test_start_acquisition_handler_with_multiple_files(
    mock_datetime: MagicMock,
    mock_trigger_dag: MagicMock,
    mock_generate: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_acquisition_handler with multiple files."""
    # given
    raw_file_names = ["file1.raw", "file2.raw", "file3.raw"]
    mock_get_xcom.return_value = raw_file_names
    run_ids = ["run_id1", "run_id2", "run_id3"]
    mock_generate.side_effect = run_ids
    mock_datetime.now.return_value = 123
    ti = Mock()

    # when
    start_acquisition_handler(ti, **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    assert mock_trigger_dag.call_count == 3  # noqa: PLR2004 no magic numbers
    for n, call in enumerate(mock_trigger_dag.call_args_list):
        assert call[1]["dag_id"].endswith("instrument1")
        assert run_ids[n] == call[1]["run_id"]
        assert {"raw_file_name": raw_file_names[n]} == call[1]["conf"]
        assert not call[1]["replace_microseconds"]


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl.DagRun.generate_run_id")
@patch("dags.impl.watcher_impl.trigger_dag")
@patch("dags.impl.watcher_impl.datetime")
def test_start_acquisition_handler_with_single_file(
    mock_datetime: MagicMock,
    mock_trigger_dag: MagicMock,
    mock_generate: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test start_acquisition_handler with a single file."""
    # given
    raw_file_names = ["file1.raw"]
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
        assert {"raw_file_name": raw_file_names[n]} == call[1]["conf"]
        assert not call[1]["replace_microseconds"]


@patch("dags.impl.watcher_impl.get_xcom")
@patch("dags.impl.watcher_impl.trigger_dag")
def test_start_acquisition_handler_with_no_files(
    mock_trigger_dag: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test start_acquisition_handler with no files."""
    # given
    mock_get_xcom.return_value = []
    ti = Mock()

    # when
    start_acquisition_handler(ti, **{OpArgs.INSTRUMENT_ID: "instrument1"})

    # then
    mock_trigger_dag.assert_not_called()
