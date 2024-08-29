"""Tests for the mover_impl module."""

import os
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.keys import DagContext, DagParams, XComKeys
from dags.impl.mover_impl import _check_main_file_to_move, get_files_to_move, move_files
from raw_file_wrapper_factory import MovePathProvider


@pytest.fixture()
def mock_raw_file() -> MagicMock:
    """Fixture for a raw file."""
    mock = MagicMock()
    mock.instrument_id = "instrument1"
    mock.original_name = "test_file.raw"
    return mock


@patch("dags.impl.mover_impl.get_raw_file_by_id")
@patch("dags.impl.mover_impl.RawFileWrapperFactory.create_write_wrapper")
@patch("dags.impl.mover_impl.put_xcom")
def test_get_files_to_move_correctly_puts_files_to_xcom(
    mock_put_xcom: MagicMock,
    mock_create_write_wrapper: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_raw_file: MagicMock,
) -> None:
    """Test get_files_to_move correctly puts files to xcom."""
    ti = MagicMock()

    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: 123}}

    mock_get_raw_file_by_id.return_value = mock_raw_file

    files_to_move = {Path("/src/file1"): Path("/dst/file1")}
    file_path_to_calculate_size = Path("/src/file1")

    mock_create_write_wrapper.return_value.get_files_to_move.return_value = (
        files_to_move
    )
    mock_create_write_wrapper.return_value.file_path_to_calculate_size.return_value = (
        file_path_to_calculate_size
    )

    # when
    get_files_to_move(ti, **kwargs)

    mock_create_write_wrapper.assert_called_once_with(
        mock_raw_file, path_provider=MovePathProvider
    )

    mock_put_xcom.assert_has_calls(
        [
            call(ti, XComKeys.FILES_TO_MOVE, {"/src/file1": "/dst/file1"}),
            call(ti, XComKeys.MAIN_FILE_TO_MOVE, str(file_path_to_calculate_size)),
        ]
    )


@patch.dict(os.environ, {"ENV_NAME": "production"})
@patch("dags.impl.mover_impl._check_main_file_to_move")
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_success(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
    mock_check_main_file_to_move: MagicMock,
) -> None:
    """Test move_raw_file success."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    mock_path.side_effect = [mock_src_path, mock_dst_path]

    mock_get_xcom.return_value = {"/src/file1": "/dst/file1"}
    ti = MagicMock()

    # when
    move_files(ti, **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_shutil_move.assert_called_once_with(mock_src_path, mock_dst_path)

    mock_dst_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_path.assert_has_calls([call("/src/file1"), call("/dst/file1")])
    mock_check_main_file_to_move.assert_called_once_with(
        ti, {"params": {"raw_file_id": "123"}}
    )


@patch.dict(os.environ, {"ENV_NAME": "NOT_production"})
@patch("dags.impl.mover_impl._check_main_file_to_move")
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_success_not_production(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
    mock_check_main_file_to_move: MagicMock,
) -> None:
    """Test move_raw_file success."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    mock_path.side_effect = [mock_src_path, mock_dst_path]

    mock_get_xcom.return_value = {mock_src_path: mock_dst_path}

    # when
    move_files(MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_shutil_move.assert_not_called()

    mock_dst_path.parent.mkdir.assert_not_called()
    mock_check_main_file_to_move.assert_called_once()


@patch("dags.impl.mover_impl._check_main_file_to_move")
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_source_not_exists(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
    mock_check_main_file_to_move: MagicMock,
) -> None:
    """Test move_raw_file raises FileNotFoundError if source does not exist."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = True

    mock_path.side_effect = [mock_src_path, mock_dst_path]

    mock_get_xcom.return_value = {mock_src_path: mock_dst_path}

    # when
    with pytest.raises(AirflowFailException):
        move_files(MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_shutil_move.assert_not_called()
    mock_check_main_file_to_move.assert_called_once()


@patch("dags.impl.mover_impl._check_main_file_to_move")
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_destination_exists(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
    mock_check_main_file_to_move: MagicMock,
) -> None:
    """Test move_raw_file FileExistsError if destination exists."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = True

    mock_path.side_effect = [mock_src_path, mock_dst_path]

    mock_get_xcom.return_value = {mock_src_path: mock_dst_path}

    # when
    with pytest.raises(AirflowFailException):
        move_files(MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_shutil_move.assert_not_called()
    mock_check_main_file_to_move.assert_called_once()


@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.get_raw_file_by_id")
@patch("dags.impl.mover_impl.get_file_size")
def test_file_size_matches_database_record(
    mock_get_file_size: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test _check_main_file_to_move success."""
    mock_get_file_size.return_value = 100
    mock_get_xcom.return_value = "path/to/file"

    raw_file = MagicMock()
    raw_file.size = 100
    mock_get_raw_file_by_id.return_value = raw_file

    # when
    _check_main_file_to_move(
        MagicMock(), {DagContext.PARAMS: {DagParams.RAW_FILE_ID: 1}}
    )
    # nothing raised: OK

    mock_get_raw_file_by_id.assert_called_once_with(1)
    mock_get_file_size.assert_called_once_with(Path("path/to/file"))


@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.get_raw_file_by_id")
@patch("dags.impl.mover_impl.get_file_size")
def test_file_size_does_not_match_database_record_raises_exception(
    mock_get_file_size: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that _check_main_file_to_move raises an exception if file size does not match the database record."""
    mock_get_file_size.return_value = 99
    mock_get_xcom.return_value = "path/to/file"

    raw_file = MagicMock()
    raw_file.size = 100
    mock_get_raw_file_by_id.return_value = raw_file

    # when
    with pytest.raises(AirflowFailException):
        _check_main_file_to_move(
            MagicMock(), {DagContext.PARAMS: {DagParams.RAW_FILE_ID: 1}}
        )

    mock_get_raw_file_by_id.assert_called_once_with(1)
    mock_get_file_size.assert_called_once_with(Path("path/to/file"))
