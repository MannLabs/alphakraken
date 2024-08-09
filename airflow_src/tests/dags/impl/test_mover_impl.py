"""Tests for the mover_impl module."""

import os
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.keys import DagContext, DagParams, XComKeys
from dags.impl.mover_impl import get_files_to_move, move_files


@pytest.fixture()
def mock_raw_file() -> MagicMock:
    """Fixture for a raw file."""
    mock = MagicMock()
    mock.instrument_id = "instrument1"
    mock.original_name = "test_file.raw"
    return mock


@patch("dags.impl.mover_impl.get_raw_file_by_id")
@patch("dags.impl.mover_impl.get_internal_instrument_data_path")
@patch("dags.impl.mover_impl.RawFileWrapperFactory.create_copy_wrapper")
@patch("dags.impl.mover_impl.put_xcom")
def test_get_files_to_move_correctly_puts_files_to_xcom(
    mock_put_xcom: MagicMock,
    mock_create_copy_wrapper: MagicMock,
    mock_get_internal_instrument_data_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_raw_file: MagicMock,
) -> None:
    """Test get_files_to_move correctly puts files to xcom."""
    ti = MagicMock()

    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: 123}}

    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_dst_path = MagicMock()
    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value = (
        mock_dst_path
    )

    files_to_move = {Path("/src/file1"): Path("/dst/file1")}

    mock_create_copy_wrapper.return_value.get_files_to_move.return_value = files_to_move

    # when
    get_files_to_move(ti, **kwargs)

    mock_put_xcom.assert_called_once_with(
        ti, XComKeys.FILES_TO_MOVE, {"/src/file1": "/dst/file1"}
    )
    mock_create_copy_wrapper.assert_called_once_with(
        "instrument1", mock_raw_file, mock_dst_path
    )


@patch.dict(os.environ, {"ENV_NAME": "production"})
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_success(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
) -> None:
    """Test move_raw_file success."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    mock_path.side_effect = [mock_src_path, mock_dst_path]

    mock_get_xcom.return_value = {"/src/file1": "/dst/file1"}

    # when
    move_files(MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_shutil_move.assert_called_once_with(mock_src_path, mock_dst_path)

    mock_dst_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_path.assert_has_calls([call("/src/file1"), call("/dst/file1")])


@patch.dict(os.environ, {"ENV_NAME": "NOT_production"})
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_success_not_production(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
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


@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_source_not_exists(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
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


@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
def test_move_raw_file_destination_exists(
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
    mock_shutil_move: MagicMock,
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
