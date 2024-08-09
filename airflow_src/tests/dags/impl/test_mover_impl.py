"""Tests for the mover_impl module."""

import os
from unittest.mock import MagicMock, patch

import pytest
from common.keys import DagContext, DagParams
from dags.impl.mover_impl import move_raw_file


@pytest.fixture()
def mock_raw_file() -> MagicMock:
    """Fixture for a raw file."""
    mock = MagicMock()
    mock.instrument_id = "instrument1"
    mock.original_name = "test_file.raw"
    return mock


@patch.dict(os.environ, {"ENV_NAME": "production"})
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_internal_instrument_data_path")
@patch("dags.impl.mover_impl.get_raw_file_by_id")
def test_move_raw_file_success(
    mock_get_raw_file_by_id: MagicMock,
    mock_get_internal_instrument_data_path: MagicMock,
    mock_shutil_move: MagicMock,
    mock_raw_file: MagicMock,
) -> None:
    """Test move_raw_file success."""
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value = (
        mock_src_path
    )
    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value.__truediv__.return_value = mock_dst_path

    # when
    move_raw_file(MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_shutil_move.assert_called_once_with(mock_src_path, mock_dst_path)

    mock_dst_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)


@patch.dict(os.environ, {"ENV_NAME": "NOT_production"})
@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_internal_instrument_data_path")
@patch("dags.impl.mover_impl.get_raw_file_by_id")
def test_move_raw_file_success_not_production(
    mock_get_raw_file_by_id: MagicMock,
    mock_get_internal_instrument_data_path: MagicMock,
    mock_shutil_move: MagicMock,
    mock_raw_file: MagicMock,
) -> None:
    """Test move_raw_file success."""
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value = (
        mock_src_path
    )
    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value.__truediv__.return_value = mock_dst_path

    # when
    move_raw_file(MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_shutil_move.assert_not_called()

    mock_dst_path.parent.mkdir.assert_not_called()


@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_internal_instrument_data_path")
@patch("dags.impl.mover_impl.get_raw_file_by_id")
def test_move_raw_file_source_not_exists(
    mock_get_raw_file_by_id: MagicMock,
    mock_get_internal_instrument_data_path: MagicMock,
    mock_shutil_move: MagicMock,
    mock_raw_file: MagicMock,
) -> None:
    """Test move_raw_file raises FileNotFoundError if source does not exist."""
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = False
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value = (
        mock_src_path
    )
    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value.__truediv__.return_value = mock_dst_path

    # when
    with pytest.raises(FileNotFoundError):
        move_raw_file(
            MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}}
        )

    mock_shutil_move.assert_not_called()


@patch("dags.impl.mover_impl.shutil.move")
@patch("dags.impl.mover_impl.get_internal_instrument_data_path")
@patch("dags.impl.mover_impl.get_raw_file_by_id")
def test_move_raw_file_destination_exists(
    mock_get_raw_file_by_id: MagicMock,
    mock_get_internal_instrument_data_path: MagicMock,
    mock_shutil_move: MagicMock,
    mock_raw_file: MagicMock,
) -> None:
    """Test move_raw_file FileExistsError if destination exists."""
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = True

    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value = (
        mock_src_path
    )
    mock_get_internal_instrument_data_path.return_value.__truediv__.return_value.__truediv__.return_value = mock_dst_path

    # when
    with pytest.raises(FileExistsError):
        move_raw_file(
            MagicMock(), **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}}
        )

    mock_shutil_move.assert_not_called()
