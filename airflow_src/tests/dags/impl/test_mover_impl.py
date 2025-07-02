"""Tests for the mover_impl module."""

import os
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.keys import DagContext, DagParams, XComKeys
from common.settings import _INSTRUMENTS
from dags.impl.mover_impl import (
    _check_main_file_to_move,
    _get_files_to_move,
    _move_files,
    get_files_to_move,
    move_files,
)
from raw_file_wrapper_factory import MovePathProvider


@pytest.fixture
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
    main_file_path = Path("/src/file1")

    mock_create_write_wrapper.return_value.get_files_to_move.return_value = (
        files_to_move
    )
    mock_create_write_wrapper.return_value.main_file_path.return_value = main_file_path

    # when
    get_files_to_move(ti, **kwargs)

    mock_create_write_wrapper.assert_called_once_with(
        mock_raw_file, path_provider=MovePathProvider
    )

    mock_put_xcom.assert_has_calls(
        [
            call(ti, XComKeys.FILES_TO_MOVE, {"/src/file1": "/dst/file1"}),
            call(ti, XComKeys.MAIN_FILE_TO_MOVE, str(main_file_path)),
        ]
    )


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "some_type"}})
@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.Path")
@patch("dags.impl.mover_impl.get_raw_file_by_id")
@patch("dags.impl.mover_impl._check_main_file_to_move")
@patch("dags.impl.mover_impl._get_files_to_move")
@patch("dags.impl.mover_impl._move_files")
def test_move_file_success(  # noqa: PLR0913
    mock_move_files: MagicMock,
    mock_get_files_to_move: MagicMock,
    mock_check_main_file_to_move: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_path: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test move_raw_file makes correct calls."""
    mock_src_path1 = MagicMock()
    mock_dst_path1 = MagicMock()
    mock_src_path2 = MagicMock()
    mock_dst_path2 = MagicMock()

    mock_path.side_effect = [
        mock_src_path1,
        mock_dst_path1,
        mock_src_path2,
        mock_dst_path2,
    ]

    mock_get_xcom.return_value = {
        "/src/file1": "/dst/file1",
        "/src/file2": "/dst/file2",
    }
    ti = MagicMock()

    mock_get_files_to_move.return_value = (
        {mock_src_path1: mock_dst_path1},
        {mock_src_path2, mock_dst_path2},
    )
    raw_file = MagicMock()
    raw_file.instrument_id = "instrument1"
    mock_get_raw_file_by_id.return_value = raw_file

    # when
    move_files(ti, **{DagContext.PARAMS: {DagParams.RAW_FILE_ID: "123"}})

    mock_path.assert_has_calls([call("/src/file1"), call("/dst/file1")])
    mock_check_main_file_to_move.assert_called_once_with(
        ti, mock_get_raw_file_by_id.return_value
    )
    mock_get_files_to_move.assert_called_once_with(
        {mock_src_path1: mock_dst_path1, mock_src_path2: mock_dst_path2}
    )
    mock_move_files.assert_has_calls(
        [
            call({mock_src_path1: mock_dst_path1}),
            call({mock_src_path2, mock_dst_path2}, only_rename=True),
        ]
    )
    mock_get_raw_file_by_id.assert_called_once_with("123")


def test_get_files_to_move_success() -> None:
    """Test move_raw_file success."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    # when
    result = _get_files_to_move({mock_src_path: mock_dst_path})

    assert result == ({mock_src_path: mock_dst_path}, {})


def test_get_files_to_move_only_dst_exists_ok() -> None:
    """Test move_raw_file returns correctly if only destination is present."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = False
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = True

    # when
    assert _get_files_to_move({mock_src_path: mock_dst_path}) == ({}, {})


def test_get_files_to_move_both_files_dont_exist_raise() -> None:
    """Test move_raw_file raises if both files are not present."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = False
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = False

    # when
    with pytest.raises(AirflowFailException):
        _get_files_to_move({mock_src_path: mock_dst_path})


@patch("dags.impl.mover_impl.compare_paths")
def test_get_files_to_move_both_files_exist_but_different_raises(
    mock_compare_paths: MagicMock,
) -> None:
    """Test move_raw_file raises if both files are present and not equal."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = True

    mock_compare_paths.return_value = ["something"], [], []

    # when

    with pytest.raises(AirflowFailException):
        _get_files_to_move({mock_src_path: mock_dst_path})

    mock_compare_paths.assert_called_once_with(mock_src_path, mock_dst_path)


@patch("dags.impl.mover_impl.compare_paths")
def test_get_files_to_move_both_files_exist_but_are_equal(
    mock_compare_paths: MagicMock,
) -> None:
    """Test move_raw_file return correctly if both files are present and equal."""
    mock_src_path = MagicMock()
    mock_src_path.exists.return_value = True
    mock_dst_path = MagicMock()
    mock_dst_path.exists.return_value = True

    mock_compare_paths.return_value = [], [], []

    # when
    assert _get_files_to_move({mock_src_path: mock_dst_path}) == (
        {},
        {mock_src_path: mock_dst_path},
    )

    mock_compare_paths.assert_called_once_with(mock_src_path, mock_dst_path)


@patch.dict(os.environ, {"ENV_NAME": "production"})
@patch("dags.impl.mover_impl.shutil.move")
def test_move_files_success_production(
    mock_shutil_move: MagicMock,
) -> None:
    """Test _move_files success for two paths."""
    mock_src_path1, mock_dst_path1 = MagicMock(), MagicMock()
    mock_src_path2, mock_dst_path2 = MagicMock(), MagicMock()
    mock_dst_path1.parent.exists.return_value = False
    mock_dst_path2.parent.exists.return_value = True

    # when
    _move_files({mock_src_path1: mock_dst_path1, mock_src_path2: mock_dst_path2})

    mock_shutil_move.assert_has_calls(
        [
            call(mock_src_path1, mock_dst_path1),
            call(mock_src_path2, mock_dst_path2),
        ]
    )

    mock_dst_path1.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_dst_path2.parent.mkdir.assert_not_called()


@patch.dict(os.environ, {"ENV_NAME": "production"})
@patch("dags.impl.mover_impl.shutil.move")
def test_move_files_rename_success(
    mock_shutil_move: MagicMock,
) -> None:
    """Test _move_files success for two paths."""
    mock_src_path1, mock_dst_path1 = MagicMock(), MagicMock()
    mock_src_path2, mock_dst_path2 = MagicMock(), MagicMock()
    mock_dst_path1.parent.exists.return_value = False
    mock_dst_path2.parent.exists.return_value = True

    # when
    _move_files(
        {mock_src_path1: mock_dst_path1, mock_src_path2: mock_dst_path2},
        only_rename=True,
    )

    mock_shutil_move.assert_not_called()

    mock_src_path1.rename.assert_called_once_with(f"{mock_src_path1}.deleteme")
    mock_src_path2.rename.assert_called_once_with(f"{mock_src_path2}.deleteme")


@patch.dict(os.environ, {"ENV_NAME": "NOT_production"})
@patch("dags.impl.mover_impl.shutil.move")
def test_move_files_success_not_production(
    mock_shutil_move: MagicMock,
) -> None:
    """Test _move_files success for two paths in non-production."""
    mock_src_path1, mock_dst_path1 = MagicMock(), MagicMock()
    mock_src_path2, mock_dst_path2 = MagicMock(), MagicMock()

    # when
    _move_files({mock_src_path1: mock_dst_path1, mock_src_path2: mock_dst_path2})

    mock_shutil_move.assert_not_called()

    mock_dst_path1.parent.mkdir.assert_not_called()
    mock_dst_path2.parent.mkdir.assert_not_called()


@patch.dict(os.environ, {"ENV_NAME": "production"})
@patch("dags.impl.mover_impl.shutil.move")
def test_move_files_permission_error_not_dir_no_rename(
    mock_shutil_move: MagicMock,
) -> None:
    """Test _move_files raises PermissionError if shutil.move raises PermissionError and src_path is not a directory."""
    mock_src_path1, mock_dst_path1 = MagicMock(), MagicMock()

    mock_shutil_move.side_effect = PermissionError

    mock_src_path1.is_dir.return_value = False

    # when
    with pytest.raises(PermissionError):
        _move_files({mock_src_path1: mock_dst_path1})

    mock_shutil_move.assert_called_once_with(mock_src_path1, mock_dst_path1)

    mock_src_path1.rename.assert_not_called()


@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.get_file_size")
def test_check_main_file_to_movefile_size_matches_database_record(
    mock_get_file_size: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test _check_main_file_to_move success."""
    mock_get_file_size.return_value = 100
    mock_get_xcom.return_value = "path/to/file"

    raw_file = MagicMock()
    raw_file.size = 100

    # when
    _check_main_file_to_move(MagicMock(), raw_file)
    # nothing raised: OK

    mock_get_file_size.assert_called_once_with(Path("path/to/file"))


@patch("dags.impl.mover_impl.get_xcom")
@patch("dags.impl.mover_impl.get_file_size")
def test_check_main_file_to_movefile_size_does_not_match_database_record_raises_exception(
    mock_get_file_size: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that _check_main_file_to_move raises an exception if file size does not match the database record."""
    mock_get_file_size.return_value = 99
    mock_get_xcom.return_value = "path/to/file"

    raw_file = MagicMock()
    raw_file.size = 100

    # when
    with pytest.raises(AirflowFailException):
        _check_main_file_to_move(MagicMock(), raw_file)

    mock_get_file_size.assert_called_once_with(Path("path/to/file"))
