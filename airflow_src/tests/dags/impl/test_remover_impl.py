"""Tests for the file_remover module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from common.keys import XComKeys
from dags.impl.remover_impl import (
    FileRemovalError,
    _check_file,
    _delete_empty_directory,
    _remove_files,
    _remove_folder,
    _safe_remove_files,
    get_raw_files_to_remove,
    remove_raw_files,
)
from raw_file_wrapper_factory import RemovePathProvider


@patch("dags.impl.remover_impl.get_airflow_variable")
@patch("dags.impl.remover_impl.get_raw_file_ids_older_than")
@patch("dags.impl.remover_impl.put_xcom")
def test_get_raw_files_to_remove(
    mock_put_xcom: MagicMock,
    mock_get_raw_file_ids_older_than: MagicMock,
    mock_get_airflow_variable: MagicMock,
) -> None:
    """Test that get_raw_files_to_remove calls the correct functions and puts the result in XCom."""
    mock_ti = MagicMock()
    mock_get_raw_file_ids_older_than.return_value = ["file1", "file2"]
    mock_get_airflow_variable.return_value = 42

    # when
    get_raw_files_to_remove(mock_ti)

    # then
    mock_get_raw_file_ids_older_than.assert_called_once_with(42)
    mock_put_xcom.assert_called_once_with(
        mock_ti, XComKeys.FILES_TO_REMOVE, ["file1", "file2"]
    )
    mock_get_airflow_variable.assert_called_once_with(
        "min_file_age_to_remove_in_days", 30
    )


@patch("dags.impl.remover_impl.get_file_size")
@patch("dags.impl.remover_impl.get_internal_backup_path")
def test_check_file_success(
    mock_backup_path: MagicMock, mock_get_file_size: MagicMock
) -> None:
    """Test that _check_file succeeds when file sizes match."""
    mock_backup_path.return_value = Path("/backup")
    mock_get_file_size.side_effect = [100, 100]  # Same size for both files
    file_path_to_remove = MagicMock(wraps=Path("/instrument/file.raw"))
    file_path_to_remove.exists.return_value = True
    file_path_pool_backup = Path("/backup/instrument/file.raw")
    file_info_in_db = {"instrument/file.raw": (100, "hash")}

    # when
    _check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)

    # then
    assert mock_get_file_size.call_count == 2  # noqa: PLR2004


@patch("dags.impl.remover_impl.get_file_size")
@patch("dags.impl.remover_impl.get_internal_backup_path")
def test_check_file_mismatch_pool(
    mock_backup_path: MagicMock, mock_get_file_size: MagicMock
) -> None:
    """Test that _check_file raises FileRemovalError when pool backup size doesn't match."""
    mock_backup_path.return_value = Path("/backup")
    mock_get_file_size.side_effect = [100, 200]  # Different sizes
    file_path_to_remove = MagicMock(wraps=Path("/instrument/file.raw"))
    file_path_to_remove.exists.return_value = True
    file_path_pool_backup = Path("/backup/instrument/file.raw")
    file_info_in_db = {"instrument/file.raw": (100, "hash")}

    # when
    with pytest.raises(FileRemovalError):
        _check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)


@patch("dags.impl.remover_impl.get_file_size")
@patch("dags.impl.remover_impl.get_internal_backup_path")
def test_check_file_mismatch_db(
    mock_backup_path: MagicMock, mock_get_file_size: MagicMock
) -> None:
    """Test that _check_file raises FileRemovalError when DB size doesn't match."""
    mock_backup_path.return_value = Path("/backup")
    mock_get_file_size.side_effect = [100, 100]  # Same size for both files
    file_path_to_remove = MagicMock(wraps=Path("/instrument/file.raw"))
    file_path_to_remove.exists.return_value = True
    file_path_pool_backup = Path("/backup/instrument/file.raw")
    file_info_in_db = {"instrument/file.raw": (200, "hash")}  # Different size in DB

    # when
    with pytest.raises(FileRemovalError):
        _check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)


@patch("dags.impl.remover_impl.get_file_size")
@patch("dags.impl.remover_impl.get_internal_backup_path")
def test_check_file_does_not_exists(
    mock_backup_path: MagicMock, mock_get_file_size: MagicMock
) -> None:
    """Test that _check_file does not raise if file does not exist."""
    mock_backup_path.return_value = MagicMock()
    file_path_to_remove = MagicMock()
    file_path_to_remove.exists.return_value = False
    file_path_pool_backup = MagicMock()
    file_info_in_db = MagicMock()

    # when
    _check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)

    assert mock_get_file_size.call_count == 0


@patch("dags.impl.remover_impl.get_env_variable")
def test_remove_files_production(mock_get_env: MagicMock) -> None:
    """Test that _remove_files removes files in production environment."""
    mock_get_env.return_value = "production"
    file_paths = [MagicMock(spec=Path), MagicMock(spec=Path)]

    # when
    _remove_files(file_paths)

    # then
    for file_path in file_paths:
        file_path.unlink.assert_called_once()


@patch("dags.impl.remover_impl.get_env_variable")
def test_remove_files_non_production(mock_get_env: MagicMock) -> None:
    """Test that _remove_files doesn't remove files in non-production environment."""
    mock_get_env.return_value = "development"
    file_paths = [MagicMock(spec=Path), MagicMock(spec=Path)]

    # when
    _remove_files(file_paths)

    # then
    for file_path in file_paths:
        file_path.unlink.assert_not_called()


@patch("dags.impl.remover_impl.get_env_variable")
def test_remove_folder_production(mock_get_env: MagicMock) -> None:
    """Test that _remove_folder removes folder in production environment."""
    mock_get_env.return_value = "production"
    folder_path = MagicMock(spec=Path)
    folder_path.exists.return_value = True
    folder_path.is_dir.return_value = True

    # when
    _remove_folder(folder_path)

    # then
    folder_path.rmdir.assert_called_once()


@patch("dags.impl.remover_impl.get_env_variable")
def test_remove_folder_non_production(mock_get_env: MagicMock) -> None:
    """Test that _remove_folder doesn't remove folder in non-production environment."""
    mock_get_env.return_value = "development"
    folder_path = MagicMock(spec=Path)
    folder_path.exists.return_value = True
    folder_path.is_dir.return_value = True

    # when
    _remove_folder(folder_path)

    # then
    folder_path.rmdir.assert_not_called()


@patch("dags.impl.remover_impl.get_raw_file_by_id")
@patch("dags.impl.remover_impl.RawFileWrapperFactory")
@patch("dags.impl.remover_impl._check_file")
@patch("dags.impl.remover_impl._remove_files")
@patch("dags.impl.remover_impl._remove_folder")
def test_safe_remove_files_success(
    mock_remove_folder: MagicMock,
    mock_remove_files: MagicMock,
    mock_check_file: MagicMock,
    mock_wrapper_factory: MagicMock,
    mock_get_raw_file: MagicMock,
) -> None:
    """Test that _safe_remove_files successfully removes files when all checks pass."""
    mock_raw_file = MagicMock()
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.file_info = {"file1": (100, "hash1")}
    mock_get_raw_file.return_value = mock_raw_file

    mock_wrapper = MagicMock()
    mock_wrapper.get_files_to_remove.return_value = {
        Path("/instrument/file1"): Path("/backup/file1")
    }
    mock_wrapper.get_folder_to_remove.return_value = None
    mock_wrapper_factory.create_write_wrapper.return_value = mock_wrapper

    # when
    _safe_remove_files("raw_file_id")

    # then
    mock_check_file.assert_called_once()
    mock_remove_files.assert_called_once()
    mock_remove_folder.assert_not_called()  # because get_folder_to_remove returned None
    mock_wrapper_factory.create_write_wrapper.assert_called_once_with(
        mock_raw_file, path_provider=RemovePathProvider
    )


@patch("dags.impl.remover_impl.get_raw_file_by_id")
@patch("dags.impl.remover_impl.RawFileWrapperFactory")
@patch("dags.impl.remover_impl._check_file")
def test_safe_remove_files_check_error(
    mock_check_file: MagicMock,
    mock_wrapper_factory: MagicMock,
    mock_get_raw_file: MagicMock,
) -> None:
    """Test that _safe_remove_files raises FileRemovalError when a check fails."""
    mock_raw_file = MagicMock()
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.file_info = {"file1": (100, "hash1")}
    mock_get_raw_file.return_value = mock_raw_file

    mock_wrapper = MagicMock()
    mock_wrapper.get_files_to_remove.return_value = {
        Path("/instrument/file1"): Path("/backup/file1")
    }
    mock_wrapper_factory.create_write_wrapper.return_value = mock_wrapper

    mock_check_file.side_effect = FileRemovalError("Check failed")

    # when
    with pytest.raises(FileRemovalError):
        _safe_remove_files("raw_file_id")


def test_delete_empty_directory() -> None:
    """Test that delete_empty_directory removes empty directories."""
    mock_dir = MagicMock(spec=Path)
    mock_subdir = MagicMock(spec=Path)
    mock_dir.glob.return_value = [mock_subdir]
    mock_subdir.is_dir.return_value = True

    # when
    _delete_empty_directory(mock_dir)

    # then
    mock_subdir.rmdir.assert_called_once()
    mock_dir.rmdir.assert_called_once()


@patch("dags.impl.remover_impl.get_xcom")
@patch("dags.impl.remover_impl._safe_remove_files")
def test_remove_raw_files_success(
    mock_safe_remove: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that remove_raw_files successfully removes files."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = ["file1", "file2"]

    # when
    remove_raw_files(mock_ti)

    # then
    assert mock_safe_remove.call_count == 2  # noqa: PLR2004


@patch("dags.impl.remover_impl.get_xcom")
@patch("dags.impl.remover_impl._safe_remove_files")
def test_remove_raw_files_error(
    mock_safe_remove: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that remove_raw_files raises ValueError when errors occur."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = ["file1", "file2"]
    mock_safe_remove.side_effect = [None, FileRemovalError("Removal failed")]

    # when
    with pytest.raises(ValueError):
        remove_raw_files(mock_ti)
