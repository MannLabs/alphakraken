"""Tests for the file_remover module."""
# ruff: noqa:  PLR2004  # Magic value used in comparison

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.keys import XComKeys
from dags.impl.remover_impl import (
    FileRemovalError,
    _change_folder_permissions,
    _check_file,
    _decide_on_raw_files_to_remove,
    _delete_empty_directory,
    _get_total_size,
    _remove_files,
    _remove_folder,
    _safe_remove_files,
    get_raw_files_to_remove,
    remove_raw_files,
)
from raw_file_wrapper_factory import RemovePathProvider


@patch("dags.impl.remover_impl.get_airflow_variable")
@patch(
    "dags.impl.remover_impl.get_instrument_ids",
    return_value=["instrument1", "instrument2"],
)
@patch("dags.impl.remover_impl._decide_on_raw_files_to_remove")
@patch("dags.impl.remover_impl.put_xcom")
def test_get_raw_files_to_remove(
    mock_put_xcom: MagicMock,
    mock_decide_on_raw_files_to_remove: MagicMock,
    mock_get_instrument_ids: MagicMock,  # noqa: ARG001
    mock_get_airflow_variable: MagicMock,
) -> None:
    """Test that get_raw_files_to_remove calls the correct functions and puts the result in XCom."""
    mock_ti = MagicMock()
    mock_decide_on_raw_files_to_remove.side_effect = [
        ["file1", "file2"],
        ["file3", "file4"],
    ]

    mock_get_airflow_variable.side_effect = [10, 42]

    # when
    get_raw_files_to_remove(mock_ti)

    mock_decide_on_raw_files_to_remove.assert_has_calls(
        [
            call(
                "instrument1",
                min_file_age=10,
                min_free_gb=42,
            ),
            call(
                "instrument2",
                min_file_age=10,
                min_free_gb=42,
            ),
        ]
    )
    mock_put_xcom.assert_has_calls(
        [
            call(
                mock_ti,
                XComKeys.FILES_TO_REMOVE,
                {"instrument1": ["file1", "file2"], "instrument2": ["file3", "file4"]},
            ),
            call(mock_ti, XComKeys.INSTRUMENTS_WITH_ERRORS, []),
        ]
    )
    mock_get_airflow_variable.assert_has_calls(
        [
            call("min_file_age_to_remove_in_days", 14),
            call("min_free_space_gb", "-1"),
        ]
    )


@patch("dags.impl.remover_impl.get_airflow_variable")
@patch(
    "dags.impl.remover_impl.get_instrument_ids",
    return_value=["instrument1", "instrument2"],
)
@patch("dags.impl.remover_impl._decide_on_raw_files_to_remove")
@patch("dags.impl.remover_impl.put_xcom")
def test_get_raw_files_to_remove_handle_error(
    mock_put_xcom: MagicMock,
    mock_decide_on_raw_files_to_remove: MagicMock,
    mock_get_instrument_ids: MagicMock,  # noqa: ARG001
    mock_get_airflow_variable: MagicMock,
) -> None:
    """Test that get_raw_files_to_remove gracefully handles errors."""
    mock_ti = MagicMock()
    mock_decide_on_raw_files_to_remove.side_effect = [
        ValueError("Error"),
        ["file3", "file4"],
    ]

    mock_get_airflow_variable.side_effect = [10, 42]

    # when
    get_raw_files_to_remove(mock_ti)

    mock_put_xcom.assert_has_calls(
        [
            call(
                mock_ti,
                XComKeys.FILES_TO_REMOVE,
                {"instrument2": ["file3", "file4"]},
            ),
            call(mock_ti, XComKeys.INSTRUMENTS_WITH_ERRORS, ["instrument1"]),
        ]
    )


@patch("dags.impl.remover_impl.get_internal_instrument_data_path")
@patch("dags.impl.remover_impl.get_raw_files_by_age")
@patch("dags.impl.remover_impl._get_total_size")
@patch("dags.impl.remover_impl.get_disk_usage")
def test_decide_on_raw_files_to_remove_ok(
    mock_get_disk_usage: MagicMock,
    mock_get_total_size: MagicMock,
    mock_get_raw_files_by_age: MagicMock,
    mock_get_internal_instrument_data_path: MagicMock,
) -> None:
    """Test that _decide_on_raw_files_to_remove returns the correct files to remove."""
    mock_get_disk_usage.return_value = (0, 0, 200)

    mock_get_total_size.side_effect = [
        FileRemovalError,
        (70 * 1024**3, 1),
        (30 * 1024**3, 1),
        (30 * 1024**3, 1),
    ]

    mock_get_raw_files_by_age.return_value = [
        MagicMock(id="file0"),  # skipped to due to FileRemovalError
        MagicMock(id="file1"),
        MagicMock(id="file2"),
        MagicMock(id="file3"),  # deletion not necessary
    ]
    mock_get_internal_instrument_data_path.return_value = MagicMock()

    # when
    result = _decide_on_raw_files_to_remove(
        "instrument1",
        min_free_gb=300,
        min_file_age=30,
    )

    assert result == ["file1", "file2"]


@patch("dags.impl.remover_impl.get_internal_instrument_data_path")
@patch("dags.impl.remover_impl.get_disk_usage")
def test_decide_on_raw_files_to_remove_nothing_to_remove_ok(
    mock_get_disk_usage: MagicMock,
    mock_get_internal_instrument_data_path: MagicMock,
) -> None:
    """Test that _decide_on_raw_files_to_remove returns empty list in case free space is already enough."""
    mock_get_disk_usage.return_value = (0, 0, 300)
    mock_path = MagicMock()
    mock_path.exists.return_value = True
    mock_get_internal_instrument_data_path.return_value = mock_path

    # when
    result = _decide_on_raw_files_to_remove(
        "instrument1",
        min_free_gb=300,
        min_file_age=30,
    )

    assert result == []


@patch("dags.impl.remover_impl.RawFileWrapperFactory")
@patch("dags.impl.remover_impl._check_file")
@patch("dags.impl.remover_impl.get_file_size")
def test_get_total_size_ok(
    mock_get_file_size: MagicMock,
    mock_check_file: MagicMock,  # noqa: ARG001
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test that _get_total_size returns correctly in case file exists."""
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_remove.return_value = {
        MagicMock(): MagicMock(),
        MagicMock(): MagicMock(),
    }

    mock_get_file_size.side_effect = [100.0, 1.0]
    mock_raw_file = MagicMock()

    # when
    assert _get_total_size(mock_raw_file) == (101.0, 2)


@patch("dags.impl.remover_impl.RawFileWrapperFactory")
def test_get_total_size_no_files_returned(
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test that _get_total_size returns correctly in case no files are returned."""
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_remove.return_value = {}

    mock_raw_file = MagicMock()

    # when
    assert _get_total_size(mock_raw_file) == (0, 0)


@patch("dags.impl.remover_impl.get_file_size")
@patch("dags.impl.remover_impl.get_internal_backup_path")
@patch("dags.impl.remover_impl.get_file_hash")
def test_check_file_success(
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
) -> None:
    """Test that _check_file succeeds when file sizes match."""
    mock_backup_path.return_value = Path("/backup")
    mock_get_file_size.side_effect = [100, 100]  # Same size for both files
    mock_get_file_hash.side_effect = [
        "some_hash",
        "some_hash",
    ]  # Same hash for both files
    file_path_to_remove = MagicMock(wraps=Path("/instrument/file.raw"))
    file_path_to_remove.exists.return_value = True
    file_path_pool_backup = MagicMock(wraps=Path("/backup/instrument/file.raw"))
    file_path_pool_backup.exists.return_value = True
    file_info_in_db = {"instrument/file.raw": (100, "some_hash")}

    # when
    _check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)

    # then
    assert mock_get_file_size.call_count == 2


@patch("dags.impl.remover_impl.get_file_size")
def test_check_file_not_existing_on_pool_backup(
    mock_get_file_size: MagicMock,  # noqa: ARG001
) -> None:
    """Test that _check_file raises correctly when file_path_pool_backup does not exist."""
    file_path_to_remove = Path("/instrument/file.raw")
    file_path_pool_backup = MagicMock(wraps=Path("/backup/instrument/file.raw"))
    file_path_pool_backup.exists.return_value = False
    file_path_pool_backup.__str__.return_value = "some_file"

    # when
    with pytest.raises(FileRemovalError, match="File some_file does not exist."):
        _check_file(file_path_to_remove, file_path_pool_backup, MagicMock())


@pytest.mark.parametrize(
    ("file_size", "file_hash"), [(200, "some_hash"), (100, "some_other_hash")]
)
@patch("dags.impl.remover_impl.get_file_size")
@patch("dags.impl.remover_impl.get_internal_backup_path")
@patch("dags.impl.remover_impl.get_file_hash")
def test_check_file_mismatch_instrument(
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    file_size: int,
    file_hash: str,
) -> None:
    """Test that _check_file raises FileRemovalError when pool backup size or hash doesn't match."""
    # ground truth:
    file_info_in_db = {"instrument/file.raw": (100, "some_hash")}

    mock_backup_path.return_value = Path("/backup")
    mock_get_file_size.return_value = file_size
    mock_get_file_hash.return_value = file_hash
    file_path_to_remove = Path("/instrument/file.raw")
    file_path_pool_backup = MagicMock(wraps=Path("/backup/instrument/file.raw"))
    file_path_pool_backup.exists.return_value = True

    expected_hash = None if file_size != 100 else f"'{file_hash}'"
    # when
    with pytest.raises(
        FileRemovalError,
        match=f"File instrument/file.raw mismatch with instrument backup: size_to_remove={file_size} vs size_in_db=100, hash_to_remove={expected_hash} vs hash_in_db='some_hash'",
    ):
        _check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)


@pytest.mark.parametrize(
    ("file_size", "file_hash"), [(200, "some_hash"), (100, "some_other_hash")]
)
@patch("dags.impl.remover_impl.get_file_size")
@patch("dags.impl.remover_impl.get_internal_backup_path")
@patch("dags.impl.remover_impl.get_file_hash")
def test_check_file_mismatch_pool(
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    file_size: int,
    file_hash: str,
) -> None:
    """Test that _check_file raises FileRemovalError when pool backup size or hash doesn't match."""
    # ground truth:
    file_info_in_db = {"instrument/file.raw": (100, "some_hash")}

    mock_backup_path.return_value = Path("/backup")
    mock_get_file_size.side_effect = [100, file_size]
    mock_get_file_hash.side_effect = ["some_hash", file_hash]
    file_path_to_remove = MagicMock(wraps=Path("/instrument/file.raw"))
    file_path_to_remove.exists.return_value = True
    file_path_pool_backup = MagicMock(wraps=Path("/backup/instrument/file.raw"))
    file_path_pool_backup.exists.return_value = True

    expected_hash = None if file_size != 100 else f"'{file_hash}'"
    # when
    with pytest.raises(
        FileRemovalError,
        match=f"File instrument/file.raw mismatch with pool backup: size_on_pool_backup={file_size} vs size_in_db=100, hash_on_pool_backup={expected_hash} vs hash_in_db='some_hash'",
    ):
        _check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)


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
@patch("dags.impl.remover_impl._change_folder_permissions")
@patch("dags.impl.remover_impl._remove_files")
@patch("dags.impl.remover_impl._remove_folder")
def test_safe_remove_files_success(  # noqa: PLR0913
    mock_remove_folder: MagicMock,
    mock_remove_files: MagicMock,
    mock_change_folder_permissions: MagicMock,
    mock_check_file: MagicMock,
    mock_wrapper_factory: MagicMock,
    mock_get_raw_file: MagicMock,
) -> None:
    """Test that _safe_remove_files successfully removes files when all checks pass."""
    mock_raw_file = MagicMock()
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.file_info = {"file1": (100, "hash1")}
    mock_get_raw_file.return_value = mock_raw_file

    mock_path_to_delete = MagicMock(wraps=Path("/instrument/Backup/file1"))
    mock_path_to_delete.exists.return_value = True

    mock_wrapper = MagicMock()
    mock_wrapper.get_files_to_remove.return_value = {
        mock_path_to_delete: Path("/backup/file1")
    }
    mock_wrapper.get_folder_to_remove.return_value = None
    mock_wrapper_factory.create_write_wrapper.return_value = mock_wrapper

    # when
    _safe_remove_files("raw_file_id")

    # then
    mock_check_file.assert_called_once_with(
        mock_path_to_delete, Path("/backup/file1"), mock_raw_file.file_info
    )
    mock_change_folder_permissions.assert_not_called()  # because get_folder_to_remove returned None
    mock_remove_files.assert_called_once_with([mock_path_to_delete])
    mock_remove_folder.assert_not_called()  # because get_folder_to_remove returned None
    mock_wrapper_factory.create_write_wrapper.assert_called_once_with(
        mock_raw_file, path_provider=RemovePathProvider
    )


@patch("dags.impl.remover_impl.get_raw_file_by_id")
@patch("dags.impl.remover_impl.RawFileWrapperFactory")
@patch("dags.impl.remover_impl._check_file")
@patch("dags.impl.remover_impl._change_folder_permissions")
@patch("dags.impl.remover_impl._remove_files")
@patch("dags.impl.remover_impl._remove_folder")
def test_safe_remove_files_folder_success(  # noqa: PLR0913
    mock_remove_folder: MagicMock,
    mock_remove_files: MagicMock,
    mock_change_folder_permissions: MagicMock,
    mock_check_file: MagicMock,
    mock_wrapper_factory: MagicMock,
    mock_get_raw_file: MagicMock,
) -> None:
    """Test that _safe_remove_files successfully removes files and the containing folders when all checks pass."""
    mock_raw_file = MagicMock()
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.file_info = {"file1": (100, "hash1")}
    mock_get_raw_file.return_value = mock_raw_file

    mock_path_to_delete = MagicMock(wraps=Path("/instrument/Backup/file1"))
    mock_path_to_delete.exists.return_value = True

    mock_wrapper = MagicMock()
    mock_wrapper.get_files_to_remove.return_value = {
        mock_path_to_delete: Path("/backup/file1")
    }
    mock_wrapper.get_folder_to_remove.return_value = Path("/instrument/Backup/file1")
    mock_wrapper_factory.create_write_wrapper.return_value = mock_wrapper

    # when
    _safe_remove_files("raw_file_id")

    # then
    mock_check_file.assert_called_once_with(
        mock_path_to_delete, Path("/backup/file1"), mock_raw_file.file_info
    )
    mock_change_folder_permissions.assert_called_once_with(
        Path("/instrument/Backup/file1")
    )
    mock_remove_files.assert_called_once_with([mock_path_to_delete])
    mock_remove_folder.assert_called_once_with(Path("/instrument/Backup/file1"))

    mock_wrapper_factory.create_write_wrapper.assert_called_once_with(
        mock_raw_file, path_provider=RemovePathProvider
    )


@patch("dags.impl.remover_impl.get_raw_file_by_id")
@patch("dags.impl.remover_impl.RawFileWrapperFactory")
@patch("dags.impl.remover_impl._check_file")
@patch("dags.impl.remover_impl._remove_files")
@patch("dags.impl.remover_impl._remove_folder")
def test_safe_remove_files_file_not_existing(
    mock_remove_folder: MagicMock,
    mock_remove_files: MagicMock,
    mock_check_file: MagicMock,
    mock_wrapper_factory: MagicMock,
    mock_get_raw_file: MagicMock,
) -> None:
    """Test that _safe_remove_files gracefully handles nonexisting file to delete."""
    mock_raw_file = MagicMock()
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.file_info = {"file1": (100, "hash1")}
    mock_get_raw_file.return_value = mock_raw_file

    mock_path_to_delete = MagicMock(wraps=Path("/instrument/file1"))
    mock_path_to_delete.exists.return_value = False

    mock_wrapper = MagicMock()
    mock_wrapper.get_files_to_remove.return_value = {
        mock_path_to_delete: Path("/backup/file1")
    }
    mock_wrapper.get_folder_to_remove.return_value = None
    mock_wrapper_factory.create_write_wrapper.return_value = mock_wrapper

    # when
    _safe_remove_files("raw_file_id")

    # then
    mock_check_file.assert_not_called()
    mock_remove_files.assert_not_called()
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

    mock_path_to_delete = MagicMock(wraps=Path("/instrument/file1"))
    mock_path_to_delete.exists.return_value = True

    mock_wrapper = MagicMock()
    mock_wrapper.get_files_to_remove.return_value = {
        mock_path_to_delete: Path("/backup/file1")
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


def test_change_folder_permissions() -> None:
    """Test that _change_folder_permissions changes directory paths' permissions correctly."""
    base_path = MagicMock(spec=Path)  # Path('/some/path')
    sub_path_1 = MagicMock(spec=Path)
    sub_path_2 = MagicMock(spec=Path)
    sub_path_1.is_dir.return_value = True
    sub_path_2.is_dir.return_value = False
    base_path.rglob.return_value = [sub_path_1, sub_path_2]

    # when
    _change_folder_permissions(base_path)

    base_path.rglob.assert_called_once_with("*")
    sub_path_1.chmod.assert_called_once_with(0o775)
    sub_path_2.chmod.assert_not_called()


@patch("dags.impl.remover_impl.get_xcom")
@patch("dags.impl.remover_impl._safe_remove_files")
def test_remove_raw_files_success(
    mock_safe_remove: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that remove_raw_files successfully removes files."""
    mock_ti = MagicMock()
    mock_get_xcom.side_effect = [{"instrument1": ["file1", "file2"]}, []]

    # when
    remove_raw_files(mock_ti)

    # then
    assert mock_safe_remove.call_count == 2


@patch("dags.impl.remover_impl.get_xcom")
@patch("dags.impl.remover_impl._safe_remove_files")
def test_remove_raw_files_upstream_task_failed(
    mock_safe_remove: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that remove_raw_files successfully raises if an upstream operation failed."""
    mock_ti = MagicMock()
    mock_get_xcom.side_effect = [{"instrument1": ["file1", "file2"]}, ["instrument2"]]

    # when
    with pytest.raises(AirflowFailException):
        remove_raw_files(mock_ti)

    # then
    assert mock_safe_remove.call_count == 2


@patch("dags.impl.remover_impl.get_xcom")
@patch("dags.impl.remover_impl._safe_remove_files")
def test_remove_raw_files_error(
    mock_safe_remove: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that remove_raw_files raises ValueError when errors occur."""
    mock_ti = MagicMock()

    mock_get_xcom.return_value = {"instrument1": ["file1", "file2"]}
    mock_safe_remove.side_effect = [None, FileRemovalError("Removal failed")]

    # when
    with pytest.raises(AirflowFailException):
        remove_raw_files(mock_ti)
