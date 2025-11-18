"""Tests for the file_checks module."""
# ruff: noqa:  PLR2004  # Magic value used in comparison

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from plugins.file_checks import FileRemovalError, check_file


@patch("plugins.file_checks.get_file_size")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("plugins.file_checks.get_file_hash")
def test_check_file_success(
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
) -> None:
    """Test that check_file succeeds when file sizes match."""
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
    check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)

    # then
    assert mock_get_file_size.call_count == 2


@patch("plugins.file_checks.get_file_size")
def test_check_file_not_existing_on_pool_backup(
    mock_get_file_size: MagicMock,  # noqa: ARG001
) -> None:
    """Test that check_file raises correctly when file_path_pool_backup does not exist."""
    file_path_to_remove = Path("/instrument/file.raw")
    file_path_pool_backup = MagicMock(wraps=Path("/backup/instrument/file.raw"))
    file_path_pool_backup.exists.return_value = False
    file_path_pool_backup.__str__.return_value = "some_file"

    # when
    with pytest.raises(FileRemovalError, match="File some_file does not exist."):
        check_file(file_path_to_remove, file_path_pool_backup, MagicMock())


@pytest.mark.parametrize(
    ("file_size", "file_hash"), [(200, "some_hash"), (100, "some_other_hash")]
)
@patch("plugins.file_checks.get_file_size")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("plugins.file_checks.get_file_hash")
def test_check_file_mismatch_instrument(
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    file_size: int,
    file_hash: str,
) -> None:
    """Test that check_file raises FileRemovalError when pool backup size or hash doesn't match."""
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
        check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)


@pytest.mark.parametrize(
    ("file_size", "file_hash"), [(200, "some_hash"), (100, "some_other_hash")]
)
@patch("plugins.file_checks.get_file_size")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("plugins.file_checks.get_file_hash")
def test_check_file_mismatch_pool(
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    file_size: int,
    file_hash: str,
) -> None:
    """Test that check_file raises FileRemovalError when pool backup size or hash doesn't match."""
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
        check_file(file_path_to_remove, file_path_pool_backup, file_info_in_db)
