"""Tests for the file_checks module."""
# ruff: noqa:  PLR2004  # Magic value used in comparison

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from plugins.file_checks import FileIdentifier, FileRemovalError


@patch("plugins.file_checks.CopyPathProvider")
@patch("plugins.file_checks.get_file_size")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("plugins.file_checks.get_file_hash")
@patch("pathlib.Path.exists")
def test_check_file_success(
    mock_path_exists: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    mock_copy_path_provider: MagicMock,
) -> None:
    """Test that check_file succeeds when file sizes match."""
    mock_copy_path_provider.return_value.get_target_folder_path.return_value = Path(
        "/backup/instrument1/2024_08"
    )
    mock_backup_path.return_value = Path("/backup")
    mock_path_exists.return_value = True

    mock_get_file_size.side_effect = [100, 100]  # Same size for both files
    mock_get_file_hash.side_effect = [
        "some_hash",
        "some_hash",
    ]  # Same hash for both files
    file_path_to_check = Path("/instrument1/file.raw")

    rel_file_path = Path("file.raw")

    file_info_in_db = {"file.raw": (100, "some_hash")}

    raw_file = MagicMock()
    raw_file.file_info = file_info_in_db

    # when
    FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)

    # then
    assert mock_get_file_size.call_count == 2


@patch("plugins.file_checks.CopyPathProvider")
@patch("pathlib.Path.exists")
def test_check_file_not_existing_on_pool_backup(
    mock_path_exists: MagicMock,
    mock_copy_path_provider: MagicMock,
) -> None:
    """Test that check_file raises correctly when file_path_pool_backup does not exist."""
    mock_copy_path_provider.return_value.get_target_folder_path.return_value = Path(
        "/backup/instrument1/2024_08"
    )
    mock_path_exists.return_value = False

    file_path_to_check = Path("/instrument/file.raw")
    rel_file_path = Path("file.raw")

    raw_file = MagicMock()
    raw_file.file_info = {}

    # when
    with pytest.raises(FileRemovalError, match="File file.raw does not exist."):
        FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)


@pytest.mark.parametrize(
    ("file_size", "file_hash"), [(200, "some_hash"), (100, "some_other_hash")]
)
@patch("plugins.file_checks.CopyPathProvider")
@patch("plugins.file_checks.get_file_size")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("plugins.file_checks.get_file_hash")
@patch("pathlib.Path.exists")
def test_check_file_mismatch_instrument(  # noqa: PLR0913
    mock_path_exists: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    mock_copy_path_provider: MagicMock,
    file_size: int,
    file_hash: str,
) -> None:
    """Test that check_file raises FileRemovalError when pool backup size or hash doesn't match."""
    # ground truth:
    raw_file = MagicMock()
    raw_file.file_info = {"file.raw": (100, "some_hash")}

    mock_copy_path_provider.return_value.get_target_folder_path.return_value = Path(
        "/backup/instrument1/2024_08"
    )
    mock_backup_path.return_value = Path("/backup")
    mock_path_exists.return_value = True
    mock_get_file_size.return_value = file_size
    mock_get_file_hash.return_value = file_hash
    file_path_to_check = Path("/instrument/file.raw")
    rel_file_path = Path("file.raw")

    expected_hash = None if file_size != 100 else f"'{file_hash}'"

    # when
    with pytest.raises(
        FileRemovalError,
        match=f"File file.raw mismatch with instrument backup: size_to_remove={file_size} vs size_in_db=100, hash_to_remove={expected_hash} vs hash_in_db='some_hash'",
    ):
        FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)


@pytest.mark.parametrize(
    ("file_size", "file_hash"), [(200, "some_hash"), (100, "some_other_hash")]
)
@patch("plugins.file_checks.CopyPathProvider")
@patch("plugins.file_checks.get_file_size")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("plugins.file_checks.get_file_hash")
@patch("pathlib.Path.exists")
def test_check_file_mismatch_pool(  # noqa: PLR0913
    mock_path_exists: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    mock_copy_path_provider: MagicMock,
    file_size: int,
    file_hash: str,
) -> None:
    """Test that check_file raises FileRemovalError when pool backup size or hash doesn't match."""
    # ground truth:
    raw_file = MagicMock()
    raw_file.file_info = {"file.raw": (100, "some_hash")}

    mock_copy_path_provider.return_value.get_target_folder_path.return_value = Path(
        "/backup/instrument1/2024_08"
    )
    mock_backup_path.return_value = Path("/backup")
    mock_path_exists.return_value = True
    mock_get_file_size.side_effect = [100, file_size]
    mock_get_file_hash.side_effect = ["some_hash", file_hash]
    file_path_to_check = Path("/instrument/file.raw")

    rel_file_path = Path("file.raw")

    expected_hash = None if file_size != 100 else f"'{file_hash}'"
    # when
    with pytest.raises(
        FileRemovalError,
        match=f"File file.raw mismatch with pool backup: size_on_pool_backup={file_size} vs size_in_db=100, hash_on_pool_backup={expected_hash} vs hash_in_db='some_hash'",
    ):
        FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)
