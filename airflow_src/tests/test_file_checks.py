"""Tests for the file_checks module."""
# ruff: noqa:  PLR2004  # Magic value used in comparison

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from plugins.file_checks import FileIdentifier


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
    assert FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)

    # then
    mock_get_file_size.assert_has_calls(
        [
            call(Path("/instrument1/file.raw"), verbose=False),
            call(Path("/instrument1/file.raw"), verbose=False),
        ]
    )
    mock_get_file_hash.assert_has_calls(
        [call(Path("/instrument1/file.raw")), call(Path("/instrument1/file.raw"))]
    )


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
    assert not FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)


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

    # when
    assert not FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)


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

    # when
    assert not FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)


@patch("plugins.file_checks.CopyPathProvider")
@patch("plugins.file_checks.get_file_size")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("plugins.file_checks.get_file_hash")
@patch("pathlib.Path.exists")
def test_check_file_old_format_fallback(
    mock_path_exists: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_backup_path: MagicMock,
    mock_get_file_size: MagicMock,
    mock_copy_path_provider: MagicMock,
) -> None:
    """Test that check_file handles old file_info format with full path as key."""
    mock_copy_path_provider.return_value.get_target_folder_path.return_value = Path(
        "/backup/instrument1/2024_08"
    )
    mock_backup_path.return_value = Path("/backup")
    mock_path_exists.return_value = True

    mock_get_file_size.side_effect = [100, 100]
    mock_get_file_hash.side_effect = ["some_hash", "some_hash"]
    file_path_to_check = Path("/instrument1/file.raw")

    rel_file_path = Path("file.raw")

    file_info_in_db = {"instrument1/2024_08/file.raw": (100, "some_hash")}

    raw_file = MagicMock()
    raw_file.file_info = file_info_in_db

    assert FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)


@patch("plugins.file_checks.CopyPathProvider")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("pathlib.Path.exists")
def test_check_file_raises_when_size_is_none(
    mock_path_exists: MagicMock,
    mock_backup_path: MagicMock,
    mock_copy_path_provider: MagicMock,
) -> None:
    """Test that check_file raises KeyError when size_in_db is None."""
    mock_copy_path_provider.return_value.get_target_folder_path.return_value = Path(
        "/backup/instrument1/2024_08"
    )
    mock_backup_path.return_value = Path("/backup")
    mock_path_exists.return_value = True

    file_path_to_check = Path("/instrument1/file.raw")
    rel_file_path = Path("file.raw")

    file_info_in_db = {"file.raw": (None, "some_hash")}

    raw_file = MagicMock()
    raw_file.file_info = file_info_in_db

    with pytest.raises(KeyError, match="has no size or hash information"):
        FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)


@patch("plugins.file_checks.CopyPathProvider")
@patch("plugins.file_checks.get_internal_backup_path")
@patch("pathlib.Path.exists")
def test_check_file_raises_when_hash_is_none(
    mock_path_exists: MagicMock,
    mock_backup_path: MagicMock,
    mock_copy_path_provider: MagicMock,
) -> None:
    """Test that check_file raises KeyError when hash_in_db is None."""
    mock_copy_path_provider.return_value.get_target_folder_path.return_value = Path(
        "/backup/instrument1/2024_08"
    )
    mock_backup_path.return_value = Path("/backup")
    mock_path_exists.return_value = True

    file_path_to_check = Path("/instrument1/file.raw")
    rel_file_path = Path("file.raw")

    file_info_in_db = {"file.raw": (100, None)}

    raw_file = MagicMock()
    raw_file.file_info = file_info_in_db

    with pytest.raises(KeyError, match="has no size or hash information"):
        FileIdentifier(raw_file).check_file(file_path_to_check, rel_file_path)
