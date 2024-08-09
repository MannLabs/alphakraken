"""Tests for the file_handling module."""

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
from plugins.common.settings import INSTRUMENTS
from plugins.file_handling import (
    _file_already_exists,
    _get_file_hash,
    copy_file,
    get_file_creation_timestamp,
    get_file_size,
)


@patch.dict(INSTRUMENTS, {"instrument1": {}})
@patch("os.stat")
def test_get_file_creation_timestamp(
    mock_stat: MagicMock,
) -> None:
    """Test get_file_creation_timestamp returns the expected values."""
    mock_stat.return_value.st_ctime = 42.0

    # when
    result = get_file_creation_timestamp("test_file.raw", "instrument1")

    assert result == 42.0  # noqa: PLR2004


@patch.dict(INSTRUMENTS, {"instrument1": {}})
def test_get_file_size() -> None:
    """Test get_file_size returns the expected values."""
    mock_path = MagicMock()
    mock_path.stat.return_value.st_size = 42.0

    # when
    result = get_file_size(mock_path)

    assert result == 42.0  # noqa: PLR2004


@patch("builtins.open", new_callable=mock_open)
def test_get_file_hash(mock_file_open: MagicMock) -> None:
    """Test get_file_hash."""
    mock_file_open.return_value.read.side_effect = [b"some_file_content", None]

    # when
    return_value = _get_file_hash(Path("/test/file/path"))

    assert return_value == "faff66b0fba39e3a4961b45dc5f9826c"


@patch("builtins.open", new_callable=mock_open)
def test_get_file_hash_chunks(mock_file_open: MagicMock) -> None:
    """Test get_file_hash with multiple chunks."""
    mock_file_open.return_value.read.side_effect = [
        b"some_",
        b"file_",
        b"content",
        None,
    ]

    # when
    return_value = _get_file_hash(Path("/test/file/path"))

    assert return_value == "faff66b0fba39e3a4961b45dc5f9826c"


@patch("plugins.file_handling._get_file_hash")
@patch("plugins.file_handling._file_already_exists")
@patch("shutil.copy2")
@patch("plugins.file_handling.get_file_size")
def test_copy_file_copies_file_and_checks_hash(
    mock_get_file_size: MagicMock,
    mock_copy2: MagicMock,
    mock_file_exists: MagicMock,
    mock_get_file_hash: MagicMock,
) -> None:
    """Test copy_file copies file and checks hash."""
    mock_file_exists.return_value = False
    mock_get_file_hash.side_effect = ["some_hash", "some_hash"]
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))
    dst_path.parent.exists.return_value = True

    # when
    copy_file(src_path, dst_path)

    mock_copy2.assert_called_once_with(src_path, dst_path)
    dst_path.parent.mkdir.assert_not_called()


@patch("plugins.file_handling._get_file_hash")
@patch("plugins.file_handling._file_already_exists")
@patch("shutil.copy2")
@patch("plugins.file_handling.get_file_size")
def test_copy_file_copies_file_and_checks_hash_raises(
    mock_get_file_size: MagicMock,
    mock_copy2: MagicMock,  # noqa: ARG001
    mock_file_exists: MagicMock,
    mock_get_file_hash: MagicMock,
) -> None:
    """Test copy_file copies file and checks hash, raises on mismatch."""
    mock_file_exists.return_value = False
    mock_get_file_hash.side_effect = ["some_hash", "some_other_hash"]
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))
    dst_path.parent.exists.return_value = True

    # when
    with pytest.raises(ValueError):
        copy_file(src_path, dst_path)


@patch("plugins.file_handling._get_file_hash")
@patch("plugins.file_handling._file_already_exists")
@patch("shutil.copy2")
@patch("plugins.file_handling.get_file_size")
def test_copy_file_copies_file_and_creates_directory(
    mock_get_file_size: MagicMock,
    mock_copy2: MagicMock,
    mock_file_exists: MagicMock,
    mock_get_file_hash: MagicMock,
) -> None:
    """Test copy_file copies file and creates target directory."""
    mock_file_exists.return_value = False
    mock_get_file_hash.side_effect = ["some_hash", "some_hash"]
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))
    dst_path.parent.exists.return_value = False
    dst_path.parent.mkdir.return_value = None

    # when
    copy_file(src_path, dst_path)

    mock_copy2.assert_called_once_with(src_path, dst_path)
    dst_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)


@patch("plugins.file_handling._get_file_hash")
@patch("plugins.file_handling._file_already_exists")
@patch("shutil.copy2")
def test_copy_file_no_copy_if_file_present(
    mock_copy2: MagicMock,
    mock_file_exists: MagicMock,
    mock_get_file_hash: MagicMock,
) -> None:
    """Test copy_file copies file and checks hash."""
    mock_file_exists.return_value = True
    mock_get_file_hash.side_effect = ["some_hash", "some_hash"]

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = Path("/path/to/backup/test_file.raw")

    # when
    copy_file(src_path, dst_path)

    mock_copy2.assert_not_called()


@patch("plugins.file_handling._get_file_hash")
@patch.object(Path, "exists")
def test_file_already_exists_file_not_existing(
    mock_exists: MagicMock, mock_get_file_hash: MagicMock
) -> None:
    """Test file_already_exists returns False when file does not exist."""
    mock_exists.return_value = False

    # when
    result = _file_already_exists(Path("/backup/test_file.raw"), "some_hash")

    mock_exists.assert_called_once()
    mock_get_file_hash.assert_not_called()
    assert result is False


@patch("plugins.file_handling._get_file_hash")
@patch.object(Path, "exists")
def test_file_already_exists_hashes_match(
    mock_exists: MagicMock, mock_get_file_hash: MagicMock
) -> None:
    """Test file_already_exists returns True when hashes match."""
    mock_exists.return_value = True
    mock_get_file_hash.return_value = "some_hash"

    # when
    result = _file_already_exists(Path("/backup/test_file.raw"), "some_hash")

    mock_exists.assert_called_once()
    mock_get_file_hash.assert_called_once_with(Path("/backup/test_file.raw"))
    assert result is True


@patch("plugins.file_handling._get_file_hash")
@patch.object(Path, "exists")
def test_file_already_exists_hashes_dont_match(
    mock_exists: MagicMock, mock_get_file_hash: MagicMock
) -> None:
    """Test file_already_exists returns False when hashes don't match."""
    mock_exists.return_value = True
    mock_get_file_hash.return_value = "some_hash"

    # when
    result = _file_already_exists(Path("/backup/test_file.raw"), "some_other_hash")

    mock_exists.assert_called_once()
    mock_get_file_hash.assert_called_once_with(Path("/backup/test_file.raw"))
    assert result is False
