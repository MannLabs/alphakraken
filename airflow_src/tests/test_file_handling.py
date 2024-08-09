"""Tests for the file_handling module."""

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
from _pytest._py.path import LocalPath
from airflow.exceptions import AirflowFailException
from plugins.common.settings import INSTRUMENTS
from plugins.file_handling import (
    _file_already_exists,
    _get_file_hash,
    compare_paths,
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


@patch("plugins.file_handling.Path.open", new_callable=mock_open)
def test_get_file_hash(mock_file_open: MagicMock) -> None:
    """Test get_file_hash."""
    mock_file_open.return_value.read.side_effect = [b"some_file_content", None]

    # when
    return_value = _get_file_hash(Path("/test/file/path"))

    assert return_value == "faff66b0fba39e3a4961b45dc5f9826c"


@patch("plugins.file_handling.Path.open", new_callable=mock_open)
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


# using a 'real' file system here to test the file handling functions
def _setup_tmpdir_folders(
    tmpdir: LocalPath,
    source_files: list[str],
    target_files: list[str],
    *,
    first_target_different_content: bool = False,
) -> tuple[Path, Path]:
    """Setup source and target folders with files in a temporary directory."""
    source = tmpdir.mkdir("source")
    target = tmpdir.mkdir("target")
    for file in source_files:
        p = source.join(file)
        p.write(file)
    for i, file in enumerate(target_files):
        p = target.join(file)
        string_to_write = file

        if first_target_different_content and i == 0:
            string_to_write = f"{string_to_write}_{i}"

        p.write(string_to_write)

    return Path(source), Path(target)


def _setup_tmpdir_files(
    tmpdir: LocalPath,
    source_file: str,
    target_file: str,
    *,
    target_different_content: bool = False,
) -> tuple[Path, Path]:
    """Setup source and target files in a temporary directory."""
    source = tmpdir.mkdir("source").join(Path(source_file))
    source.write(source_file)

    target = tmpdir.mkdir("target").join(Path(target_file))
    string_to_write = target_file
    if target_different_content:
        string_to_write = f"{string_to_write}_0"
    target.write(string_to_write)

    return Path(source), Path(target)


def test_compare_paths_files_match(tmpdir: LocalPath) -> None:
    """Test compare_paths returns empty lists when files match."""
    source_path, target_path = _setup_tmpdir_files(tmpdir, "file1", "file1")

    missing_files, different_files, items_only_in_target = compare_paths(
        source_path, target_path
    )

    assert missing_files == []
    assert different_files == []
    assert items_only_in_target == []


def test_compare_paths_files_correctly_identifies_missing(tmpdir: LocalPath) -> None:
    """Test compare_paths correctly identifies missing files."""
    source_path, target_path = _setup_tmpdir_files(tmpdir, "file1", "file1")
    target_path = target_path / "MISSING"

    missing_files, different_files, items_only_in_target = compare_paths(
        source_path, target_path
    )

    assert missing_files == ["file1"]
    assert different_files == []
    assert items_only_in_target == []


def test_compare_paths_files_orrectly_identifies_different_files(
    tmpdir: LocalPath,
) -> None:
    """Test compare_paths correctly identifies different files."""
    source_path, target_path = _setup_tmpdir_files(
        tmpdir, "file1", "file1", target_different_content=True
    )
    missing_files, different_files, items_only_in_target = compare_paths(
        source_path, target_path
    )

    assert missing_files == []
    assert different_files == ["file1"]
    assert items_only_in_target == []


def test_compare_paths_folders_all_match(tmpdir: LocalPath) -> None:
    """Test compare_paths returns empty lists when folders match."""
    source_path, target_path = _setup_tmpdir_folders(
        tmpdir, ["file1", "file2"], ["file1", "file2"]
    )

    missing_files, different_files, items_only_in_target = compare_paths(
        source_path, target_path
    )

    assert missing_files == []
    assert different_files == []
    assert items_only_in_target == []


def test_compare_paths_folders_correctly_identifies_missing_files(
    tmpdir: LocalPath,
) -> None:
    """Test compare_paths correctly identifies missing files in folders."""
    source_path, target_path = _setup_tmpdir_folders(
        tmpdir, ["file1", "file2"], ["file1"]
    )

    missing_files, different_files, items_only_in_target = compare_paths(
        source_path, target_path
    )

    assert missing_files == ["file2"]
    assert different_files == []
    assert items_only_in_target == []


def test_compare_paths_folders_correctly_identifies_different_files(
    tmpdir: LocalPath,
) -> None:
    """Test compare_paths correctly identifies different files in folders."""
    source_path, target_path = _setup_tmpdir_folders(
        tmpdir,
        ["file1", "file2"],
        ["file1", "file2"],
        first_target_different_content=True,
    )

    missing_files, different_files, items_only_in_target = compare_paths(
        source_path, target_path
    )

    assert missing_files == []
    assert different_files == ["file1"]
    assert items_only_in_target == []


def test_compare_paths_folders_correctly_identifies_items_only_in_target(
    tmpdir: LocalPath,
) -> None:
    """Test compare_paths correctly identifies items only in target folder."""
    source_path, target_path = _setup_tmpdir_folders(
        tmpdir, ["file1", "file2"], ["file1", "file2", "file3"]
    )

    missing_files, different_files, items_only_in_target = compare_paths(
        source_path, target_path
    )

    assert missing_files == []
    assert different_files == []
    assert items_only_in_target == ["file3"]


def test_compare_paths_raises_exception_if_source_is_dir_and_target_is_not() -> None:
    """Test compare_paths raises an exception if source is a directory and target is not."""
    source_path = MagicMock(spec=Path)
    target_path = MagicMock(spec=Path)
    source_path.is_dir.return_value = True
    target_path.is_dir.return_value = False

    with pytest.raises(AirflowFailException):
        compare_paths(source_path, target_path)
