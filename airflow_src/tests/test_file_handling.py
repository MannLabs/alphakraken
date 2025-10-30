"""Tests for the file_handling module."""

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
from _pytest._py.path import LocalPath
from airflow.exceptions import AirflowFailException
from common.settings import _INSTRUMENTS
from plugins.common.constants import BYTES_TO_GB
from plugins.file_handling import (
    HashMismatchError,
    _decide_if_copy_required,
    _identical_copy_exists,
    _md5hashes_to_etag,
    compare_paths,
    copy_file,
    get_disk_usage,
    get_file_creation_timestamp,
    get_file_hash,
    get_file_hash_with_etag,
    get_file_size,
    move_existing_file,
)


@patch.dict(_INSTRUMENTS, {"instrument1": {}})
@patch("os.stat")
def test_get_file_creation_timestamp(
    mock_stat: MagicMock,
) -> None:
    """Test get_file_creation_timestamp returns the expected values."""
    mock_stat.return_value.st_ctime = 42.0

    # when
    result = get_file_creation_timestamp("test_file.raw", "instrument1")

    assert result == 42.0


@patch.dict(_INSTRUMENTS, {"instrument1": {}})
def test_get_file_size() -> None:
    """Test get_file_size returns the expected values."""
    mock_path = MagicMock()
    mock_path.stat.return_value.st_size = 42.0

    # when
    result = get_file_size(mock_path)

    assert result == 42.0


@patch.dict(_INSTRUMENTS, {"instrument1": {}})
def test_get_file_size_default() -> None:
    """Test get_file_size returns the expected values."""
    mock_path = MagicMock()
    mock_path.exists.return_value = False

    # when
    result = get_file_size(mock_path, -1)

    assert result == -1


@patch.dict(_INSTRUMENTS, {"instrument1": {}})
def test_get_file_size_no_default_raises() -> None:
    """Test get_file_size returns the expected values."""
    mock_path = MagicMock()
    mock_path.exists.return_value = False

    # when
    with pytest.raises(FileNotFoundError):
        get_file_size(mock_path)


@patch("plugins.file_handling.shutil.disk_usage")
def test_get_disk_usage_returns_correct_values(mock_disk_usage: MagicMock) -> None:
    """Test get_disk_usage returns the expected values."""
    mock_disk_usage.return_value = (1000, 600, 400)

    total_gb, used_gb, free_gb = get_disk_usage(Path("/fake/path"))
    assert total_gb == 1000 * BYTES_TO_GB
    assert used_gb == 600 * BYTES_TO_GB
    assert free_gb == 400 * BYTES_TO_GB


@patch("plugins.file_handling.get_file_size", return_value=123)
@patch("plugins.file_handling.Path.open", new_callable=mock_open)
def test_get_file_hash(
    mock_file_open: MagicMock,
    mock_get_file_size: MagicMock,  # noqa: ARG001
) -> None:
    """Test get_file_hash."""
    mock_file_open.return_value.read.side_effect = [b"some_file_content", None]

    # when
    return_value = get_file_hash(Path("/test/file/path"))

    assert return_value == "faff66b0fba39e3a4961b45dc5f9826c"


@patch("plugins.file_handling.get_file_size", return_value=123)
@patch("plugins.file_handling.Path.open", new_callable=mock_open)
def test_get_file_hash_chunks(
    mock_file_open: MagicMock,
    mock_get_file_size: MagicMock,  # noqa: ARG001
) -> None:
    """Test get_file_hash with multiple chunks."""
    mock_file_open.return_value.read.side_effect = [
        b"some_",
        b"file_",
        b"content",
        b"",
    ]

    # when
    return_value = get_file_hash(Path("/test/file/path"))

    assert return_value == "faff66b0fba39e3a4961b45dc5f9826c"


@patch("plugins.file_handling.get_file_size", return_value=123)
@patch("plugins.file_handling.Path.open", new_callable=mock_open)
def test_get_file_hash_with_etag(
    mock_file_open: MagicMock,
    mock_get_file_size: MagicMock,  # noqa: ARG001
) -> None:
    """Test get_file_hash."""
    mock_file_open.return_value.read.side_effect = [b"some_file_content", None]

    # when
    return_value = get_file_hash_with_etag(Path("/test/file/path"), calculate_etag=True)

    assert return_value == (
        "faff66b0fba39e3a4961b45dc5f9826c",
        "faff66b0fba39e3a4961b45dc5f9826c",
    )


@patch("plugins.file_handling.get_file_size", return_value=123)
@patch("plugins.file_handling.Path.open", new_callable=mock_open)
def test_get_file_hash_with_etag_chunks(
    mock_file_open: MagicMock,
    mock_get_file_size: MagicMock,  # noqa: ARG001
) -> None:
    """Test get_file_hash with multiple chunks."""
    mock_file_open.return_value.read.side_effect = [
        b"some_",
        b"file_",
        b"content",
        b"",
    ]

    # when
    return_value = get_file_hash_with_etag(Path("/test/file/path"), calculate_etag=True)

    assert return_value == (
        "faff66b0fba39e3a4961b45dc5f9826c",
        "6de5b63da956997419b0f495fa7f265a-3",
    )


def test_md5hashes_to_etag_empty_file() -> None:
    """Test _md5hashes_to_etag returns MD5 of empty bytes for empty file."""
    result = _md5hashes_to_etag([])

    # MD5 hash of empty bytes
    assert result == "d41d8cd98f00b204e9800998ecf8427e"


def test_md5hashes_to_etag_single_part() -> None:
    """Test _md5hashes_to_etag returns plain MD5 hex for single part upload."""
    import hashlib

    single_hash = hashlib.md5(b"test_data_1").digest()  # noqa: S324

    result = _md5hashes_to_etag([single_hash])

    # Should return hex representation without part count
    assert result == "6bd1fc852555783416536b7af9172d22"


def test_md5hashes_to_etag_multipart_two_parts() -> None:
    """Test _md5hashes_to_etag returns correct format for two-part upload."""
    import hashlib

    hash1 = hashlib.md5(b"test_data_1").digest()  # noqa: S324
    hash2 = hashlib.md5(b"test_data_2").digest()  # noqa: S324

    result = _md5hashes_to_etag([hash1, hash2])

    # Should be MD5 of concatenated hashes with "-2" suffix
    assert result == "6abb33a475f0a19b502a691a155200d5-2"


def test_md5hashes_to_etag_multipart_three_parts() -> None:
    """Test _md5hashes_to_etag returns correct format for three-part upload."""
    import hashlib

    hash1 = hashlib.md5(b"test_data_1").digest()  # noqa: S324
    hash2 = hashlib.md5(b"test_data_2").digest()  # noqa: S324
    hash3 = hashlib.md5(b"test_data_3").digest()  # noqa: S324

    result = _md5hashes_to_etag([hash1, hash2, hash3])

    # Should be MD5 of concatenated hashes with "-3" suffix
    assert result == "c0f39841d0a3da5a047629eacf7e6270-3"


def test_md5hashes_to_etag_multipart_many_parts() -> None:
    """Test _md5hashes_to_etag handles large part count correctly."""
    # Simulate 42 parts (e.g., 21 GB file with 500 MB chunks)
    hashes = [b"\x00" * 16 for _ in range(42)]

    result = _md5hashes_to_etag(hashes)

    # Should have "-42" suffix
    assert result.endswith("-42")
    assert len(result.split("-")) == 2


@patch("plugins.file_handling.get_file_hash")
@patch("plugins.file_handling._decide_if_copy_required")
@patch("shutil.copy2")
@patch("plugins.file_handling.get_file_size")
def test_copy_file_copies_file_and_checks_hash(
    mock_get_file_size: MagicMock,
    mock_copy2: MagicMock,
    mock_decide_if_copy_required: MagicMock,
    mock_get_file_hash: MagicMock,
) -> None:
    """Test copy_file copies file and checks hash."""
    mock_decide_if_copy_required.return_value = True
    mock_get_file_hash.return_value = "some_hash"
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))
    dst_path.parent.exists.return_value = True

    # when
    result = copy_file(src_path, dst_path, "some_hash")

    assert result == (1000, "some_hash")
    mock_copy2.assert_called_once_with(src_path, dst_path)
    dst_path.parent.mkdir.assert_not_called()


@patch("plugins.file_handling.get_file_hash")
@patch("plugins.file_handling._decide_if_copy_required")
@patch("shutil.copy2")
@patch("plugins.file_handling.get_file_size")
def test_copy_file_copies_file_and_checks_hash_hash_mismatch_raises(
    mock_get_file_size: MagicMock,
    mock_copy2: MagicMock,  # noqa: ARG001
    mock_decide_if_copy_required: MagicMock,
    mock_get_file_hash: MagicMock,
) -> None:
    """Test copy_file copies file and checks hash, raises on mismatch."""
    mock_decide_if_copy_required.return_value = True
    mock_get_file_hash.return_value = "some_other_hash"
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))
    dst_path.parent.exists.return_value = True

    # when
    with pytest.raises(AirflowFailException):
        copy_file(src_path, dst_path, "some_hash")


@patch("plugins.file_handling.get_file_hash")
@patch("plugins.file_handling._decide_if_copy_required")
@patch("shutil.copy2")
@patch("plugins.file_handling.get_file_size")
def test_copy_file_copies_file_and_creates_directory(
    mock_get_file_size: MagicMock,
    mock_copy2: MagicMock,
    mock_decide_if_copy_required: MagicMock,
    mock_get_file_hash: MagicMock,
) -> None:
    """Test copy_file copies file and creates target directory."""
    mock_decide_if_copy_required.return_value = True
    mock_get_file_hash.return_value = "some_hash"
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))
    dst_path.parent.exists.return_value = False
    dst_path.parent.mkdir.return_value = None

    # when
    result = copy_file(src_path, dst_path, "some_hash")

    assert result == (1000, "some_hash")
    mock_copy2.assert_called_once_with(src_path, dst_path)
    dst_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)


@patch("plugins.file_handling._identical_copy_exists")
def test_decide_if_copy_required_yes(
    mock_identical_copy_exists: MagicMock,
) -> None:
    """Test decide_if_copy_required returns correctly if identical file exists."""
    mock_identical_copy_exists.return_value = False

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))

    # when
    result = _decide_if_copy_required(src_path, dst_path, "some_hash", overwrite=True)

    assert result


@patch("plugins.file_handling._identical_copy_exists")
def test_decide_if_copy_required_no(
    mock_identical_copy_exists: MagicMock,
) -> None:
    """Test decide_if_copy_required returns correctly if no file exists."""
    mock_identical_copy_exists.return_value = True

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))

    # when
    result = _decide_if_copy_required(src_path, dst_path, "some_hash", overwrite=True)

    assert not result


@patch("plugins.file_handling._identical_copy_exists")
@patch("plugins.file_handling.get_file_size")
def test_decide_if_copy_required_hash_mismatch_no_overwrite_file(
    mock_get_file_size: MagicMock,
    mock_identical_copy_exists: MagicMock,
) -> None:
    """Test decide_if_copy_required raises in case file is existsing and overwrite is not set."""
    mock_identical_copy_exists.side_effect = HashMismatchError
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))

    # when
    with pytest.raises(AirflowFailException):
        _ = _decide_if_copy_required(src_path, dst_path, "some_hash", overwrite=False)


@patch("plugins.file_handling._identical_copy_exists")
@patch("plugins.file_handling.get_file_size")
def test_decide_if_copy_required_hash_mismatch_overwrite_file(
    mock_get_file_size: MagicMock,
    mock_identical_copy_exists: MagicMock,
) -> None:
    """Test decide_if_copy_required overwrites existing file overwrite is set."""
    mock_identical_copy_exists.side_effect = HashMismatchError
    mock_get_file_size.return_value = 1000

    src_path = Path("/path/to/instrument/test_file.raw")
    dst_path = MagicMock(wraps=Path("/path/to/backup/test_file.raw"))

    # when
    result = _decide_if_copy_required(src_path, dst_path, "some_hash", overwrite=True)

    assert result


@patch("plugins.file_handling.get_file_hash")
@patch.object(Path, "exists")
def test_identical_copy_exists_file_not_existing(
    mock_exists: MagicMock, mock_get_file_hash: MagicMock
) -> None:
    """Test file_already_exists returns False when file does not exist."""
    mock_exists.return_value = False

    # when
    result = _identical_copy_exists(Path("/backup/test_file.raw"), "some_hash")

    mock_exists.assert_called_once()
    mock_get_file_hash.assert_not_called()
    assert not result


@patch("plugins.file_handling.get_file_hash")
@patch.object(Path, "exists")
def test_identical_copy_exists_hashes_match(
    mock_exists: MagicMock, mock_get_file_hash: MagicMock
) -> None:
    """Test file_already_exists returns True when hashes match."""
    mock_exists.return_value = True
    mock_get_file_hash.return_value = "some_hash"

    # when
    result = _identical_copy_exists(Path("/backup/test_file.raw"), "some_hash")

    mock_exists.assert_called_once()
    mock_get_file_hash.assert_called_once_with(Path("/backup/test_file.raw"))
    assert result


@patch("plugins.file_handling.get_file_hash")
@patch.object(Path, "exists")
def test_identical_copy_exists_hashes_dont_match(
    mock_exists: MagicMock, mock_get_file_hash: MagicMock
) -> None:
    """Test file_already_exists returns False when hashes don't match."""
    mock_exists.return_value = True
    mock_get_file_hash.return_value = "some_hash"

    # when
    with pytest.raises(HashMismatchError):
        _identical_copy_exists(Path("/backup/test_file.raw"), "some_other_hash")

    mock_exists.assert_called_once()
    mock_get_file_hash.assert_called_once_with(Path("/backup/test_file.raw"))


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


def test_move_existing_file_when_file_does_not_exist() -> None:
    """Test move_existing_file returns original path when file doesn't exist."""
    # given
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = False

    # when
    result = move_existing_file(mock_path)

    # then
    assert result == mock_path
    mock_path.rename.assert_not_called()


def test_move_existing_file_when_file_exists_single_backup() -> None:
    """Test move_existing_file moves file to .0.alphakraken.bkp when file exists."""
    # given
    mock_path = MagicMock(spec=Path)
    mock_path.exists.side_effect = [True, False]
    mock_path.parent = MagicMock(spec=Path)
    mock_path.stem = "testfile"
    mock_path.suffix = ".raw"

    mock_path2 = MagicMock(spec=Path)
    mock_path2.exists.return_value = False
    mock_path.parent.__truediv__.return_value = mock_path2

    expected_backup_path = mock_path.parent / "testfile.raw.0.alphakraken.bkp"

    # when
    result = move_existing_file(mock_path)

    # then
    assert result == expected_backup_path
    mock_path.rename.assert_called_once_with(expected_backup_path)


def test_move_existing_file_when_multiple_backups_exist() -> None:
    """Test move_existing_file increments backup number when previous backups exist."""
    # given
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.parent = MagicMock(spec=Path)
    mock_path.stem = "testfile"
    mock_path.suffix = ".raw"

    # Mock that .0 and .1 backups exist, but .2 doesn't
    backup_paths = []
    for i in range(3):
        backup_path = MagicMock(spec=Path)
        backup_path.exists.return_value = i < 2  # .0 and .1 exist, .2 doesn't
        backup_paths.append(backup_path)
        mock_path.parent.__truediv__.return_value = backup_path

    expected_backup_path = backup_paths[2]

    # when
    result = move_existing_file(mock_path)

    # then
    assert result == expected_backup_path
    mock_path.rename.assert_called_once_with(expected_backup_path)
