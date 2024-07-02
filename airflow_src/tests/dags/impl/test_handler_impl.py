"""Unit tests for handler_impl.py."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, call, mock_open, patch

import pytest
from common.keys import DagContext, DagParams, OpArgs
from dags.impl.handler_impl import (
    _copy_raw_file,
    _file_already_exists,
    _get_file_hash,
    copy_raw_file,
    start_acquisition_processor,
    update_raw_file_status,
)
from db.models import RawFileStatus


@patch("dags.impl.handler_impl.update_raw_file")
def test_update_raw_file_status_calls_update_with_correct_args(
    mock_update_status: MagicMock,
) -> None:
    """Test update_raw_file_status calls update with correct arguments."""
    ti = MagicMock()
    kwargs = {"params": {"raw_file_name": "test_file.raw"}}

    update_raw_file_status(ti, **kwargs)

    mock_update_status.assert_called_once_with(
        "test_file.raw", new_status=RawFileStatus.ACQUISITION_STARTED
    )


@patch("builtins.open", new_callable=mock_open)
def test_get_file_hash(mock_file_open: MagicMock) -> None:
    """Test get_file_hash."""
    mock_file_open.return_value.read.side_effect = [b"some_file_content", None]

    # when
    return_value = _get_file_hash("/test/file/path")

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
    return_value = _get_file_hash("/test/file/path")

    assert return_value == "faff66b0fba39e3a4961b45dc5f9826c"


@patch("dags.impl.handler_impl._get_file_hash")
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


@patch("dags.impl.handler_impl._get_file_hash")
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


@patch("dags.impl.handler_impl._get_file_hash")
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


@patch("dags.impl.handler_impl.get_internal_instrument_data_path")
@patch("dags.impl.handler_impl.get_internal_instrument_backup_path")
@patch("dags.impl.handler_impl._get_file_hash")
@patch("dags.impl.handler_impl._file_already_exists")
@patch("shutil.copy2")
def test_copy_raw_file_copies_file_and_checks_hash(
    mock_copy2: MagicMock,
    mock_file_exists: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_get_backup_path: MagicMock,
    mock_get_data_path: MagicMock,
) -> None:
    """Test copy_raw_file copies file and checks hash."""
    mock_get_data_path.return_value = Path("/path/to/data")
    mock_output_path = MagicMock()
    mock_get_backup_path.return_value.__truediv__.return_value = mock_output_path
    mock_output_path.stat.return_value.st_size = 1000

    mock_file_exists.return_value = False
    mock_get_file_hash.side_effect = ["some_hash", "some_hash"]

    # when
    _copy_raw_file(
        "test_file.raw",
        "instrument1",
    )

    mock_get_data_path.assert_called_once_with("instrument1")
    mock_get_backup_path.assert_called_once_with("instrument1")
    mock_copy2.assert_called_once_with(
        Path("/path/to/data/test_file.raw"), mock_output_path
    )


@patch("dags.impl.handler_impl.get_internal_instrument_data_path")
@patch("dags.impl.handler_impl.get_internal_instrument_backup_path")
@patch("dags.impl.handler_impl._get_file_hash")
@patch("dags.impl.handler_impl._file_already_exists")
@patch("shutil.copy2")
def test_copy_raw_file_copies_file_and_checks_hash_raises(
    mock_copy2: MagicMock,  # noqa: ARG001
    mock_file_exists: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_get_backup_path: MagicMock,
    mock_get_data_path: MagicMock,
) -> None:
    """Test copy_raw_file copies file and checks hash, raises on mismatch."""
    mock_get_data_path.return_value = Path("/path/to/data")
    mock_output_path = MagicMock()
    mock_get_backup_path.return_value.__truediv__.return_value = mock_output_path
    mock_output_path.stat.return_value.st_size = 1000

    mock_file_exists.return_value = False
    mock_get_file_hash.side_effect = ["some_hash", "some_other_hash"]

    # when
    with pytest.raises(ValueError):
        _copy_raw_file(
            "test_file.raw",
            "instrument1",
        )


@patch("dags.impl.handler_impl.get_internal_instrument_data_path")
@patch("dags.impl.handler_impl.get_internal_instrument_backup_path")
@patch("dags.impl.handler_impl._get_file_hash")
@patch("dags.impl.handler_impl._file_already_exists")
@patch("shutil.copy2")
def test_copy_raw_file_no_copy_if_file_present(
    mock_copy2: MagicMock,
    mock_file_exists: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_get_backup_path: MagicMock,
    mock_get_data_path: MagicMock,
) -> None:
    """Test copy_raw_file copies file and checks hash."""
    mock_get_data_path.return_value = Path("/path/to/data")
    mock_output_path = MagicMock()
    mock_get_backup_path.return_value.__truediv__.return_value = mock_output_path
    mock_output_path.stat.return_value.st_size = 1000

    mock_file_exists.return_value = True
    mock_get_file_hash.side_effect = ["some_hash", "some_hash"]

    # when
    _copy_raw_file(
        "test_file.raw",
        "instrument1",
    )

    mock_get_data_path.assert_called_once_with("instrument1")
    mock_get_backup_path.assert_called_once_with("instrument1")
    mock_copy2.assert_not_called()


@patch("dags.impl.handler_impl._copy_raw_file")
@patch("dags.impl.handler_impl._get_file_size")
@patch("dags.impl.handler_impl.update_raw_file")
def test_copy_raw_file_calls_update_with_correct_args(
    mock_update_status: MagicMock,
    mock_get_file_size: MagicMock,
    mock_copy_raw_file: MagicMock,
) -> None:
    """Test copy_raw_file calls update with correct arguments."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_name": "test_file.raw"},
        "instrument_id": "instrument1",
    }
    mock_get_file_size.return_value = 1000

    # when
    copy_raw_file(ti, **kwargs)

    # then
    mock_copy_raw_file("test_file.raw", "instrument1")
    mock_update_status.assert_has_calls(
        [
            call("test_file.raw", new_status=RawFileStatus.COPYING),
            call("test_file.raw", new_status=RawFileStatus.COPYING_FINISHED, size=1000),
        ]
    )


@patch("dags.impl.handler_impl.trigger_dag_run")
def test_start_acquisition_processor_with_single_file(
    mock_trigger_dag_run: MagicMock,
) -> None:
    """Test start_acquisition_processor with a single file."""
    # given
    raw_file_names = {"file1.raw": ("PID1", True)}
    ti = Mock()

    # when
    start_acquisition_processor(
        ti,
        **{
            OpArgs.INSTRUMENT_ID: "instrument1",
            DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "file1.raw"},
        },
    )

    # then
    assert mock_trigger_dag_run.call_count == 1  # no magic numbers
    for n, call_ in enumerate(mock_trigger_dag_run.call_args_list):
        assert call_.args[0] == ("acquisition_processor.instrument1")
        assert {
            "raw_file_name": list(raw_file_names.keys())[n],
        } == call_.args[1]
