"""Unit tests for monitor_impl.py."""

from unittest.mock import MagicMock, Mock, call, mock_open, patch

from common.keys import DagContext, DagParams, OpArgs
from dags.impl.monitor_impl import (
    _get_file_hash,
    copy_raw_file,
    start_acquisition_handler,
    update_raw_file_status,
)
from db.models import RawFileStatus


@patch("dags.impl.monitor_impl.update_raw_file")
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
    return_value = _get_file_hash("/test/file/path")

    assert return_value == "faff66b0fba39e3a4961b45dc5f9826c"


@patch("shutil.copy2")
@patch("dags.impl.monitor_impl.Path")
@patch("dags.impl.monitor_impl._get_file_size")
@patch("dags.impl.monitor_impl.update_raw_file")
def test_copy_raw_file_calls_update_with_correct_args(
    mock_update_status: MagicMock,
    mock_get_file_size: MagicMock,
    mock_path: MagicMock,  # noqa: ARG001
    mock_copy2: MagicMock,  # noqa: ARG001
) -> None:
    """Test copy_raw_file calls update with correct arguments."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_name": "test_file.raw"},
        "instrument_id": "instrument1",
    }
    mock_get_file_size.return_value = 1000

    copy_raw_file(ti, **kwargs)

    mock_get_file_size.assert_called_once_with("test_file.raw", "instrument1")

    mock_update_status.assert_has_calls(
        [
            call("test_file.raw", new_status=RawFileStatus.COPYING),
            call("test_file.raw", new_status=RawFileStatus.COPYING_FINISHED, size=1000),
        ]
    )


@patch("dags.impl.monitor_impl.trigger_dag_run")
def test_start_acquisition_handler_with_single_file(
    mock_trigger_dag_run: MagicMock,
) -> None:
    """Test start_acquisition_handler with a single file."""
    # given
    raw_file_names = {"file1.raw": ("PID1", True)}
    ti = Mock()

    # when
    start_acquisition_handler(
        ti,
        **{
            OpArgs.INSTRUMENT_ID: "instrument1",
            DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "file1.raw"},
        },
    )

    # then
    assert mock_trigger_dag_run.call_count == 1  # no magic numbers
    for n, call_ in enumerate(mock_trigger_dag_run.call_args_list):
        assert call_.args[0] == ("acquisition_handler.instrument1")
        assert {
            "raw_file_name": list(raw_file_names.keys())[n],
        } == call_.args[1]
