"""Unit tests for handler_impl.py."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

from common.keys import DagContext, DagParams, OpArgs
from dags.impl.handler_impl import (
    copy_raw_file,
    start_acquisition_processor,
)
from db.models import RawFileStatus


@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.copy_file")
@patch("dags.impl.handler_impl.RawFileWrapperFactory")
@patch("dags.impl.handler_impl.get_file_size")
@patch("dags.impl.handler_impl.update_raw_file")
def test_copy_raw_file_calls_update_with_correct_args(
    mock_update_status: MagicMock,
    mock_get_file_size: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_copy_file: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test copy_raw_file calls update with correct arguments."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
        "instrument_id": "test1",
    }
    mock_raw_file = MagicMock()
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_file_size.return_value = 1000
    mock_raw_file_wrapper_factory.create_copy_wrapper.return_value.get_files_to_copy.return_value = {
        Path("/path/to/instrument/test_file.raw"): Path("/path/to/backup/test_file.raw")
    }

    # when
    copy_raw_file(ti, **kwargs)

    # then
    mock_copy_file.assert_called_once_with(
        Path("/path/to/instrument/test_file.raw"), Path("/path/to/backup/test_file.raw")
    )
    mock_update_status.assert_has_calls(
        [
            call("test_file.raw", new_status=RawFileStatus.COPYING),
            call("test_file.raw", new_status=RawFileStatus.COPYING_DONE, size=1000),
        ]
    )


@patch("dags.impl.handler_impl.trigger_dag_run")
@patch("dags.impl.handler_impl.update_raw_file")
def test_start_acquisition_processor_with_single_file(
    mock_update_raw_file: MagicMock,
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
            DagContext.PARAMS: {DagParams.RAW_FILE_ID: "file1.raw"},
        },
    )

    # then
    assert mock_trigger_dag_run.call_count == 1  # no magic numbers
    for n, call_ in enumerate(mock_trigger_dag_run.call_args_list):
        assert call_.args[0] == ("acquisition_processor.instrument1")
        assert {
            "raw_file_id": list(raw_file_names.keys())[n],
        } == call_.args[1]
    mock_update_raw_file.assert_called_once_with(
        "file1.raw", new_status=RawFileStatus.QUEUED_FOR_QUANTING
    )
