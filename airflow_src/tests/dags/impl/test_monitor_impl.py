"""Unit tests for monitor_impl.py."""

from unittest.mock import MagicMock, Mock, patch

from common.keys import DagContext, DagParams, OpArgs
from dags.impl.monitor_impl import (
    copy_raw_file,
    start_acquisition_handler,
    update_raw_file_in_db,
)
from db.models import RawFileStatus


@patch("dags.impl.monitor_impl.update_raw_file_status")
def test_update_raw_file_in_db_calls_update_with_correct_args(
    mock_update_status: MagicMock,
) -> None:
    """Test update_raw_file_in_db calls update with correct arguments."""
    ti = MagicMock()
    kwargs = {"params": {"raw_file_name": "test_file.raw"}}

    update_raw_file_in_db(ti, **kwargs)

    mock_update_status.assert_called_once_with(
        "test_file.raw", new_status=RawFileStatus.ACQUISITION_STARTED
    )


@patch("dags.impl.monitor_impl._get_file_size")
@patch("dags.impl.monitor_impl.update_raw_file_status")
def test_copy_raw_file_calls_update_with_correct_args(
    mock_update_status: MagicMock, mock_get_file_size: MagicMock
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
    mock_update_status.assert_called_once_with(
        "test_file.raw", new_status=RawFileStatus.COPYING_FINISHED, size=1000
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
