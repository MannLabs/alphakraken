"""Unit tests for monitor_impl.py."""

from unittest.mock import MagicMock, Mock, patch

from common.keys import DagContext, DagParams, OpArgs
from dags.impl.monitor_impl import start_acquisition_handler


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
