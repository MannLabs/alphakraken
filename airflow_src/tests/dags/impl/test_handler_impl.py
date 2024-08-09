"""Tests for the handler_impl module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from dags.impl.handler_impl import add_to_db, run_quanting
from plugins.common.keys import DagContext, DagParams, OpArgs, XComKeys


@patch("dags.impl.handler_impl.get_instrument_data_path")
@patch("os.stat")
@patch("dags.impl.handler_impl.add_new_raw_file_to_db")
@patch("dags.impl.handler_impl.put_xcom")
def test_add_to_db(
    mock_put_xcom: MagicMock,
    mock_add_new_raw_file_to_db: MagicMock,
    mock_stat: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test add_to_db makes the expected calls."""
    # Given
    ti = Mock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "test_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }
    mock_get_instrument_data_path.return_value = Path("/path/to/data")
    mock_stat.return_value.st_size = 42.0
    mock_stat.return_value.st_ctime = 43.0

    # When
    add_to_db(ti, **kwargs)

    # Then
    mock_get_instrument_data_path.assert_called_once_with("instrument1")
    mock_add_new_raw_file_to_db.assert_called_once_with(
        "test_file.raw", instrument_id="instrument1", size=42.0, creation_ts=43.0
    )
    mock_put_xcom.assert_called_once_with(ti, XComKeys.RAW_FILE_NAME, "test_file.raw")


@patch("dags.impl.handler_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.handler_impl.put_xcom")
def test_run_quanting_executes_ssh_command_and_stores_job_id(
    mock_put_xcom: MagicMock,
    mock_ssh_execute: MagicMock,
) -> None:
    """Test that the run_quanting function executes the SSH command and stores the job ID."""
    # given
    mock_ssh_execute.return_value = "12345"
    ti = MagicMock()
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
        OpArgs.COMMAND: "some_command",
    }

    # when
    run_quanting(ti, **kwargs)

    # then
    mock_ssh_execute.assert_called_once_with("some_command", mock_ssh_hook)
    mock_put_xcom.assert_called_once_with(ti, XComKeys.JOB_ID, "12345")
