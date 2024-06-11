"""Tests for the handler_impl module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from dags.impl.handler_impl import (
    add_to_db,
    compute_metrics,
    run_quanting,
    upload_metrics,
)
from plugins.common.keys import DagContext, DagParams, OpArgs, XComKeys

from shared.db.engine import RawFileStatus


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


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.handler_impl.put_xcom")
@patch("dags.impl.handler_impl.update_raw_file_status")
def test_run_quanting_executes_ssh_command_and_stores_job_id(
    mock_update: MagicMock,
    mock_put_xcom: MagicMock,
    mock_ssh_execute: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that the run_quanting function executes the SSH command and stores the job ID."""
    # given
    mock_get_xcom.return_value = "test_file.raw"
    mock_ssh_execute.return_value = "12345"
    ti = MagicMock()
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
    }

    # when
    run_quanting(ti, **kwargs)

    # then
    expected_command = "export RAW_FILE_NAME=test_file.raw\n\ncd ~/kraken &&\nJID=$(sbatch run.sh)\necho ${JID##* }\n"
    mock_ssh_execute.assert_called_once_with(expected_command, mock_ssh_hook)
    mock_put_xcom.assert_called_once_with(ti, XComKeys.JOB_ID, "12345")
    mock_update.assert_called_once_with("test_file.raw", RawFileStatus.PROCESSING)


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.calc_metrics")
@patch("dags.impl.handler_impl.put_xcom")
def test_compute_metrics(
    mock_put_xcom: MagicMock,
    mock_calc_metrics: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that compute_metrics makes the expected calls."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = "raw_file_name"
    mock_calc_metrics.return_value = {"metric1": "value1"}

    # when
    compute_metrics(mock_ti)

    mock_get_xcom.assert_called_once_with(mock_ti, XComKeys.RAW_FILE_NAME)
    mock_calc_metrics.assert_called_once_with(
        "/opt/airflow/mounts/output/out_raw_file_name"
    )
    mock_put_xcom.assert_called_once_with(
        mock_ti, XComKeys.METRICS, {"metric1": "value1"}
    )


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.add_metrics_to_raw_file")
@patch("dags.impl.handler_impl.update_raw_file_status")
def test_upload_metrics(
    mock_update: MagicMock,
    mock_add: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that compute_metrics makes the expected calls."""
    mock_get_xcom.side_effect = ["raw_file_name", {"metric1": "value1"}]

    # when
    upload_metrics(MagicMock())

    mock_add.assert_called_once_with("raw_file_name", {"metric1": "value1"})
    mock_update.assert_called_once_with("raw_file_name", RawFileStatus.PROCESSED)
