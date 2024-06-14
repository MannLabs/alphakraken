"""Tests for the handler_impl module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from common.settings import INSTRUMENTS
from dags.impl.handler_impl import (
    add_to_db,
    compute_metrics,
    get_job_info,
    run_quanting,
    upload_metrics,
)
from plugins.common.keys import DagContext, DagParams, OpArgs, XComKeys

from shared.db.models import RawFileStatus


@patch("dags.impl.handler_impl.get_internal_instrument_data_path")
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
        "test_file.raw",
        status="new",
        instrument_id="instrument1",
        size=42.0,
        creation_ts=43.0,
    )
    mock_put_xcom.assert_called_once_with(ti, XComKeys.RAW_FILE_NAME, "test_file.raw")


@patch.dict(INSTRUMENTS, {"some_instrument_id": {"raw_data_path": "path/to/data"}})
@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.handler_impl.put_xcom")
@patch("dags.impl.handler_impl.update_raw_file_status")
@patch("dags.impl.handler_impl.random")
def test_run_quanting_executes_ssh_command_and_stores_job_id(
    mock_random: MagicMock,
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
        OpArgs.INSTRUMENT_ID: "some_instrument_id",
    }

    mock_random.return_value = 0.44

    # when
    run_quanting(ti, **kwargs)

    # then
    expected_command = (
        "export RAW_FILE_NAME=test_file.raw\n"
        "export POOL_BACKUP_INSTRUMENT_SUBFOLDER=path/to/data\n"
        "export OUTPUT_FOLDER_NAME=out_test_file.raw\n"
        "export SPECLIB_FILE_NAME=hela_hybrid.small.4.hdf\n\n"
        "cd ~/slurm/jobs &&\n"
        "JID=$(sbatch ~/slurm/submit_job.sh)\n"
        "echo ${JID##* }\n"
    )
    mock_ssh_execute.assert_called_once_with(expected_command, mock_ssh_hook)
    mock_put_xcom.assert_called_once_with(ti, XComKeys.JOB_ID, "12345")
    mock_update.assert_called_once_with("test_file.raw", RawFileStatus.PROCESSING)

    mock_random.assert_called_once()  # TODO: remove patching random once the hack is removed


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.handler_impl.put_xcom")
def test_get_job_info_happy_path(
    mock_put_xcom: MagicMock, mock_ssh_execute: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that get_job_info makes the expected calls."""
    mock_ti = MagicMock()
    mock_get_xcom.side_effect = ["ssh_hook", "job_id"]
    mock_ssh_execute.return_value = "00:08:42\nsome\nother\nlines\n"

    mock_ssh_hook = MagicMock()
    get_job_info(mock_ti, **{OpArgs.SSH_HOOK: mock_ssh_hook})

    mock_get_xcom.assert_called_once_with(mock_ti, XComKeys.JOB_ID)
    mock_ssh_execute.assert_called_once_with(
        "TIME_ELAPSED=$(sacct --format=Elapsed -j  ssh_hook | tail -n 1); echo $TIME_ELAPSED\nsacct -l -j ssh_hook\ncat ~/slurm/jobs/slurm-ssh_hook.out\n",
        mock_ssh_hook,
    )

    mock_put_xcom.assert_called_once_with(mock_ti, XComKeys.TIME_ELAPSED, 522)


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
        Path("/opt/airflow/mounts/output/out_raw_file_name")
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
    mock_get_xcom.side_effect = ["raw_file_name", {"metric1": "value1"}, 123]

    # when
    upload_metrics(MagicMock())

    mock_add.assert_called_once_with(
        "raw_file_name", {"metric1": "value1", "time_elapsed": 123}
    )
    mock_update.assert_called_once_with("raw_file_name", RawFileStatus.PROCESSED)
