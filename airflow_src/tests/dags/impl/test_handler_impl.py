"""Tests for the handler_impl module."""

import os
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.settings import INSTRUMENTS
from dags.impl.handler_impl import (
    _get_project_id_for_raw_file,
    compute_metrics,
    get_job_info,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from plugins.common.keys import DagContext, DagParams, OpArgs, QuantingEnv, XComKeys

from shared.db.models import RawFileStatus


@patch("dags.impl.handler_impl.get_all_project_ids")
@patch("dags.impl.handler_impl.get_unique_project_id")
def test_get_project_id_for_raw_file(
    mock_get_unique_project_id: MagicMock,
    mock_get_all_project_ids: MagicMock,
) -> None:
    """Test that _get_project_id_for_raw_file makes the expected calls."""
    mock_get_all_project_ids.return_value = ["some_project_id", "P2"]
    mock_get_unique_project_id.return_value = "some_project_id"

    # when
    _get_project_id_for_raw_file("test_file.raw")

    mock_get_all_project_ids.assert_called_once_with()
    mock_get_unique_project_id.assert_called_once_with(
        "test_file.raw", ["some_project_id", "P2"]
    )


@patch.dict(INSTRUMENTS, {"instrument1": {"raw_data_path": "path/to/data"}})
@patch.dict(os.environ, {"IO_POOL_FOLDER": "some_io_pool_folder"})
@patch("dags.impl.handler_impl.put_xcom")
@patch("dags.impl.handler_impl.random")
@patch("dags.impl.handler_impl._get_project_id_for_raw_file")
@patch("dags.impl.handler_impl.get_settings_for_project")
def test_prepare_quanting(
    mock_get_settings: MagicMock,
    mock_get_project_id_for_raw_file: MagicMock,
    mock_random: MagicMock,
    mock_put_xcom: MagicMock,
) -> None:
    """Test that prepare_quanting makes the expected calls."""
    mock_random.return_value = 0.44
    mock_get_project_id_for_raw_file.return_value = "some_project_id"
    mock_get_settings.return_value = MagicMock(
        speclib_file_name="some_speclib_file_name",
        fasta_file_name="some_fasta_file_name",
        config_file_name="some_config_file_name",
        software="some_software",
    )
    ti = MagicMock()

    # when
    kwargs = {
        OpArgs.INSTRUMENT_ID: "instrument1",
        DagContext.PARAMS: {
            DagParams.RAW_FILE_NAME: "test_file.raw",
        },
    }

    # when
    prepare_quanting(ti, **kwargs)

    mock_get_project_id_for_raw_file.assert_called_once_with("test_file.raw")
    mock_get_settings.assert_called_once_with("some_project_id")

    expected_quanting_env = {
        "RAW_FILE_NAME": "test_file.raw",
        "INSTRUMENT_SUBFOLDER": "path/to/data",
        "OUTPUT_FOLDER_NAME": "out_test_file.raw",
        "SPECLIB_FILE_NAME": "4_some_speclib_file_name",
        "FASTA_FILE_NAME": "some_fasta_file_name",
        "CONFIG_FILE_NAME": "some_config_file_name",
        "SOFTWARE": "some_software",
        "PROJECT_ID": "some_project_id",
        "IO_POOL_FOLDER": "some_io_pool_folder",
    }

    mock_put_xcom.assert_has_calls(
        [
            call(ti, "quanting_env", expected_quanting_env),
            call(ti, "raw_file_name", "test_file.raw"),
        ]
    )
    mock_random.assert_called_once()  # TODO: remove patching random once the hack is removed


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
    mock_get_xcom.return_value = {
        QuantingEnv.RAW_FILE_NAME: "test_file.raw"
        # rest of quanting_env is left out here for brevity
    }
    mock_ssh_execute.return_value = "12345"
    ti = MagicMock()
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
    }

    # when
    run_quanting(ti, **kwargs)

    # then
    expected_export_command = (
        "export RAW_FILE_NAME=test_file.raw\n"
        # rest of quanting_env is left out here for brevity
    )

    expected_command = expected_export_command + (
        "cd ~/slurm/jobs\n"
        "cat ~/slurm/submit_job.sh\n"
        "JID=$(sbatch ~/slurm/submit_job.sh)\n"
        "echo ${JID##* }\n"
    )
    mock_ssh_execute.assert_called_once_with(expected_command, mock_ssh_hook)
    mock_put_xcom.assert_called_once_with(ti, XComKeys.JOB_ID, "12345")
    mock_update.assert_called_once_with("test_file.raw", RawFileStatus.PROCESSING)


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.SSHSensorOperator.ssh_execute")
def test_run_quanting_executes_ssh_command_error_wrong_job_id(
    mock_ssh_execute: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """run_quanting function raises an exception if the job ID is not an integer."""
    # given
    mock_get_xcom.return_value = {QuantingEnv.RAW_FILE_NAME: "test_file.raw"}
    mock_ssh_execute.return_value = "some_wrong_job_id"
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
    }

    # when
    with pytest.raises(AirflowFailException):
        run_quanting(MagicMock(), **kwargs)


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
