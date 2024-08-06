"""Tests for the processor_impl module."""

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pytest
import pytz
from airflow.exceptions import AirflowFailException
from common.settings import INSTRUMENTS
from dags.impl.processor_impl import (
    _get_project_id_or_fallback,
    check_quanting_result,
    compute_metrics,
    get_business_errors,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from plugins.common.keys import (
    DagContext,
    DagParams,
    JobStates,
    OpArgs,
    QuantingEnv,
    XComKeys,
)

from shared.db.models import RawFile, RawFileStatus


def test_get_project_id_for_raw_file() -> None:
    """Test that _get_project_id_for_raw_file returns correct project id."""
    # when
    project_id = _get_project_id_or_fallback("PID1", "some_instrument_id")

    assert project_id == "PID1"


@patch.dict(INSTRUMENTS, {"instrument1": {"type": "some_type"}})
def test_get_project_id_for_raw_file_fallback() -> None:
    """Test that _get_project_id_for_raw_file returns correct project id for non-bruker."""
    # when
    project_id = _get_project_id_or_fallback(None, "instrument1")

    assert project_id == "_FALLBACK"


@patch.dict(INSTRUMENTS, {"instrument1": {"type": "bruker"}})
def test_get_project_id_for_raw_file_fallback_bruker() -> None:
    """Test that _get_project_id_for_raw_file returns correct project id for bruker."""
    # when
    project_id = _get_project_id_or_fallback(None, "instrument1")

    assert project_id == "_FALLBACK_BRUKER"


@patch.dict(INSTRUMENTS, {"instrument1": {}})
@patch.dict(
    os.environ,
    {
        "POOL_BASE_PATH": "/pool/path/to",
        "QUANTING_POOL_FOLDER": "some_quanting_pool_folder",
        "BACKUP_POOL_FOLDER": "some_backup_pool_folder",
    },
)
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.random")
@patch("dags.impl.processor_impl._get_project_id_or_fallback")
@patch("dags.impl.processor_impl.get_settings_for_project")
def test_prepare_quanting(
    mock_get_settings: MagicMock,
    mock_get_project_id_for_raw_file: MagicMock,
    mock_random: MagicMock,
    mock_put_xcom: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting makes the expected calls."""
    mock_random.return_value = 0.44

    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_project_id_for_raw_file.return_value = "some_project_id"
    mock_get_settings.return_value = MagicMock(
        speclib_file_name="some_speclib_file_name",
        fasta_file_name="some_fasta_file_name",
        config_file_name="some_config_file_name",
        software="some_software",
    )
    ti = MagicMock()

    kwargs = {
        OpArgs.INSTRUMENT_ID: "instrument1",
        DagContext.PARAMS: {
            DagParams.RAW_FILE_ID: "test_file.raw",
        },
    }

    # when
    prepare_quanting(ti, **kwargs)

    mock_get_project_id_for_raw_file.assert_called_once_with(
        "some_project_id", "instrument1"
    )
    mock_get_settings.assert_called_once_with("some_project_id")

    # when you adapt something here, don't forget to adapt also the submit_job.sh script
    expected_quanting_env = {
        "RAW_FILE_PATH": "/pool/path/to/some_backup_pool_folder/instrument1/1970_01/test_file.raw",
        "SETTINGS_PATH": "/pool/path/to/some_quanting_pool_folder/settings/some_project_id",
        "OUTPUT_PATH": "/pool/path/to/some_quanting_pool_folder/output/some_project_id/out_test_file.raw",
        "SPECLIB_FILE_NAME": "4_some_speclib_file_name",
        "FASTA_FILE_NAME": "some_fasta_file_name",
        "CONFIG_FILE_NAME": "some_config_file_name",
        "SOFTWARE": "some_software",
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID_OR_FALLBACK": "some_project_id",
    }

    mock_put_xcom.assert_has_calls(
        [
            call(ti, "quanting_env", expected_quanting_env),
            call(ti, "raw_file_id", "test_file.raw"),
        ]
    )
    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_random.assert_called_once()  # TODO: remove patching random once the hack is removed


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.update_raw_file")
def test_run_quanting_executes_ssh_command_and_stores_job_id(
    mock_update: MagicMock,
    mock_put_xcom: MagicMock,
    mock_ssh_execute: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that the run_quanting function executes the SSH command and stores the job ID."""
    # given
    mock_get_xcom.side_effect = [
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
            # rest of quanting_env is left out here for brevity
        },
        -1,
    ]
    mock_ssh_execute.return_value = "12345"
    mock_raw_file = MagicMock(
        wraps=RawFile, created_at=datetime.fromtimestamp(0, tz=pytz.UTC)
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    ti = MagicMock()
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
    }

    # when
    run_quanting(ti, **kwargs)

    # then
    expected_export_command = (
        "export RAW_FILE_ID=test_file.raw\n" "export PROJECT_ID_OR_FALLBACK=PID123\n"
        # rest of quanting_env is left out here for brevity
    )

    expected_command = expected_export_command + (
        "mkdir -p ~/slurm/jobs/1970_01\n"
        "cd ~/slurm/jobs/1970_01\n"
        "cat ~/slurm/submit_job.sh\n"
        "JID=$(sbatch ~/slurm/submit_job.sh)\n"
        "echo ${JID##* }\n"
    )
    mock_ssh_execute.assert_called_once_with(expected_command, mock_ssh_hook)
    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_put_xcom.assert_called_once_with(ti, XComKeys.JOB_ID, "12345")
    mock_update.assert_called_once_with(
        "test_file.raw", new_status=RawFileStatus.QUANTING
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
def test_run_quanting_job_id_exists(
    mock_ssh_execute: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """run_quanting function skips execution if the job ID already exists."""
    # given
    mock_get_xcom.side_effect = [
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
            # rest of quanting_env is left out here for brevity
        },
        12345,
    ]
    ti = MagicMock()
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
    }

    # when
    run_quanting(ti, **kwargs)

    # then
    mock_ssh_execute.assert_not_called()


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.Path")
@patch("dags.impl.processor_impl.get_airflow_variable")
def test_run_quanting_output_folder_exists(
    mock_get_airflow_variable: MagicMock,
    mock_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """run_quanting function raises an exception if the output path already exists."""
    # given
    mock_get_xcom.side_effect = [
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
            # rest of quanting_env is left out here for brevity
        },
        -1,
    ]
    mock_path.return_value.exists.return_value = True
    mock_get_airflow_variable.return_value = "False"

    ti = MagicMock()
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
    }

    # when
    with pytest.raises(AirflowFailException):
        run_quanting(ti, **kwargs)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_airflow_variable.assert_called_once_with("allow_output_overwrite", "False")


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
def test_run_quanting_executes_ssh_command_error_wrong_job_id(
    mock_ssh_execute: MagicMock,
    mock_get_raw_file_by_id: MagicMock,  # noqa: ARG001
    mock_get_xcom: MagicMock,
) -> None:
    """run_quanting function raises an exception if the job ID is not an integer."""
    # given
    mock_get_xcom.side_effect = [
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
        },
        -1,
    ]
    mock_ssh_execute.return_value = "some_wrong_job_id"
    mock_ssh_hook = MagicMock()

    kwargs = {
        OpArgs.SSH_HOOK: mock_ssh_hook,
    }

    # when
    with pytest.raises(AirflowFailException):
        run_quanting(MagicMock(), **kwargs)


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.processor_impl.put_xcom")
def test_check_quanting_result_happy_path(
    mock_put_xcom: MagicMock, mock_ssh_execute: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that check_quanting_result makes the expected calls."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = "12345"
    mock_ssh_execute.return_value = (
        f"00:08:42\nsome\nother\nlines\n{JobStates.COMPLETED}"
    )

    mock_ssh_hook = MagicMock()

    # when
    continue_downstream_tasks = check_quanting_result(
        mock_ti, **{OpArgs.SSH_HOOK: mock_ssh_hook}
    )

    assert continue_downstream_tasks
    mock_ssh_execute.assert_called_once_with(
        "TIME_ELAPSED=$(sacct --format=Elapsed -j 12345 | tail -n 1); echo $TIME_ELAPSED\nsacct -l -j 12345\n"
        "cat ~/slurm/jobs/*/slurm-12345.out\n\nST=$(sacct -j 12345 -o State | awk 'FNR == 3 {print $1}')\necho $ST\n",
        mock_ssh_hook,
    )

    mock_put_xcom.assert_called_once_with(mock_ti, XComKeys.QUANTING_TIME_ELAPSED, 522)


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
def test_check_quanting_result_unknown_job_status(
    mock_ssh_execute: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that check_quanting_result raises on unknown quanting job status."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = "12345"
    mock_ssh_execute.return_value = "00:08:42\nsome\nother\nlines\nSOME_JOB_STATE"

    mock_ssh_hook = MagicMock()

    # when
    with pytest.raises(AirflowFailException):
        check_quanting_result(mock_ti, **{OpArgs.SSH_HOOK: mock_ssh_hook})


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.update_raw_file")
def test_check_quanting_result_business_error(
    mock_update_raw_file: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_ssh_execute: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly on business errors."""
    mock_ti = MagicMock()
    mock_get_xcom.side_effect = [
        "12345",
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        },
    ]
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_ssh_execute.return_value = "00:08:42\nsome\nother\nlines\nFAILED"
    mock_get_business_errors.return_value = ["error1", "error2"]

    mock_ssh_hook = MagicMock()

    # when
    continue_downstream_tasks = check_quanting_result(
        mock_ti, **{OpArgs.SSH_HOOK: mock_ssh_hook}
    )
    assert not continue_downstream_tasks

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(mock_raw_file, "PID1")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="error1;error2",
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.update_raw_file")
def test_check_quanting_result_business_error_raises(
    mock_update_raw_file: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_ssh_execute: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly if business error is unknown."""
    mock_ti = MagicMock()
    mock_get_xcom.side_effect = [
        "12345",
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        },
    ]
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_ssh_execute.return_value = "00:08:42\nsome\nother\nlines\nFAILED"
    mock_get_business_errors.return_value = ["error1", "__UNKNOWN_ERROR"]

    mock_ssh_hook = MagicMock()

    # when
    with pytest.raises(AirflowFailException):
        check_quanting_result(mock_ti, **{OpArgs.SSH_HOOK: mock_ssh_hook})

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(mock_raw_file, "PID1")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="error1;__UNKNOWN_ERROR",
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.SSHSensorOperator.ssh_execute")
@patch("dags.impl.processor_impl.update_raw_file")
def test_check_quanting_result_timeout(
    mock_update_raw_file: MagicMock,
    mock_ssh_execute: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly on timeout."""
    mock_ti = MagicMock()
    mock_get_xcom.side_effect = [
        "12345",
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        },
    ]
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_ssh_execute.return_value = "00:08:42\nsome\nother\nlines\nTIMEOUT"

    mock_ssh_hook = MagicMock()

    # when
    continue_downstream_tasks = check_quanting_result(
        mock_ti, **{OpArgs.SSH_HOOK: mock_ssh_hook}
    )
    assert not continue_downstream_tasks

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="TIMEOUT",
    )


@patch("dags.impl.processor_impl.get_internal_output_path")
def test_get_business_errors_with_valid_errors(mock_path: MagicMock) -> None:
    """Test that get_business_errors returns the expected business errors."""
    mock_content = [
        '{"name": "exception", "error_code": "ERROR1"}',
        '{"name": "exception", "error_code": "ERROR2"}',
        '{"name": "other", "error_code": "ERROR3"}',
    ]
    mock_open_file = mock_open(read_data="\n".join(mock_content))
    mock_path.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value.open.return_value = mock_open_file()

    mock_raw_file = MagicMock()

    # when
    result = get_business_errors(mock_raw_file, "project_id")

    assert result == ["ERROR1", "ERROR2"]
    mock_path.assert_called_once_with(mock_raw_file, "project_id")


@patch("dags.impl.processor_impl.get_internal_output_path")
def test_get_business_errors_with_no_errors(mock_path: MagicMock) -> None:
    """Test that get_business_errors returns an empty list when there are no (valid) errors."""
    mock_content = [
        '{"name": "other", "error_code": "ERROR3"}',  # not an exception
        '{"name": "exception", "error_code": ""}',  # error code empty
        "invalid json",  # not a json
        '{"name": "exception"}',  # no error code
    ]
    mock_open_file = mock_open(read_data="\n".join(mock_content))
    mock_path.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value.open.return_value = mock_open_file()

    raw_file = MagicMock()

    # when
    result = get_business_errors(raw_file, "project_id")

    assert result == ["__COULD_NOT_DETERMINE_ERROR"]


@patch("dags.impl.processor_impl.get_internal_output_path")
def test_get_business_errors_file_not_found(mock_path: MagicMock) -> None:
    """Test that get_business_errors returns an empty list when the file is not found."""
    mock_path.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value.open.side_effect = FileNotFoundError

    raw_file = MagicMock()

    # when
    result = get_business_errors(raw_file, "project_id")

    assert result == ["__COULD_NOT_DETERMINE_ERROR"]


@patch("dags.impl.processor_impl.get_internal_output_path")
def test_get_business_errors_with_unknown_error(mock_path: MagicMock) -> None:
    """Test that get_business_errors returns an empty list when there are no (valid) errors."""
    mock_events_content = [
        '{"name": "other", "error_code": "ERROR3"}',  # not an exception
    ]
    mock_open_events_file = mock_open(read_data="\n".join(mock_events_content))
    mock_path.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value.open.return_value = mock_open_events_file()

    mock_log_content = [
        "ERROR: bla"  # -> unknown error
    ]
    mock_open_log_file = mock_open(read_data="\n".join(mock_log_content))
    mock_path.return_value.__truediv__.return_value.open.return_value = (
        mock_open_log_file()
    )

    raw_file = MagicMock()

    # when
    result = get_business_errors(raw_file, "project_id")

    assert result == ["__UNKNOWN_ERROR"]


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.calc_metrics")
@patch("dags.impl.processor_impl.put_xcom")
def test_compute_metrics(
    mock_put_xcom: MagicMock,
    mock_calc_metrics: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that compute_metrics makes the expected calls."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = {
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID_OR_FALLBACK": "P1",
    }
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_calc_metrics.return_value = {"metric1": "value1"}

    # when
    compute_metrics(mock_ti)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_calc_metrics.assert_called_once_with(
        Path("/opt/airflow/mounts/output/P1/out_test_file.raw")
    )
    mock_put_xcom.assert_called_once_with(
        mock_ti, XComKeys.METRICS, {"metric1": "value1"}
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
@patch("dags.impl.processor_impl.update_raw_file")
def test_upload_metrics(
    mock_update: MagicMock,
    mock_add: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that compute_metrics makes the expected calls."""
    mock_get_xcom.side_effect = ["some_file.raw", {"metric1": "value1"}, 123]

    # when
    upload_metrics(MagicMock())

    mock_add.assert_called_once_with(
        "some_file.raw", {"metric1": "value1", "quanting_time_elapsed": 123}
    )
    mock_update.assert_called_once_with("some_file.raw", new_status=RawFileStatus.DONE)
