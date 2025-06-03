"""Tests for the processor_impl module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pytest
import pytz
from airflow.exceptions import AirflowFailException
from common.settings import _INSTRUMENTS
from dags.impl.processor_impl import (
    _get_project_id_or_fallback,
    _get_slurm_job_id_from_log,
    check_quanting_result,
    compute_metrics,
    get_business_errors,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from mongoengine import DoesNotExist
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


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "some_type"}})
def test_get_project_id_for_raw_file_fallback() -> None:
    """Test that _get_project_id_for_raw_file returns correct project id for non-bruker."""
    # when
    project_id = _get_project_id_or_fallback(None, "instrument1")

    assert project_id == "_FALLBACK"


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "bruker"}})
def test_get_project_id_for_raw_file_fallback_bruker() -> None:
    """Test that _get_project_id_for_raw_file returns correct project id for bruker."""
    # when
    project_id = _get_project_id_or_fallback(None, "instrument1")

    assert project_id == "_FALLBACK_BRUKER"


@patch.dict(_INSTRUMENTS, {"instrument1": {}})
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl._get_project_id_or_fallback")
@patch("dags.impl.processor_impl.get_settings_for_project")
def test_prepare_quanting(
    mock_get_settings: MagicMock,
    mock_get_project_id_for_raw_file: MagicMock,
    mock_put_xcom: MagicMock,
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting makes the expected calls."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.side_effect = [
        Path("/some_backup_base_path"),
        Path("/some_quanting_settings_path"),
        Path("/some_quanting_output_path"),
    ]
    mock_get_project_id_for_raw_file.return_value = "some_project_id"
    mock_get_settings.return_value = MagicMock(
        speclib_file_name="some_speclib_file_name",
        fasta_file_name="some_fasta_file_name",
        config_file_name="some_config_file_name",
        software="some_software",
        version=1,
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
        "RAW_FILE_PATH": "/some_backup_base_path/instrument1/1970_01/test_file.raw",
        "SETTINGS_PATH": "/some_quanting_settings_path/some_project_id",
        "OUTPUT_PATH": "/some_quanting_output_path/some_project_id/out_test_file.raw",
        "SPECLIB_FILE_NAME": "some_speclib_file_name",
        "FASTA_FILE_NAME": "some_fasta_file_name",
        "CONFIG_FILE_NAME": "some_config_file_name",
        "SOFTWARE": "some_software",
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID_OR_FALLBACK": "some_project_id",
        "SETTINGS_VERSION": 1,
    }

    mock_put_xcom.assert_has_calls(
        [
            call(ti, "quanting_env", expected_quanting_env),
            call(ti, "raw_file_id", "test_file.raw"),
        ]
    )
    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_path.assert_has_calls([call("backup"), call("settings"), call("output")])


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl._get_project_id_or_fallback")
@patch("dags.impl.processor_impl.get_settings_for_project")
def test_prepare_quanting_no_project_raise(
    mock_get_settings: MagicMock,
    mock_get_project_id_for_raw_file: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting raises an exception if no project is found."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_project_id_for_raw_file.return_value = "some_project_id"

    mock_get_settings.side_effect = DoesNotExist

    kwargs = {
        OpArgs.INSTRUMENT_ID: "instrument1",
        DagContext.PARAMS: {
            DagParams.RAW_FILE_ID: "test_file.raw",
        },
    }

    # when
    with pytest.raises(AirflowFailException):
        prepare_quanting(MagicMock(), **kwargs)


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl._get_project_id_or_fallback")
@patch("dags.impl.processor_impl.get_settings_for_project")
def test_prepare_quanting_no_settings_raise(
    mock_get_settings: MagicMock,
    mock_get_project_id_for_raw_file: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting raises an exception if no settings are found."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_project_id_for_raw_file.return_value = "some_project_id"

    mock_get_settings.return_value = None

    kwargs = {
        OpArgs.INSTRUMENT_ID: "instrument1",
        DagContext.PARAMS: {
            DagParams.RAW_FILE_ID: "test_file.raw",
        },
    }

    # when
    with pytest.raises(AirflowFailException):
        prepare_quanting(MagicMock(), **kwargs)


def test_get_slurm_job_id_from_log_returns_slurm_job_id_if_present_in_log() -> None:
    """Test that _get_slurm_job_id_from_log returns the job ID if it is present in the log."""
    log_content = "Some log content\nslurm_job_id: 12345\nMore log content"

    with (
        patch("pathlib.Path.open", mock_open(read_data=log_content)),
        patch("pathlib.Path.exists", return_value=True),
    ):
        assert _get_slurm_job_id_from_log(Path("/mock/path")) == "12345"


def test_get_slurm_job_id_from_log_returns_none_if_slurm_job_id_not_present_in_log() -> (
    None
):
    """Test that _get_slurm_job_id_from_log returns None if the job ID is not present in the log."""
    log_content = "Some log content\nNo job id here\nMore log content"
    with patch("pathlib.Path.open", mock_open(read_data=log_content)):
        # when
        assert _get_slurm_job_id_from_log(Path("/mock/path")) is None


def test_get_slurm_job_id_from_log_returns_none_if_file_not_exists() -> None:
    """Test that _get_slurm_job_id_from_log returns None if the job ID is not present in the log."""
    with patch("pathlib.Path.open") as mock_path:
        mock_path.exists.return_value = False
        # when
        assert _get_slurm_job_id_from_log(Path("/mock/path")) is None


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.start_job")
@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.update_raw_file")
def test_run_quanting_executes_ssh_command_and_stores_job_id(
    mock_update: MagicMock,
    mock_put_xcom: MagicMock,
    mock_start_job: MagicMock,
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
    mock_start_job.return_value = "12345"
    mock_raw_file = MagicMock(
        wraps=RawFile, created_at=datetime.fromtimestamp(0, tz=pytz.UTC)
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    ti = MagicMock()

    # when
    run_quanting(ti)

    mock_start_job.assert_called_once_with(
        "submit_job.sh",
        {"RAW_FILE_ID": "test_file.raw", "PROJECT_ID_OR_FALLBACK": "PID123"},
        "1970_01",
    )
    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_put_xcom.assert_called_once_with(ti, XComKeys.JOB_ID, "12345")
    mock_update.assert_called_once_with(
        "test_file.raw", new_status=RawFileStatus.QUANTING
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.start_job")
def test_run_quanting_job_id_exists(
    mock_start_job: MagicMock,
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

    # when
    run_quanting(ti)

    # then
    mock_start_job.assert_not_called()


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
    mock_get_airflow_variable.return_value = "raise"

    ti = MagicMock()

    # when
    with pytest.raises(AirflowFailException):
        run_quanting(ti)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_airflow_variable.assert_called_once_with("output_exists_mode", "raise")


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.Path")
@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl._get_slurm_job_id_from_log")
@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.update_raw_file")
def test_run_quanting_output_folder_exists_associate(  # noqa: PLR0913
    mock_update: MagicMock,
    mock_put_xcom: MagicMock,
    mock_get_slurm_job_id_from_log: MagicMock,
    mock_get_airflow_variable: MagicMock,
    mock_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,  # noqa: ARG001
    mock_get_xcom: MagicMock,
) -> None:
    """run_quanting function correctly fills xcom if the output path already exists and mode is 'associate'."""
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
    mock_get_airflow_variable.return_value = "associate"
    mock_get_slurm_job_id_from_log.return_value = "54321"
    ti = MagicMock()

    # when
    run_quanting(ti)

    mock_put_xcom.assert_called_once_with(ti, XComKeys.JOB_ID, "54321")
    mock_update.assert_not_called()


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.Path")
@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl._get_slurm_job_id_from_log")
@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.update_raw_file")
def test_run_quanting_output_folder_exists_associate_raise(  # noqa: PLR0913
    mock_update: MagicMock,
    mock_put_xcom: MagicMock,
    mock_get_slurm_job_id_from_log: MagicMock,
    mock_get_airflow_variable: MagicMock,
    mock_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,  # noqa: ARG001
    mock_get_xcom: MagicMock,
) -> None:
    """run_quanting function correctly raises if the output path already exists and mode is 'associate' and no job id."""
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
    mock_get_airflow_variable.return_value = "associate"
    mock_get_slurm_job_id_from_log.return_value = None
    ti = MagicMock()

    # when
    with pytest.raises(AirflowFailException):
        run_quanting(ti)

    mock_put_xcom.assert_not_called()
    mock_update.assert_not_called()


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.put_xcom")
def test_check_quanting_result_happy_path(
    mock_put_xcom: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that check_quanting_result makes the expected calls."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = "12345"

    mock_get_job_result.return_value = (JobStates.COMPLETED, 522)

    # when
    continue_downstream_tasks = check_quanting_result(mock_ti)

    assert continue_downstream_tasks

    mock_put_xcom.assert_called_once_with(mock_ti, XComKeys.QUANTING_TIME_ELAPSED, 522)


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_job_result")
def test_check_quanting_result_unknown_job_status(
    mock_get_job_result: MagicMock, mock_get_xcom: MagicMock
) -> None:
    """Test that check_quanting_result raises on unknown quanting job status."""
    mock_ti = MagicMock()
    mock_get_xcom.return_value = "12345"
    mock_get_job_result.return_value = ("SOME_JOB_STATE", 522)

    # when
    with pytest.raises(AirflowFailException):
        check_quanting_result(mock_ti)


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_business_error(  # noqa: PLR0913
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_get_job_result: MagicMock,
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
            QuantingEnv.SETTINGS_VERSION: 1,
        },
    ]
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = ("FAILED", 522)
    mock_get_business_errors.return_value = ["error1", "error2"]

    # when
    continue_downstream_tasks = check_quanting_result(mock_ti)
    assert not continue_downstream_tasks

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(mock_raw_file, "PID1")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="error1;error2",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_version=1,
        metrics_type="alphadia",
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_business_error_raises(  # noqa: PLR0913
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_get_job_result: MagicMock,
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
            QuantingEnv.SETTINGS_VERSION: 1,
        },
    ]
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "FAILED", 522
    mock_get_business_errors.return_value = ["error1", "__UNKNOWN_ERROR"]

    # when
    with pytest.raises(AirflowFailException):
        check_quanting_result(mock_ti)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(mock_raw_file, "PID1")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="error1;__UNKNOWN_ERROR",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_version=1,
        metrics_type="alphadia",
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_timeout(
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_job_result: MagicMock,
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
            QuantingEnv.SETTINGS_VERSION: 1,
        },
    ]
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "TIMEOUT", 522

    # when
    continue_downstream_tasks = check_quanting_result(mock_ti)
    assert not continue_downstream_tasks

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="TIMEOUT",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_version=1,
        metrics_type="alphadia",
    )


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_oom(
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly on out of memory."""
    mock_ti = MagicMock()
    mock_get_xcom.side_effect = [
        "12345",
        {
            QuantingEnv.RAW_FILE_ID: "test_file.raw",
            QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
            QuantingEnv.SETTINGS_VERSION: 1,
        },
    ]
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "OUT_OF_ME+", 522

    # when
    continue_downstream_tasks = check_quanting_result(mock_ti)
    assert not continue_downstream_tasks

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="OUT_OF_MEMORY",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_version=1,
        metrics_type="alphadia",
    )


@patch("dags.impl.processor_impl.get_internal_output_path_for_raw_file")
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


@patch("dags.impl.processor_impl.get_internal_output_path_for_raw_file")
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


@patch("dags.impl.processor_impl.get_internal_output_path_for_raw_file")
def test_get_business_errors_file_not_found(mock_path: MagicMock) -> None:
    """Test that get_business_errors returns an empty list when the file is not found."""
    mock_path.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value.open.side_effect = FileNotFoundError

    raw_file = MagicMock()

    # when
    result = get_business_errors(raw_file, "project_id")

    assert result == ["__COULD_NOT_DETERMINE_ERROR"]


@patch("dags.impl.processor_impl.get_internal_output_path_for_raw_file")
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

    mock_get_xcom.side_effect = [
        {
            "RAW_FILE_ID": "test_file.raw",
            "PROJECT_ID_OR_FALLBACK": "P1",
        },
        123,
    ]
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
        Path("/opt/airflow/mounts/output/P1/out_test_file.raw"), metrics_type="alphadia"
    )
    assert mock_put_xcom.call_count == 2  # noqa: PLR2004
    mock_put_xcom.assert_any_call(
        mock_ti, XComKeys.METRICS, {"metric1": "value1", "quanting_time_elapsed": 123}
    )
    mock_put_xcom.assert_any_call(mock_ti, XComKeys.METRICS_TYPE, "alphadia")


@patch("dags.impl.processor_impl.get_xcom")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
@patch("dags.impl.processor_impl.update_raw_file")
def test_upload_metrics(
    mock_update: MagicMock,
    mock_add: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test that upload_metrics makes the expected calls."""
    mock_get_xcom.side_effect = [
        "some_file.raw",  # RAW_FILE_ID
        {"SETTINGS_VERSION": 1},  # QUANTING_ENV
        {
            "metric1": "value1"
        },  # METRICS (already includes quanting_time_elapsed from compute_metrics)
        "alphadia",  # METRICS_TYPE
    ]

    # when
    upload_metrics(MagicMock())

    mock_add.assert_called_once_with(
        "some_file.raw",
        metrics_type="alphadia",
        metrics={
            "metric1": "value1",
        },
        settings_version=1,
    )
    mock_update.assert_called_once_with(
        "some_file.raw", new_status=RawFileStatus.DONE, status_details=None
    )
