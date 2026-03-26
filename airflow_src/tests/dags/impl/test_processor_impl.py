"""Tests for the processor_impl module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pytest
import pytz
from airflow.exceptions import AirflowFailException, AirflowSkipException
from common.settings import _INSTRUMENTS
from dags.impl.processor_impl import (
    _UPLOAD_METRICS_TASK_ID,
    _get_slurm_job_id_from_log,
    check_quanting_result,
    compute_metrics,
    finalize_raw_file_status,
    get_business_errors,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from mongoengine import DoesNotExist
from plugins.common.keys import (
    JobStates,
    QuantingEnv,
)

from shared.db.models import RawFile, RawFileStatus


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "thermo"}})
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
def test_prepare_quanting(
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting makes the expected calls."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.side_effect = [
        Path("/some_backup_base_path"),
        Path("/some_quanting_settings_path"),
        Path("/some_quanting_output_path"),
    ]
    mock_settings = MagicMock()
    mock_settings.name = "test_settings"
    mock_settings.speclib_file_name = "some_speclib_file_name"
    mock_settings.fasta_file_name = "some_fasta_file_name"
    mock_settings.config_file_name = "some_config_file_name"
    mock_settings.config_params = ""
    mock_settings.software = "some_software"
    mock_settings.software_type = "alphadia"
    mock_settings.metrics_type = "alphadia"
    mock_settings.version = 1
    mock_settings.slurm_cpus_per_task = 8
    mock_settings.slurm_mem = "62G"
    mock_settings.slurm_time = "02:00:00"
    mock_settings.num_threads = 8
    mock_get_settings.return_value = [MagicMock()]
    mock_resolve_scoped.return_value = [mock_settings]

    # when
    result = prepare_quanting(raw_file_id="test_file.raw")

    mock_get_settings.assert_called_once_with("some_project_id")
    mock_resolve_scoped.assert_called_once()

    # when you adapt something here, don't forget to adapt also the submit_job.sh script
    expected_quanting_env = {
        "RAW_FILE_PATH": "/some_backup_base_path/instrument1/1970_01/test_file.raw",
        "SETTINGS_PATH": "/some_quanting_settings_path/test_settings",
        "OUTPUT_PATH": "/some_quanting_output_path/some_project_id/out_test_file.raw/alphadia",
        "RELATIVE_OUTPUT_PATH": "some_project_id/out_test_file.raw/alphadia",
        "SPECLIB_FILE_NAME": "some_speclib_file_name",
        "FASTA_FILE_NAME": "some_fasta_file_name",
        "CONFIG_FILE_NAME": "some_config_file_name",
        "SOFTWARE": "some_software",
        "SOFTWARE_TYPE": "alphadia",
        "METRICS_TYPE": "alphadia",
        "CUSTOM_COMMAND": "",
        "_SLURM_CPUS_PER_TASK": 8,
        "_SLURM_MEM": "62G",
        "_SLURM_TIME": "02:00:00",
        "NUM_THREADS": 8,
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID_OR_FALLBACK": "some_project_id",
        "SETTINGS_NAME": "test_settings",
        "SETTINGS_VERSION": 1,
        "_INTERNAL_OUTPUT_PATH": "/opt/airflow/mounts/output/some_project_id/out_test_file.raw/alphadia",
    }

    assert result == [expected_quanting_env]
    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_path.assert_has_calls([call("backup"), call("settings"), call("output")])


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "thermo"}})
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
def test_prepare_quanting_custom_software(
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting handles custom software settings with parameter substitution."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.side_effect = [
        Path("/some_backup_base_path"),
        Path("/some_quanting_settings_path"),
        Path("/some_quanting_output_path"),
        Path("/some_software_base_path"),
    ]
    mock_settings = MagicMock()
    mock_settings.name = "test_custom_settings"
    mock_settings.speclib_file_name = "some_speclib_file_name"
    mock_settings.fasta_file_name = "some_fasta_file_name"
    mock_settings.config_file_name = ""
    mock_settings.config_params = "--qvalue 0.01 --f RAW_FILE_PATH --lib SETTINGS_PATH/some_speclib_file_name --out OUTPUT_PATH --fasta SETTINGS_PATH/some_fasta_file_name --threads NUM_THREADS --some_param RELATIVE_RAW_FILE_PATH --some_param2 RELATIVE_OUTPUT_PATH"
    mock_settings.software = "custom1.2.3"
    mock_settings.software_type = "custom"
    mock_settings.metrics_type = "custom"
    mock_settings.version = 1
    mock_settings.slurm_cpus_per_task = 8
    mock_settings.slurm_mem = "62G"
    mock_settings.slurm_time = "02:00:00"
    mock_settings.num_threads = 8
    mock_get_settings.return_value = [MagicMock()]
    mock_resolve_scoped.return_value = [mock_settings]

    # when
    result = prepare_quanting(raw_file_id="test_file.raw")

    expected_custom_command = (
        "/some_software_base_path/custom1.2.3 --qvalue 0.01 --f /some_backup_base_path/instrument1/1970_01/test_file.raw "
        "--lib /some_quanting_settings_path/test_custom_settings/some_speclib_file_name "
        "--out /some_quanting_output_path/some_project_id/out_test_file.raw/custom "
        "--fasta /some_quanting_settings_path/test_custom_settings/some_fasta_file_name --threads 8 "
        "--some_param instrument1/1970_01/test_file.raw --some_param2 some_project_id/out_test_file.raw/custom"
    )

    expected_quanting_env = {
        "RAW_FILE_PATH": "/some_backup_base_path/instrument1/1970_01/test_file.raw",
        "SETTINGS_PATH": "/some_quanting_settings_path/test_custom_settings",
        "OUTPUT_PATH": "/some_quanting_output_path/some_project_id/out_test_file.raw/custom",
        "RELATIVE_OUTPUT_PATH": "some_project_id/out_test_file.raw/custom",
        "SPECLIB_FILE_NAME": "some_speclib_file_name",
        "FASTA_FILE_NAME": "some_fasta_file_name",
        "CONFIG_FILE_NAME": "",
        "SOFTWARE": "custom1.2.3",
        "SOFTWARE_TYPE": "custom",
        "METRICS_TYPE": "custom",
        "CUSTOM_COMMAND": expected_custom_command,
        "_SLURM_CPUS_PER_TASK": 8,
        "_SLURM_MEM": "62G",
        "_SLURM_TIME": "02:00:00",
        "NUM_THREADS": 8,
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID_OR_FALLBACK": "some_project_id",
        "SETTINGS_NAME": "test_custom_settings",
        "SETTINGS_VERSION": 1,
        "_INTERNAL_OUTPUT_PATH": "/opt/airflow/mounts/output/some_project_id/out_test_file.raw/custom",
    }

    assert result == [expected_quanting_env]


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "thermo"}})
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
def test_prepare_quanting_multiple_settings(
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting returns one quanting_env per assigned settings."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.side_effect = [
        Path("/backup"),
        # settings 1 (alphadia)
        Path("/settings"),
        Path("/output"),
        # settings 2 (msqc — also needs software path for custom command)
        Path("/settings"),
        Path("/output"),
        Path("/software"),
    ]
    mock_settings_alphadia = MagicMock(
        name="alphadia_default",
        speclib_file_name="lib.speclib",
        fasta_file_name="human.fasta",
        config_file_name="config.yaml",
        config_params="",
        software="alphadia-1.0",
        software_type="alphadia",
        version=1,
        slurm_cpus_per_task=8,
        slurm_mem="62G",
        slurm_time="02:00:00",
        num_threads=8,
    )
    mock_settings_msqc = MagicMock(
        name="msqc_default",
        speclib_file_name="lib.speclib",
        fasta_file_name="human.fasta",
        config_file_name="config.yaml",
        config_params="",
        software="msqc-1.0",
        software_type="msqc",
        version=1,
        slurm_cpus_per_task=2,
        slurm_mem="31G",
        slurm_time="00:10:00",
        num_threads=2,
    )
    mock_get_settings.return_value = [MagicMock(), MagicMock()]
    mock_resolve_scoped.return_value = [mock_settings_alphadia, mock_settings_msqc]

    result = prepare_quanting(raw_file_id="test_file.raw")

    assert len(result) == 2
    assert result[0][QuantingEnv.SOFTWARE_TYPE] == "alphadia"
    assert result[1][QuantingEnv.SOFTWARE_TYPE] == "msqc"
    assert "alphadia" in result[0][QuantingEnv.OUTPUT_PATH]
    assert "msqc" in result[1][QuantingEnv.OUTPUT_PATH]

    assert result[0][QuantingEnv.SLURM_CPUS_PER_TASK] == 8
    assert result[0][QuantingEnv.SLURM_MEM] == "62G"
    assert result[0][QuantingEnv.SLURM_TIME] == "02:00:00"
    assert result[0][QuantingEnv.NUM_THREADS] == 8

    assert result[1][QuantingEnv.SLURM_CPUS_PER_TASK] == 2
    assert result[1][QuantingEnv.SLURM_MEM] == "31G"
    assert result[1][QuantingEnv.SLURM_TIME] == "00:10:00"
    assert result[1][QuantingEnv.NUM_THREADS] == 2


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "thermo"}})
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
def test_prepare_quanting_custom_slurm_params(
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that custom SLURM params from settings take precedence over defaults."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.side_effect = [
        Path("/some_backup_base_path"),
        Path("/some_quanting_settings_path"),
        Path("/some_quanting_output_path"),
    ]
    mock_settings = MagicMock()
    mock_settings.name = "test_settings"
    mock_settings.speclib_file_name = "some_speclib_file_name"
    mock_settings.fasta_file_name = "some_fasta_file_name"
    mock_settings.config_file_name = "some_config_file_name"
    mock_settings.config_params = ""
    mock_settings.software = "some_software"
    mock_settings.software_type = "alphadia"
    mock_settings.metrics_type = "alphadia"
    mock_settings.version = 1
    mock_settings.slurm_cpus_per_task = 16
    mock_settings.slurm_mem = "128G"
    mock_settings.slurm_time = "04:00:00"
    mock_settings.num_threads = 16
    mock_get_settings.return_value = [MagicMock()]
    mock_resolve_scoped.return_value = [mock_settings]

    result = prepare_quanting(raw_file_id="test_file.raw")

    assert result[0][QuantingEnv.SLURM_CPUS_PER_TASK] == 16
    assert result[0][QuantingEnv.SLURM_MEM] == "128G"
    assert result[0][QuantingEnv.SLURM_TIME] == "04:00:00"
    assert result[0][QuantingEnv.NUM_THREADS] == 16


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "thermo"}})
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
def test_prepare_quanting_validation_error_raises(
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that prepare_quanting raises on validation errors."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.side_effect = [
        Path("some_backup_base_path"),
        Path("some_quanting_settings_path"),
        Path("some_quanting_output_path"),
        Path("some_software_base_path"),
    ]
    mock_get_settings.return_value = [MagicMock()]
    mock_resolve_scoped.return_value = [
        MagicMock(
            speclib_file_name="some_speclib_file_name",
            fasta_file_name="../some_fasta_file_name",  # .. -> this will raise
            config_file_name="",
            config_params="--qvalue 0.01 --f RAW_FILE_PATH --lib LIBRARY_PATH --out OUTPUT_PATH --fasta FASTA_PATH",
            software="custom1.2.3",
            software_type="custom",
            version=1,
            slurm_cpus_per_task=None,
            slurm_mem=None,
            slurm_time=None,
            num_threads=None,
        )
    ]

    # when
    with pytest.raises(AirflowFailException):
        prepare_quanting(raw_file_id="test_file.raw")


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.get_instrument_settings")
def test_prepare_quanting_no_project_raise(
    mock_get_instrument_settings: MagicMock,
    mock_get_settings: MagicMock,
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

    mock_get_instrument_settings.return_value = "thermo"

    mock_get_settings.side_effect = DoesNotExist

    # when
    with pytest.raises(AirflowFailException):
        prepare_quanting(raw_file_id="test_file.raw")


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
@patch("dags.impl.processor_impl.get_instrument_settings")
def test_prepare_quanting_no_settings_raise(
    mock_get_instrument_settings: MagicMock,
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
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

    mock_get_instrument_settings.return_value = "thermo"

    mock_get_settings.return_value = [MagicMock()]
    mock_resolve_scoped.return_value = []

    # when
    with pytest.raises(AirflowFailException):
        prepare_quanting(raw_file_id="test_file.raw")


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


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.start_job")
@patch("dags.impl.processor_impl.update_raw_file")
def test_run_quanting_executes_ssh_command_and_stores_job_id(
    mock_update: MagicMock,
    mock_start_job: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that the run_quanting function executes the SSH command and stores the job ID."""
    # given
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
        QuantingEnv.CUSTOM_COMMAND: "",
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID123/out_test_file.raw/alphadia",
    }
    mock_raw_file = MagicMock(
        wraps=RawFile,
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        instrument_id="_test1_",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_start_job.return_value = "12345"

    # when
    result = run_quanting(quanting_env=quanting_env)

    assert result == "12345"
    mock_start_job.assert_called_once_with(
        "submit_job.sh",
        quanting_env,
        "1970_01",
    )
    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_update.assert_called_once_with(
        "test_file.raw", new_status=RawFileStatus.QUANTING
    )


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_airflow_variable")
def test_run_quanting_output_folder_exists(
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """run_quanting function raises an exception if the output path already exists."""
    # given
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
        QuantingEnv.CUSTOM_COMMAND: "",
        QuantingEnv.INTERNAL_OUTPUT_PATH: str(output_dir),
    }
    mock_raw_file = MagicMock(
        wraps=RawFile,
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        instrument_id="_test1_",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_airflow_variable.return_value = "raise"

    # when
    with pytest.raises(AirflowFailException):
        run_quanting(quanting_env=quanting_env)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_airflow_variable.assert_called_once_with("output_exists_mode", "raise")


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl._get_slurm_job_id_from_log")
def test_run_quanting_output_folder_exists_associate(
    mock_get_slurm_job_id_from_log: MagicMock,
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """run_quanting function returns extracted job_id if the output path already exists and mode is 'associate'."""
    # given
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
        QuantingEnv.CUSTOM_COMMAND: "",
        QuantingEnv.INTERNAL_OUTPUT_PATH: str(output_dir),
    }
    mock_raw_file = MagicMock(
        wraps=RawFile,
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        instrument_id="_test1_",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_airflow_variable.return_value = "associate"
    mock_get_slurm_job_id_from_log.return_value = "54321"

    # when
    result = run_quanting(quanting_env=quanting_env)

    assert result == "54321"


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl._get_slurm_job_id_from_log")
def test_run_quanting_output_folder_exists_associate_raise(
    mock_get_slurm_job_id_from_log: MagicMock,
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """run_quanting function correctly raises if the output path already exists and mode is 'associate' and no job id."""
    # given
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID123",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
        QuantingEnv.CUSTOM_COMMAND: "",
        QuantingEnv.INTERNAL_OUTPUT_PATH: str(output_dir),
    }
    mock_raw_file = MagicMock(
        wraps=RawFile,
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        instrument_id="_test1_",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_airflow_variable.return_value = "associate"
    mock_get_slurm_job_id_from_log.return_value = None

    # when
    with pytest.raises(AirflowFailException):
        run_quanting(quanting_env=quanting_env)


@patch("dags.impl.processor_impl.get_job_result")
def test_check_quanting_result_happy_path(
    mock_get_job_result: MagicMock,
) -> None:
    """Test that check_quanting_result makes the expected calls."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.METRICS_TYPE: "alphadia",
    }

    mock_get_job_result.return_value = (JobStates.COMPLETED, 522)

    # when
    result = check_quanting_result(quanting_env=quanting_env, job_id="12345")

    assert result == {"quanting_time_elapsed": 522}


@patch("dags.impl.processor_impl.get_job_result")
def test_check_quanting_result_unknown_job_status(
    mock_get_job_result: MagicMock,
) -> None:
    """Test that check_quanting_result raises on unknown quanting job status."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.METRICS_TYPE: "alphadia",
    }
    mock_get_job_result.return_value = ("SOME_JOB_STATE", 522)

    # when
    with pytest.raises(AirflowFailException):
        check_quanting_result(quanting_env=quanting_env, job_id="12345")


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_business_error(
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly on business errors."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = ("FAILED", 522)
    mock_get_business_errors.return_value = ["error1", "error2"]

    # when
    with pytest.raises(AirflowSkipException):
        check_quanting_result(quanting_env=quanting_env, job_id="12345")

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(
        mock_raw_file,
        Path("/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia"),
    )
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="error1;error2",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
    )


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_business_error_raises(
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly if business error is unknown."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "FAILED", 522
    mock_get_business_errors.return_value = ["error1", "__UNKNOWN_ERROR"]

    # when
    with pytest.raises(AirflowFailException):
        check_quanting_result(quanting_env=quanting_env, job_id="12345")

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(
        mock_raw_file,
        Path("/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia"),
    )
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="error1;__UNKNOWN_ERROR",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
    )


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_timeout(
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly on timeout."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "TIMEOUT", 522

    # when
    with pytest.raises(AirflowSkipException):
        check_quanting_result(quanting_env=quanting_env, job_id="12345")

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="TIMEOUT",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
    )


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.update_raw_file")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_quanting_result_oom(
    mock_add_metrics: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that check_quanting_result behaves correctly on out of memory."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID_OR_FALLBACK: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "OUT_OF_ME+", 522

    # when
    with pytest.raises(AirflowSkipException):
        check_quanting_result(quanting_env=quanting_env, job_id="12345")

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_update_raw_file.assert_called_once_with(
        "test_file.raw",
        new_status="quanting_failed",
        status_details="OUT_OF_MEMORY",
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"quanting_time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
    )


def test_get_business_errors_with_valid_errors(tmp_path: Path) -> None:
    """Test that get_business_errors returns the expected business errors."""
    raw_file = MagicMock()
    raw_file.id = "test_file.raw"

    progress_dir = tmp_path / "quant" / "test_file"
    progress_dir.mkdir(parents=True)
    events_file = progress_dir / "events.jsonl"
    events_file.write_text(
        '{"name": "exception", "error_code": "ERROR1"}\n'
        '{"name": "exception", "error_code": "ERROR2"}\n'
        '{"name": "other", "error_code": "ERROR3"}\n'
    )

    result = get_business_errors(raw_file, tmp_path)

    assert result == ["ERROR1", "ERROR2"]


def test_get_business_errors_with_no_errors(tmp_path: Path) -> None:
    """Test that get_business_errors returns no-log-file error when events have no valid errors."""
    raw_file = MagicMock()
    raw_file.id = "test_file.raw"

    progress_dir = tmp_path / "quant" / "test_file"
    progress_dir.mkdir(parents=True)
    events_file = progress_dir / "events.jsonl"
    events_file.write_text(
        '{"name": "other", "error_code": "ERROR3"}\n'
        '{"name": "exception", "error_code": ""}\n'
        "invalid json\n"
        '{"name": "exception"}\n'
    )

    result = get_business_errors(raw_file, tmp_path)

    assert result == ["__NO_LOG_FILE"]


def test_get_business_errors_file_not_found(tmp_path: Path) -> None:
    """Test that get_business_errors returns no-log-file error when events file is not found."""
    raw_file = MagicMock()
    raw_file.id = "test_file.raw"

    result = get_business_errors(raw_file, tmp_path)

    assert result == ["__NO_LOG_FILE"]


def test_get_business_errors_with_unknown_error(tmp_path: Path) -> None:
    """Test that get_business_errors returns unknown error from log file."""
    raw_file = MagicMock()
    raw_file.id = "test_file.raw"

    progress_dir = tmp_path / "quant" / "test_file"
    progress_dir.mkdir(parents=True)
    events_file = progress_dir / "events.jsonl"
    events_file.write_text('{"name": "other", "error_code": "ERROR3"}\n')

    log_file = tmp_path / "log.txt"
    log_file.write_text("ERROR: bla\n")

    result = get_business_errors(raw_file, tmp_path)

    assert result == ["__UNKNOWN_ERROR"]


@patch("dags.impl.processor_impl.calc_metrics")
def test_compute_metrics(
    mock_calc_metrics: MagicMock,
) -> None:
    """Test that compute_metrics makes the expected calls."""
    quanting_env = {
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID_OR_FALLBACK": "P1",
        "SOFTWARE_TYPE": "alphadia",
        "METRICS_TYPE": "alphadia",
        "_INTERNAL_OUTPUT_PATH": "/opt/airflow/mounts/output/P1/out_test_file.raw/alphadia",
    }

    mock_calc_metrics.return_value = {"metric1": "value1"}

    # when
    result = compute_metrics(quanting_env=quanting_env, quanting_time_elapsed=123)

    mock_calc_metrics.assert_called_once_with(
        Path("/opt/airflow/mounts/output/P1/out_test_file.raw/alphadia"),
        metrics_type="alphadia",
    )
    assert result == {
        "metrics": {"metric1": "value1", "quanting_time_elapsed": 123},
        "metrics_type": "alphadia",
    }


@patch("dags.impl.processor_impl.calc_metrics")
def test_compute_metrics_msqc_software_type(
    mock_calc_metrics: MagicMock,
) -> None:
    """Test that compute_metrics correctly maps MSQC software type to MSQC metrics type."""
    quanting_env = {
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID_OR_FALLBACK": "P1",
        "SOFTWARE_TYPE": "msqc",
        "METRICS_TYPE": "msqc",
        "_INTERNAL_OUTPUT_PATH": "/opt/airflow/mounts/output/P1/out_test_file.raw/msqc",
    }
    mock_calc_metrics.return_value = {"qc_metric": 42}

    result = compute_metrics(quanting_env=quanting_env)

    mock_calc_metrics.assert_called_once_with(
        Path("/opt/airflow/mounts/output/P1/out_test_file.raw/msqc"),
        metrics_type="msqc",
    )
    assert result == {
        "metrics": {"qc_metric": 42},
        "metrics_type": "msqc",
    }


@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_upload_metrics(
    mock_add: MagicMock,
) -> None:
    """Test that upload_metrics makes the expected calls."""
    # when
    upload_metrics(
        quanting_env={
            "SETTINGS_NAME": "test_settings",
            "SETTINGS_VERSION": 1,
            "RAW_FILE_ID": "some_file.raw",
        },
        metrics={"metric1": "value1"},
        metrics_type="alphadia",
    )

    mock_add.assert_called_once_with(
        "some_file.raw",
        metrics_type="alphadia",
        metrics={
            "metric1": "value1",
        },
        settings_name="test_settings",
        settings_version=1,
    )


@patch("dags.impl.processor_impl.update_raw_file")
def test_finalize_raw_file_status_all_succeeded(mock_update: MagicMock) -> None:
    """Test that finalize_raw_file_status sets DONE when all branches succeed."""
    ti = MagicMock()
    upload_ti = MagicMock(task_id=_UPLOAD_METRICS_TASK_ID, state="success")
    other_ti = MagicMock(task_id="quanting_pipeline.compute_metrics", state="success")
    ti.get_dagrun.return_value.get_task_instances.return_value = [upload_ti, other_ti]

    finalize_raw_file_status(ti=ti, raw_file_id="test.raw")

    mock_update.assert_called_once_with(
        "test.raw", new_status=RawFileStatus.DONE, status_details=None
    )


@patch("dags.impl.processor_impl.update_raw_file")
def test_finalize_raw_file_status_branch_failed(mock_update: MagicMock) -> None:
    """Test that finalize_raw_file_status sets ERROR and raises when a branch fails."""
    ti = MagicMock()
    upload_ti_ok = MagicMock(task_id=_UPLOAD_METRICS_TASK_ID, state="success")
    upload_ti_fail = MagicMock(task_id=_UPLOAD_METRICS_TASK_ID, state="failed")
    ti.get_dagrun.return_value.get_task_instances.return_value = [
        upload_ti_ok,
        upload_ti_fail,
    ]

    with pytest.raises(AirflowFailException):
        finalize_raw_file_status(ti=ti, raw_file_id="test.raw")

    mock_update.assert_called_once_with("test.raw", new_status=RawFileStatus.ERROR)


@patch("dags.impl.processor_impl.update_raw_file")
def test_finalize_raw_file_status_upstream_failed(mock_update: MagicMock) -> None:
    """Test that finalize_raw_file_status treats upstream_failed as failure."""
    ti = MagicMock()
    upload_ti = MagicMock(task_id=_UPLOAD_METRICS_TASK_ID, state="upstream_failed")
    ti.get_dagrun.return_value.get_task_instances.return_value = [upload_ti]

    with pytest.raises(AirflowFailException):
        finalize_raw_file_status(ti=ti, raw_file_id="test.raw")

    mock_update.assert_called_once_with("test.raw", new_status=RawFileStatus.ERROR)


def test_finalize_raw_file_status_no_upload_tasks() -> None:
    """Test that finalize_raw_file_status raises when no upload_metrics tasks found."""
    ti = MagicMock()
    ti.get_dagrun.return_value.get_task_instances.return_value = [
        MagicMock(task_id="some_other_task", state="success")
    ]

    with pytest.raises(AirflowFailException):
        finalize_raw_file_status(ti=ti, raw_file_id="test.raw")
