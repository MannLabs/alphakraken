"""Tests for the processor_impl module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
import pytz
from airflow.exceptions import AirflowFailException
from common.settings import _INSTRUMENTS
from dags.impl.processor_impl import (
    _PREPARE_JOB_TASK_ID,
    _TASK_GROUP_PREFIX,
    QuantingFailedKnownErrorException,
    QuantingFailedNewErrorException,
    _create_quanting_env,
    _extract_errors,
    _find_next_free_run_suffix,
    _get_slurm_job_id_from_log,
    check_job_result,
    compute_metrics,
    finalize_raw_file_status,
    get_business_errors,
    prepare_job,
    resolve_settings,
    run_job,
    store_metrics,
)
from mongoengine import DoesNotExist
from plugins.common.keys import (
    JobStates,
    QuantingEnv,
    XComKeys,
)

from shared.db.models import RawFile, RawFileStatus


@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.get_output_folder_rel_path")
@patch("dags.impl.processor_impl.get_internal_output_path_for_raw_file")
def test_create_quanting_env(
    mock_internal_output_path: MagicMock,
    mock_output_rel_path: MagicMock,
    mock_get_path: MagicMock,
) -> None:
    """Test that _create_quanting_env builds the expected environment dict."""
    mock_get_path.side_effect = [
        Path("/some_quanting_settings_path"),
        Path("/some_quanting_output_path"),
    ]
    mock_output_rel_path.return_value = Path(
        "some_project_id/out_test_file.raw/alphadia"
    )
    mock_internal_output_path.return_value = Path(
        "/opt/airflow/mounts/output/some_project_id/out_test_file.raw/alphadia"
    )

    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        project_id="some_project_id",
    )
    mock_settings = MagicMock()
    mock_settings.name = "test_settings"
    mock_settings.speclib_file_name = "some_speclib_file_name"
    mock_settings.fasta_file_name = "some_fasta_file_name"
    mock_settings.config_file_name = "some_config_file_name"
    mock_settings.software = "some_software"
    mock_settings.software_type = "alphadia"
    mock_settings.metrics_type = "alphadia"
    mock_settings.version = 1
    mock_settings.slurm_cpus_per_task = 8
    mock_settings.slurm_mem = "62G"
    mock_settings.slurm_time = "02:00:00"
    mock_settings.num_threads = 8

    result = _create_quanting_env(
        settings=mock_settings,
        raw_file=mock_raw_file,
        raw_file_path=Path("/some_backup_base_path/instrument1/1970_01/test_file.raw"),
        relative_raw_file_path=Path("instrument1/1970_01/test_file.raw"),
    )

    # when you adapt something here, don't forget to adapt also the submit_job.sh script
    expected = {
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
        "PROJECT_ID": "some_project_id",
        "SETTINGS_NAME": "test_settings",
        "SETTINGS_VERSION": 1,
        "_INTERNAL_OUTPUT_PATH": "/opt/airflow/mounts/output/some_project_id/out_test_file.raw/alphadia",
    }
    assert result == expected


@patch("dags.impl.processor_impl.get_path")
@patch("dags.impl.processor_impl.get_output_folder_rel_path")
@patch("dags.impl.processor_impl.get_internal_output_path_for_raw_file")
def test_create_quanting_env_custom_software(
    mock_internal_output_path: MagicMock,
    mock_output_rel_path: MagicMock,
    mock_get_path: MagicMock,
) -> None:
    """Test that _create_quanting_env handles custom software settings with parameter substitution."""
    mock_get_path.side_effect = [
        Path("/some_quanting_settings_path"),
        Path("/some_quanting_output_path"),
        Path("/some_software_base_path"),
    ]
    mock_output_rel_path.return_value = Path("some_project_id/out_test_file.raw/custom")
    mock_internal_output_path.return_value = Path(
        "/opt/airflow/mounts/output/some_project_id/out_test_file.raw/custom"
    )

    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        project_id="some_project_id",
    )
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

    result = _create_quanting_env(
        settings=mock_settings,
        raw_file=mock_raw_file,
        raw_file_path=Path("/some_backup_base_path/instrument1/1970_01/test_file.raw"),
        relative_raw_file_path=Path("instrument1/1970_01/test_file.raw"),
    )

    expected_custom_command = (
        "/some_software_base_path/custom1.2.3 --qvalue 0.01 --f /some_backup_base_path/instrument1/1970_01/test_file.raw "
        "--lib /some_quanting_settings_path/test_custom_settings/some_speclib_file_name "
        "--out /some_quanting_output_path/some_project_id/out_test_file.raw/custom "
        "--fasta /some_quanting_settings_path/test_custom_settings/some_fasta_file_name --threads 8 "
        "--some_param instrument1/1970_01/test_file.raw --some_param2 some_project_id/out_test_file.raw/custom"
    )

    expected = {
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
        "PROJECT_ID": "some_project_id",
        "SETTINGS_NAME": "test_custom_settings",
        "SETTINGS_VERSION": 1,
        "_INTERNAL_OUTPUT_PATH": "/opt/airflow/mounts/output/some_project_id/out_test_file.raw/custom",
    }
    assert result == expected


@patch.dict(_INSTRUMENTS, {"instrument1": {"type": "thermo"}})
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
@patch("dags.impl.processor_impl.get_internal_output_path_for_raw_file")
def test_resolve_settings(
    mock_get_internal_output_path: MagicMock,
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that resolve_settings returns settings info dicts."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_settings_1 = MagicMock()
    mock_settings_1.id = "sid1"
    mock_settings_1.name = "settings_A"
    mock_settings_2 = MagicMock()
    mock_settings_2.id = "sid2"
    mock_settings_2.name = "settings_B"
    mock_get_settings.return_value = [mock_settings_1, mock_settings_2]
    mock_resolve_scoped.return_value = [mock_settings_1, mock_settings_2]

    result = resolve_settings(raw_file_id="test_file.raw")

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_settings.assert_called_once_with("some_project_id")
    mock_resolve_scoped.assert_called_once()
    mock_get_internal_output_path.assert_called_once_with(mock_raw_file)
    mock_get_internal_output_path.return_value.mkdir.assert_called_once_with(
        parents=True, exist_ok=True
    )
    assert result == ["sid1", "sid2"]


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.get_instrument_settings")
def test_resolve_settings_no_project_raise(
    mock_get_instrument_settings: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that resolve_settings raises an exception if no project is found."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_instrument_settings.return_value = "thermo"

    mock_get_settings.side_effect = DoesNotExist

    with pytest.raises(AirflowFailException):
        resolve_settings(raw_file_id="test_file.raw")


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_project_settings")
@patch("dags.impl.processor_impl.resolve_scoped_settings")
@patch("dags.impl.processor_impl.get_instrument_settings")
def test_resolve_settings_no_settings_raise(
    mock_get_instrument_settings: MagicMock,
    mock_resolve_scoped: MagicMock,
    mock_get_settings: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test that resolve_settings raises an exception if no settings are found."""
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

    with pytest.raises(AirflowFailException):
        resolve_settings(raw_file_id="test_file.raw")


@patch("dags.impl.processor_impl._create_quanting_env")
@patch("dags.impl.processor_impl.get_settings_by_id")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
def test_prepare_job(
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_settings_by_id: MagicMock,
    mock_create_env: MagicMock,
) -> None:
    """Test that prepare_job orchestrates the expected calls for a single settings entry."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.return_value = Path("/some_backup_base_path")
    mock_settings = MagicMock(config_params=[])
    mock_get_settings_by_id.return_value = mock_settings
    mock_env = {
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/nonexistent/output/path",
    }
    mock_create_env.return_value = mock_env

    result = prepare_job(raw_file_id="test_file.raw", settings_id="sid1")

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_settings_by_id.assert_called_once_with("sid1")
    mock_create_env.assert_called_once_with(
        mock_settings,
        mock_raw_file,
        Path("/some_backup_base_path/instrument1/1970_01/test_file.raw"),
        Path("instrument1/1970_01/test_file.raw"),
    )
    assert result == mock_env


@patch("dags.impl.processor_impl._check_content")
@patch("dags.impl.processor_impl._create_quanting_env")
@patch("dags.impl.processor_impl.get_settings_by_id")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
def test_prepare_job_validation_error_raises(
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_settings_by_id: MagicMock,
    mock_create_env: MagicMock,
    mock_check_content: MagicMock,
) -> None:
    """Test that prepare_job raises on validation errors in the quanting env."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.return_value = Path("/some_backup_base_path")
    mock_settings = MagicMock()
    mock_get_settings_by_id.return_value = mock_settings
    mock_env = {"SOFTWARE_TYPE": "custom"}
    mock_create_env.return_value = mock_env
    mock_check_content.return_value = ["some_error"]

    with pytest.raises(AirflowFailException, match="some_error"):
        prepare_job(raw_file_id="test_file.raw", settings_id="sid1")

    mock_create_env.assert_called_once_with(
        mock_settings,
        mock_raw_file,
        Path("/some_backup_base_path/instrument1/1970_01/test_file.raw"),
        Path("instrument1/1970_01/test_file.raw"),
    )
    mock_check_content.assert_called_once_with(mock_env, mock_settings)


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
def test_run_job_executes_ssh_command_and_stores_job_id(
    mock_update: MagicMock,
    mock_start_job: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """Test that the run_job function executes the SSH command and stores the job ID."""
    # given
    output_dir = tmp_path / "PID123" / "out_test_file.raw" / "alphadia"
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID123",
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

    mock_start_job.return_value = "12345"

    # when
    result = run_job(quanting_env=quanting_env)

    assert result == "12345"
    assert output_dir.exists()
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
def test_run_job_output_folder_exists(
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """run_job function raises an exception if the output path already exists."""
    # given
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID123",
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
        run_job(quanting_env=quanting_env)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_airflow_variable.assert_called_once_with("output_exists_mode", "raise")


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl._get_slurm_job_id_from_log")
def test_run_job_output_folder_exists_associate(
    mock_get_slurm_job_id_from_log: MagicMock,
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """run_job function returns extracted job_id if the output path already exists and mode is 'associate'."""
    # given
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID123",
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
    result = run_job(quanting_env=quanting_env)

    assert result == "54321"


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl._get_slurm_job_id_from_log")
def test_run_job_output_folder_exists_associate_raise(
    mock_get_slurm_job_id_from_log: MagicMock,
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """run_job function correctly raises if the output path already exists and mode is 'associate' and no job id."""
    # given
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID123",
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
        run_job(quanting_env=quanting_env)


def test_find_next_free_run_suffix(tmp_path: Path) -> None:
    """Test that _find_next_free_run_suffix finds the next available .runN suffix."""
    base = tmp_path / "output"
    base.mkdir()

    # base exists -> .run2
    assert _find_next_free_run_suffix(base) == ".run2"

    # base + .run2 exist -> .run3
    (tmp_path / "output.run2").mkdir()
    assert _find_next_free_run_suffix(base) == ".run3"

    # base + .run2 + .run3 exist -> .run4
    (tmp_path / "output.run3").mkdir()
    assert _find_next_free_run_suffix(base) == ".run4"


def test_find_next_free_run_suffix_base_not_exists(tmp_path: Path) -> None:
    """Test that _find_next_free_run_suffix returns .run2 when base does not exist."""
    base = tmp_path / "output"
    assert _find_next_free_run_suffix(base) == ".run2"


@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl._check_content")
@patch("dags.impl.processor_impl._create_quanting_env")
@patch("dags.impl.processor_impl.get_settings_by_id")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_path")
def test_prepare_job_add_mode(  # noqa: PLR0913
    mock_get_path: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_settings_by_id: MagicMock,
    mock_create_env: MagicMock,
    mock_check_content: MagicMock,
    mock_get_airflow_variable: MagicMock,
    tmp_path: Path,
) -> None:
    """Test that prepare_job suffixes paths when output_exists_mode is 'add' and output exists."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="test_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
        instrument_id="instrument1",
    )
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_path.return_value = Path("/some_backup_base_path")
    mock_settings = MagicMock(config_params=[])
    mock_get_settings_by_id.return_value = mock_settings
    mock_check_content.return_value = []

    # create the internal output path so the "add" logic triggers
    internal_output = tmp_path / "internal_output"
    internal_output.mkdir()

    mock_create_env.return_value = {
        QuantingEnv.INTERNAL_OUTPUT_PATH: str(internal_output),
        QuantingEnv.OUTPUT_PATH: "/output/some_path",
        QuantingEnv.RELATIVE_OUTPUT_PATH: "some_path",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
    }
    mock_get_airflow_variable.return_value = "add"

    result = prepare_job(raw_file_id="test_file.raw", settings_id="sid1")

    assert result[QuantingEnv.INTERNAL_OUTPUT_PATH] == str(
        internal_output.parent / "internal_output.run2"
    )
    assert result[QuantingEnv.OUTPUT_PATH] == "/output/some_path.run2"
    assert result[QuantingEnv.RELATIVE_OUTPUT_PATH] == "some_path.run2"


@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_airflow_variable")
@patch("dags.impl.processor_impl.start_job")
@patch("dags.impl.processor_impl.update_raw_file")
def test_run_job_output_folder_exists_add(
    mock_update: MagicMock,  # noqa: ARG001
    mock_start_job: MagicMock,  # noqa: ARG001
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    tmp_path: Path,
) -> None:
    """run_job raises when output_exists_mode is 'add' but the output path already exists."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID123",
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
    mock_get_airflow_variable.return_value = "add"

    with pytest.raises(AirflowFailException, match="should have created a unique name"):
        run_job(quanting_env=quanting_env)


@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.get_job_result")
def test_check_job_result_happy_path(
    mock_get_job_result: MagicMock,
    mock_put_xcom: MagicMock,
) -> None:
    """Test that check_job_result makes the expected calls."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.METRICS_TYPE: "alphadia",
    }

    mock_get_job_result.return_value = (JobStates.COMPLETED, 522)
    mock_ti = MagicMock()

    # when
    result = check_job_result(quanting_env=quanting_env, job_id="12345", ti=mock_ti)

    assert result == {"time_elapsed": 522}
    mock_put_xcom.assert_not_called()


@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.get_job_result")
def test_check_job_result_unknown_job_status(
    mock_get_job_result: MagicMock,
    mock_put_xcom: MagicMock,
) -> None:
    """Test that check_job_result raises on unknown quanting job status."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.METRICS_TYPE: "alphadia",
    }
    mock_get_job_result.return_value = ("SOME_JOB_STATE", 522)
    mock_ti = MagicMock()

    # when
    with pytest.raises(AirflowFailException):
        check_job_result(quanting_env=quanting_env, job_id="12345", ti=mock_ti)

    mock_put_xcom.assert_called_once_with(
        mock_ti, key=XComKeys.BRANCH_ERRORS, value="unknown_job_status: SOME_JOB_STATE"
    )


@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_job_result_business_error(
    mock_add_metrics: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_put_xcom: MagicMock,
) -> None:
    """Test that check_job_result behaves correctly on business errors."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.OUTPUT_PATH: "/data/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = ("FAILED", 522)
    mock_get_business_errors.return_value = ["error1", "error2"]
    mock_ti = MagicMock()

    # when
    with pytest.raises(QuantingFailedKnownErrorException):
        check_job_result(quanting_env=quanting_env, job_id="12345", ti=mock_ti)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(
        mock_raw_file,
        Path("/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia"),
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
        output_path="/data/output/PID1/out_test_file.raw/alphadia",
    )
    mock_put_xcom.assert_called_once_with(
        mock_ti, key=XComKeys.BRANCH_ERRORS, value="error1;error2"
    )


@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.get_business_errors")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_job_result_business_error_raises(
    mock_add_metrics: MagicMock,
    mock_get_business_errors: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_put_xcom: MagicMock,
) -> None:
    """Test that check_job_result behaves correctly if business error is unknown."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.OUTPUT_PATH: "/data/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
        QuantingEnv.SOFTWARE_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "FAILED", 522
    mock_get_business_errors.return_value = ["error1", "__UNKNOWN_ERROR"]
    mock_ti = MagicMock()

    # when
    with pytest.raises(QuantingFailedNewErrorException):
        check_job_result(quanting_env=quanting_env, job_id="12345", ti=mock_ti)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_get_business_errors.assert_called_once_with(
        mock_raw_file,
        Path("/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia"),
    )
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
        output_path="/data/output/PID1/out_test_file.raw/alphadia",
    )
    mock_put_xcom.assert_called_once_with(
        mock_ti, key=XComKeys.BRANCH_ERRORS, value="error1;__UNKNOWN_ERROR"
    )


@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_job_result_timeout(
    mock_add_metrics: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_put_xcom: MagicMock,
) -> None:
    """Test that check_job_result behaves correctly on timeout."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.OUTPUT_PATH: "/data/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "TIMEOUT", 522
    mock_ti = MagicMock()

    # when
    with pytest.raises(QuantingFailedKnownErrorException):
        check_job_result(quanting_env=quanting_env, job_id="12345", ti=mock_ti)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
        output_path="/data/output/PID1/out_test_file.raw/alphadia",
    )
    mock_put_xcom.assert_called_once_with(
        mock_ti, key=XComKeys.BRANCH_ERRORS, value="TIMEOUT"
    )


@patch("dags.impl.processor_impl.put_xcom")
@patch("dags.impl.processor_impl.get_raw_file_by_id")
@patch("dags.impl.processor_impl.get_job_result")
@patch("dags.impl.processor_impl.add_metrics_to_raw_file")
def test_check_job_result_oom(
    mock_add_metrics: MagicMock,
    mock_get_job_result: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_put_xcom: MagicMock,
) -> None:
    """Test that check_job_result behaves correctly on out of memory."""
    quanting_env = {
        QuantingEnv.RAW_FILE_ID: "test_file.raw",
        QuantingEnv.PROJECT_ID: "PID1",
        QuantingEnv.SETTINGS_NAME: "test_settings",
        QuantingEnv.SETTINGS_VERSION: 1,
        QuantingEnv.INTERNAL_OUTPUT_PATH: "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.OUTPUT_PATH: "/data/output/PID1/out_test_file.raw/alphadia",
        QuantingEnv.METRICS_TYPE: "alphadia",
    }
    mock_raw_file = MagicMock(wraps=RawFile, id="test_file.raw")
    mock_get_raw_file_by_id.return_value = mock_raw_file
    mock_get_job_result.return_value = "OUT_OF_ME+", 522
    mock_ti = MagicMock()

    # when
    with pytest.raises(QuantingFailedKnownErrorException):
        check_job_result(quanting_env=quanting_env, job_id="12345", ti=mock_ti)

    mock_get_raw_file_by_id.assert_called_once_with("test_file.raw")
    mock_add_metrics.assert_called_once_with(
        "test_file.raw",
        metrics={"time_elapsed": 522},
        settings_name="test_settings",
        settings_version=1,
        metrics_type="alphadia",
        output_path="/data/output/PID1/out_test_file.raw/alphadia",
    )
    mock_put_xcom.assert_called_once_with(
        mock_ti, key=XComKeys.BRANCH_ERRORS, value="OUT_OF_MEMORY"
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
        "PROJECT_ID": "P1",
        "SOFTWARE_TYPE": "alphadia",
        "METRICS_TYPE": "alphadia",
        "_INTERNAL_OUTPUT_PATH": "/opt/airflow/mounts/output/P1/out_test_file.raw/alphadia",
    }

    mock_calc_metrics.return_value = {"metric1": "value1"}

    # when
    result = compute_metrics(quanting_env=quanting_env, time_elapsed=123)

    mock_calc_metrics.assert_called_once_with(
        Path("/opt/airflow/mounts/output/P1/out_test_file.raw/alphadia"),
        metrics_type="alphadia",
    )
    assert result == {
        "metrics": {"metric1": "value1", "time_elapsed": 123},
        "metrics_type": "alphadia",
    }


@patch("dags.impl.processor_impl.calc_metrics")
def test_compute_metrics_msqc_software_type(
    mock_calc_metrics: MagicMock,
) -> None:
    """Test that compute_metrics correctly maps MSQC software type to MSQC metrics type."""
    quanting_env = {
        "RAW_FILE_ID": "test_file.raw",
        "PROJECT_ID": "P1",
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
def test_store_metrics(
    mock_add: MagicMock,
) -> None:
    """Test that store_metrics makes the expected calls."""
    # when
    store_metrics(
        quanting_env={
            "SETTINGS_NAME": "test_settings",
            "SETTINGS_VERSION": 1,
            "RAW_FILE_ID": "some_file.raw",
            "OUTPUT_PATH": "/data/output/P1/out_some_file.raw/alphadia",
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
        output_path="/data/output/P1/out_some_file.raw/alphadia",
    )


# --- helpers for finalize / _extract_errors tests ---


def _branch_ti(task_name: str, map_index: int, state: str) -> MagicMock:
    """Create a mock TaskInstance for a task within a processing branch."""
    return MagicMock(
        task_id=f"{_TASK_GROUP_PREFIX}{task_name}",
        map_index=map_index,
        state=state,
    )


def _make_get_xcom(quanting_envs, branch_errors=None):  # noqa: ANN001, ANN202
    """Create a side_effect for get_xcom that returns the right values."""
    if branch_errors is None:
        branch_errors = {}

    def side_effect(_ti, *, key, **kwargs):  # noqa: ANN001, ANN202
        if key == XComKeys.BRANCH_ERRORS:
            return branch_errors.get(kwargs.get("map_indexes"))
        if kwargs.get("task_ids") == _PREPARE_JOB_TASK_ID:
            return quanting_envs[kwargs.get("map_indexes")]
        return quanting_envs

    return side_effect


# --- finalize_raw_file_status tests (mock _extract_errors) ---


@patch("dags.impl.processor_impl._extract_errors")
@patch("dags.impl.processor_impl.update_raw_file")
def test_finalize_sets_done_when_no_errors(
    mock_update: MagicMock, mock_extract: MagicMock
) -> None:
    """All branches succeeded → DONE."""
    ti = MagicMock()
    ti.get_dagrun.return_value.get_task_instances.return_value = [
        _branch_ti("run_job", 0, "success"),
    ]
    mock_extract.return_value = ([], [])

    finalize_raw_file_status(ti=ti, raw_file_id="test.raw")

    mock_update.assert_called_once_with(
        "test.raw", new_status=RawFileStatus.DONE, status_details=None
    )


@patch("dags.impl.processor_impl._extract_errors")
@patch("dags.impl.processor_impl.update_raw_file")
def test_finalize_sets_error_on_airflow_failures(
    mock_update: MagicMock, mock_extract: MagicMock
) -> None:
    """Airflow failure in any branch → ERROR with details, raises AirflowFailException."""
    ti = MagicMock()
    ti.get_dagrun.return_value.get_task_instances.return_value = [
        _branch_ti("run_job", 0, "success"),
        _branch_ti("run_job", 1, "failed"),
    ]
    mock_extract.return_value = (
        [("settings_B", "UNKNOWN_ERROR")],
        [],
    )

    with pytest.raises(AirflowFailException):
        finalize_raw_file_status(ti=ti, raw_file_id="test.raw")

    mock_update.assert_called_once_with(
        "test.raw",
        new_status=RawFileStatus.ERROR,
        status_details="error while processing: [settings_B] UNKNOWN_ERROR",
    )


@patch("dags.impl.processor_impl._extract_errors")
@patch("dags.impl.processor_impl.update_raw_file")
def test_finalize_sets_quanting_failed_on_business_errors(
    mock_update: MagicMock, mock_extract: MagicMock
) -> None:
    """Only business errors → QUANTING_FAILED with details, does not raise."""
    ti = MagicMock()
    ti.get_dagrun.return_value.get_task_instances.return_value = [
        _branch_ti("check_job_result", 0, "skipped"),
    ]
    mock_extract.return_value = (
        [],
        [("settings_A", "OUT_OF_MEMORY")],
    )

    finalize_raw_file_status(ti=ti, raw_file_id="test.raw")

    mock_update.assert_called_once_with(
        "test.raw",
        new_status=RawFileStatus.QUANTING_FAILED,
        status_details="error while processing: [settings_A] OUT_OF_MEMORY",
    )


@patch("dags.impl.processor_impl._extract_errors")
@patch("dags.impl.processor_impl.update_raw_file")
def test_finalize_error_takes_priority_over_business_errors(
    mock_update: MagicMock, mock_extract: MagicMock
) -> None:
    """Mix of airflow + business errors → ERROR with all details combined."""
    ti = MagicMock()
    ti.get_dagrun.return_value.get_task_instances.return_value = [
        _branch_ti("run_job", 0, "failed"),
        _branch_ti("check_job_result", 1, "skipped"),
    ]
    mock_extract.return_value = (
        [("settings_A", "failed at run_job")],
        [("settings_B", "TIMEOUT")],
    )

    with pytest.raises(AirflowFailException):
        finalize_raw_file_status(ti=ti, raw_file_id="test.raw")

    mock_update.assert_called_once_with(
        "test.raw",
        new_status=RawFileStatus.ERROR,
        status_details="error while processing: [settings_A] failed at run_job; [settings_B] TIMEOUT",
    )


def test_finalize_raises_when_no_branch_tasks() -> None:
    """No branch task instances → AirflowFailException."""
    ti = MagicMock()
    ti.get_dagrun.return_value.get_task_instances.return_value = [
        MagicMock(task_id="prepare_job", state="success", map_index=-1)
    ]

    with pytest.raises(AirflowFailException):
        finalize_raw_file_status(ti=ti, raw_file_id="test.raw")


# --- _extract_errors tests ---


def _make_branch_tis_by_index(branches):  # noqa: ANN001, ANN202
    """Build branch_tis_by_index dict from a compact spec.

    branches: list of (map_index, [(task_name, state), ...])
    """
    result: dict[int, list[MagicMock]] = {}
    for idx, tasks in branches:
        result[idx] = [_branch_ti(name, idx, state) for name, state in tasks]
    return result


@patch("dags.impl.processor_impl.get_xcom")
def test_extract_errors_all_success(mock_get_xcom: MagicMock) -> None:
    """All branches succeed → no errors."""
    branch_tis = _make_branch_tis_by_index(
        [
            (0, [("run_job", "success"), ("check_job_result", "success")]),
        ]
    )
    envs = [{QuantingEnv.SETTINGS_NAME: "s_A"}]
    mock_get_xcom.side_effect = _make_get_xcom(envs)

    airflow_errors, business_errors = _extract_errors(branch_tis, MagicMock())

    assert airflow_errors == []
    assert business_errors == []


@patch("dags.impl.processor_impl.get_xcom")
def test_extract_errors_business_error(mock_get_xcom: MagicMock) -> None:
    """check_job_result skipped with XCom → business error."""
    branch_tis = _make_branch_tis_by_index(
        [
            (
                0,
                [
                    ("run_job", "success"),
                    ("check_job_result", "skipped"),
                    ("store_metrics", "skipped"),
                ],
            ),
        ]
    )
    envs = [{QuantingEnv.SETTINGS_NAME: "s_A"}]
    mock_get_xcom.side_effect = _make_get_xcom(envs, branch_errors={0: "OUT_OF_MEMORY"})

    airflow_errors, business_errors = _extract_errors(branch_tis, MagicMock())

    assert airflow_errors == []
    assert business_errors == [("s_A", "OUT_OF_MEMORY")]


@patch("dags.impl.processor_impl.get_xcom")
def test_extract_errors_airflow_failure_with_xcom(mock_get_xcom: MagicMock) -> None:
    """check_job_result failed with XCom → airflow error with XCom details."""
    branch_tis = _make_branch_tis_by_index(
        [
            (
                0,
                [
                    ("run_job", "success"),
                    ("check_job_result", "failed"),
                    ("store_metrics", "upstream_failed"),
                ],
            ),
        ]
    )
    envs = [{QuantingEnv.SETTINGS_NAME: "s_A"}]
    mock_get_xcom.side_effect = _make_get_xcom(envs, branch_errors={0: "UNKNOWN_ERROR"})

    airflow_errors, business_errors = _extract_errors(branch_tis, MagicMock())

    assert airflow_errors == [("s_A", "UNKNOWN_ERROR")]
    assert business_errors == []


@patch("dags.impl.processor_impl.get_xcom")
def test_extract_errors_early_task_failed_no_xcom(mock_get_xcom: MagicMock) -> None:
    """Early task failed, no XCom available → fallback to task name."""
    branch_tis = _make_branch_tis_by_index(
        [
            (
                0,
                [
                    ("run_job", "failed"),
                    ("check_job_result", "upstream_failed"),
                    ("store_metrics", "upstream_failed"),
                ],
            ),
        ]
    )
    envs = [{QuantingEnv.SETTINGS_NAME: "s_A"}]
    mock_get_xcom.side_effect = _make_get_xcom(envs)

    airflow_errors, business_errors = _extract_errors(branch_tis, MagicMock())

    assert airflow_errors == [("s_A", "failed at run_job")]
    assert business_errors == []


@patch("dags.impl.processor_impl.get_xcom")
def test_extract_errors_intentional_skip(mock_get_xcom: MagicMock) -> None:
    """All tasks skipped without XCom (e.g. skip_quanting) → no error."""
    branch_tis = _make_branch_tis_by_index(
        [
            (
                0,
                [
                    ("run_job", "skipped"),
                    ("check_job_result", "skipped"),
                    ("store_metrics", "skipped"),
                ],
            ),
        ]
    )
    envs = [{QuantingEnv.SETTINGS_NAME: "s_A"}]
    mock_get_xcom.side_effect = _make_get_xcom(envs)

    airflow_errors, business_errors = _extract_errors(branch_tis, MagicMock())

    assert airflow_errors == []
    assert business_errors == []


@patch("dags.impl.processor_impl.get_xcom")
def test_extract_errors_multiple_branches_mixed(mock_get_xcom: MagicMock) -> None:
    """Multiple branches: one airflow failure, one business error, one success."""
    branch_tis = _make_branch_tis_by_index(
        [
            (0, [("run_job", "success"), ("store_metrics", "success")]),
            (1, [("run_job", "failed"), ("store_metrics", "upstream_failed")]),
            (2, [("check_job_result", "skipped"), ("store_metrics", "skipped")]),
        ]
    )
    envs = [
        {QuantingEnv.SETTINGS_NAME: "s_A"},
        {QuantingEnv.SETTINGS_NAME: "s_B"},
        {QuantingEnv.SETTINGS_NAME: "s_C"},
    ]
    mock_get_xcom.side_effect = _make_get_xcom(
        envs, branch_errors={1: None, 2: "TIMEOUT"}
    )

    airflow_errors, business_errors = _extract_errors(branch_tis, MagicMock())

    assert airflow_errors == [("s_B", "failed at run_job")]
    assert business_errors == [("s_C", "TIMEOUT")]
