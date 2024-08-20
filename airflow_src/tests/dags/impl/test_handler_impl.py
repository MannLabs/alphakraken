"""Unit tests for handler_impl.py."""

import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

from common.keys import DagContext, DagParams, OpArgs
from dags.impl.handler_impl import (
    copy_raw_file,
    decide_processing,
    start_acquisition_processor,
    start_file_mover,
)
from db.models import RawFileStatus


@patch.dict(
    os.environ,
    {"POOL_BASE_PATH": "/path/to/pool", "BACKUP_POOL_FOLDER": "some_backup_folder"},
)
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.copy_file")
@patch("dags.impl.handler_impl.RawFileWrapperFactory")
@patch("dags.impl.handler_impl.get_file_size")
@patch("dags.impl.handler_impl.update_raw_file")
def test_copy_raw_file_calls_update_with_correct_args(
    mock_update_raw_file: MagicMock,
    mock_get_file_size: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_copy_file: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test copy_raw_file calls update with correct arguments."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
    }
    mock_raw_file = MagicMock()
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_file_size.return_value = 1000
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_copy.return_value = {
        Path("/path/to/instrument/test_file.raw"): Path(
            "/opt/airflow/mounts/backup/test_file.raw"
        )
    }
    mock_copy_file.return_value = (1001, "some_hash")

    mock_file_path_to_calculate_size = MagicMock()
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.file_path_to_calculate_size.return_value = mock_file_path_to_calculate_size

    # when
    copy_raw_file(ti, **kwargs)

    # then
    mock_copy_file.assert_called_once_with(
        Path("/path/to/instrument/test_file.raw"),
        Path("/opt/airflow/mounts/backup/test_file.raw"),
    )
    mock_update_raw_file.assert_has_calls(
        [
            call("test_file.raw", new_status=RawFileStatus.COPYING),
            call(
                "test_file.raw",
                new_status=RawFileStatus.COPYING_DONE,
                size=1000,
                file_info={
                    "test_file.raw": (
                        1001,
                        "some_hash",
                    )
                },
                backup_base_path="/path/to/pool/some_backup_folder",
            ),
        ]
    )
    mock_get_file_size.assert_called_once_with(mock_file_path_to_calculate_size, -1)


@patch("dags.impl.handler_impl.trigger_dag_run")
def test_start_file_mover(mock_trigger_dag_run: MagicMock) -> None:
    """Test start_file_mover."""
    ti = Mock()

    # when
    start_file_mover(
        ti,
        **{
            DagContext.PARAMS: {DagParams.RAW_FILE_ID: "file1.raw"},
        },
    )

    mock_trigger_dag_run.assert_called_once_with(
        "file_mover",
        {
            DagParams.RAW_FILE_ID: "file1.raw",
        },
    )


@patch("dags.impl.handler_impl.get_xcom", return_value=None)
def test_decide_processing_returns_true_if_no_errors(
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns True if no errors are present."""
    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: 1}}
    assert decide_processing(ti, **kwargs) is True


@patch("dags.impl.handler_impl.get_xcom", return_value=["error1"])
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_errors_present(
    mock_update_raw_file: MagicMock,
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns False if errors are present."""
    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: 1}}
    assert decide_processing(ti, **kwargs) is False

    mock_update_raw_file.assert_called_once_with(
        1, new_status=RawFileStatus.ACQUISITION_FAILED, status_details="error1"
    )


@patch("dags.impl.handler_impl.trigger_dag_run")
@patch("dags.impl.handler_impl.update_raw_file")
def test_start_acquisition_processor_with_single_file(
    mock_update_raw_file: MagicMock,
    mock_trigger_dag_run: MagicMock,
) -> None:
    """Test start_acquisition_processor with a single file."""
    # given
    raw_file_names = {"file1.raw": ("PID1", True)}
    ti = Mock()

    # when
    start_acquisition_processor(
        ti,
        **{
            OpArgs.INSTRUMENT_ID: "instrument1",
            DagContext.PARAMS: {DagParams.RAW_FILE_ID: "file1.raw"},
        },
    )

    # then
    assert mock_trigger_dag_run.call_count == 1  # no magic numbers
    for n, call_ in enumerate(mock_trigger_dag_run.call_args_list):
        assert call_.args[0] == ("acquisition_processor.instrument1")
        assert {
            "raw_file_id": list(raw_file_names.keys())[n],
        } == call_.args[1]
    mock_update_raw_file.assert_called_once_with(
        "file1.raw", new_status=RawFileStatus.QUEUED_FOR_QUANTING
    )
