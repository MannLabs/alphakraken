"""Unit tests for handler_impl.py."""

import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from common.keys import DagContext, DagParams, OpArgs
from common.settings import _INSTRUMENTS
from dags.impl.handler_impl import (
    _count_special_characters,
    copy_raw_file,
    decide_processing,
    start_acquisition_processor,
    start_file_mover,
)

from shared.db.models import RawFileStatus


@patch.dict(
    os.environ,
    {"BACKUP_BASE_PATH": "some_backup_folder"},
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
        overwrite=False,
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
                backup_base_path="some_backup_folder",
            ),
        ]
    )
    mock_get_file_size.assert_called_once_with(mock_file_path_to_calculate_size, -1)


@patch.dict(
    os.environ,
    {"BACKUP_BASE_PATH": "some_backup_folder"},
)
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.copy_file")
@patch("dags.impl.handler_impl.RawFileWrapperFactory")
@patch("dags.impl.handler_impl.get_airflow_variable", return_value="test_file.raw")
@patch("dags.impl.handler_impl.get_file_size")
@patch("dags.impl.handler_impl.update_raw_file")
def test_copy_raw_file_calls_update_with_correct_args_overwrite(  # noqa: PLR0913
    mock_update_raw_file: MagicMock,  # noqa:ARG001
    mock_get_file_size: MagicMock,  # noqa:ARG001
    mock_get_airflow_variable: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_copy_file: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test copy_raw_file calls update with correct arguments in case overwrite is requested."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
    }
    mock_raw_file = MagicMock()
    mock_raw_file.id = "test_file.raw"
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_copy.return_value = {
        Path("/path/to/instrument/test_file.raw"): Path(
            "/opt/airflow/mounts/backup/test_file.raw"
        )
    }
    mock_copy_file.return_value = (1001, "some_hash")

    # when
    copy_raw_file(ti, **kwargs)

    # then
    mock_copy_file.assert_called_once_with(
        Path("/path/to/instrument/test_file.raw"),
        Path("/opt/airflow/mounts/backup/test_file.raw"),
        overwrite=True,
    )

    mock_get_airflow_variable.assert_called_once_with("backup_overwrite_file_id", "")

    # not repeating the checks of test_copy_raw_file_calls_update_with_correct_args


@patch.dict(_INSTRUMENTS, {"instrument1": {"file_move_delay_m": 1}})
@patch("dags.impl.handler_impl.trigger_dag_run")
def test_start_file_mover(mock_trigger_dag_run: MagicMock) -> None:
    """Test start_file_mover."""
    ti = Mock()

    # when
    start_file_mover(
        ti,
        **{
            DagContext.PARAMS: {
                DagParams.RAW_FILE_ID: "file1.raw",
            },
        },
        **{OpArgs.INSTRUMENT_ID: "instrument1"},
    )

    mock_trigger_dag_run.assert_called_once_with(
        "file_mover",
        {
            DagParams.RAW_FILE_ID: "file1.raw",
        },
        time_delay_minutes=1,
    )


@patch.dict(_INSTRUMENTS, {"instrument1": {"file_move_delay_m": -1}})
@patch("dags.impl.handler_impl.trigger_dag_run")
def test_start_file_mover_skipped(mock_trigger_dag_run: MagicMock) -> None:
    """Test start_file_mover skips moving if time delay < 1."""
    ti = Mock()

    # when
    start_file_mover(
        ti,
        **{
            DagContext.PARAMS: {
                DagParams.RAW_FILE_ID: "file1.raw",
            },
        },
        **{OpArgs.INSTRUMENT_ID: "instrument1"},
    )

    mock_trigger_dag_run.assert_not_called()


@patch("dags.impl.handler_impl.get_xcom", return_value=None)
@patch("dags.impl.handler_impl.get_raw_file_by_id", return_value=MagicMock())
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
def test_decide_processing_returns_true_if_no_errors(
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_get_raw_file_by_id: MagicMock,  # noqa:ARG001
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns True if no errors are present."""
    ti = MagicMock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }

    # when
    assert decide_processing(ti, **kwargs) is True


@patch("dags.impl.handler_impl.get_xcom", return_value=["error1"])
@patch("dags.impl.handler_impl.update_raw_file")
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
def test_decide_processing_returns_false_if_acquisition_errors_present(
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_update_raw_file: MagicMock,
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns False if acquisition errors are present."""
    ti = MagicMock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }

    # when
    assert decide_processing(ti, **kwargs) is False

    mock_update_raw_file.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ACQUISITION_FAILED,
        status_details="error1",
    )


@patch("dags.impl.handler_impl.get_xcom", return_value=None)
@patch("dags.impl.handler_impl.get_raw_file_by_id", return_value=MagicMock(size=0))
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_file_size_zero(
    mock_update_raw_file: MagicMock,
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_get_raw_file_by_id: MagicMock,  # noqa:ARG001
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns False if file name contains 'dda'."""
    ti = MagicMock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }

    # when
    assert decide_processing(ti, **kwargs) is False
    mock_update_raw_file.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ACQUISITION_FAILED,
        status_details="File size is zero.",
    )


@patch("dags.impl.handler_impl.get_xcom", return_value=None)
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=True)
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_skip_quanting_is_set(
    mock_update_raw_file: MagicMock,
    mock_get_instrument_settings: MagicMock,
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns False if instrument settings has skip_quanting set."""
    ti = MagicMock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }

    # when
    assert decide_processing(ti, **kwargs) is False
    mock_get_instrument_settings.assert_called_once_with("instrument1", "skip_quanting")
    mock_update_raw_file.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.DONE_NOT_QUANTED,
        status_details="Quanting disabled for this instrument.",
    )


@patch("dags.impl.handler_impl.get_xcom", return_value=None)
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_dda(
    mock_update_raw_file: MagicMock,
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns False if file name contains 'dda'."""
    ti = MagicMock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_dda_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }

    # when
    assert decide_processing(ti, **kwargs) is False
    mock_update_raw_file.assert_called_once_with(
        "some_dda_file.raw",
        new_status=RawFileStatus.DONE_NOT_QUANTED,
        status_details="Filename contains 'dda'.",
    )


@patch("dags.impl.handler_impl.get_xcom", return_value=None)
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
@patch("dags.impl.handler_impl._count_special_characters", return_value=1)
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_special_characters(
    mock_update_raw_file: MagicMock,
    mock_count_special_characters: MagicMock,  # noqa:ARG001
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns False if file name contains special characters."""
    ti = MagicMock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }

    # when
    assert decide_processing(ti, **kwargs) is False
    mock_update_raw_file.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.DONE_NOT_QUANTED,
        status_details="Filename contains special characters.",
    )


@pytest.mark.parametrize(
    ("raw_file_name", "has_special_chars"),
    [
        ("0123456789_abcedfghijklmnopqrstuvwxyz+-.raw", False),
        ("0123456789_ABCEDFGHIJKLMNOPQRSTUVWXYZ+-.raw", False),
        ('"\\/`~!@#$%^&*()={}[]:;?<>, µ', True),  # all bad characters here
    ],
)
def test_count_special_characters(
    raw_file_name: str,
    has_special_chars: bool,  # noqa: FBT001
) -> None:
    """Test _count_special_characters returns correctly for several conditions."""
    if not has_special_chars:
        assert _count_special_characters(raw_file_name) == 0
    else:
        assert _count_special_characters(raw_file_name) == len(raw_file_name)


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
        assert call_.args[1] == {
            "raw_file_id": list(raw_file_names.keys())[n],
        }
    mock_update_raw_file.assert_called_once_with(
        "file1.raw", new_status=RawFileStatus.QUEUED_FOR_QUANTING
    )
