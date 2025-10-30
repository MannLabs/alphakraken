"""Unit tests for handler_impl.py."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.keys import AcquisitionMonitorErrors, DagContext, DagParams, OpArgs
from common.settings import _INSTRUMENTS
from dags.impl.handler_impl import (
    _count_special_characters,
    _handle_file_copying,
    _verify_copied_files,
    compute_checksum,
    copy_raw_file,
    decide_processing,
    start_acquisition_processor,
    start_file_mover,
)

from shared.db.models import RawFileStatus


@pytest.mark.parametrize(
    "file_info",
    [
        {"test_file.raw": (1000, "some_hash")},
        {},
    ],
)
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.RawFileWrapperFactory")
@patch("dags.impl.handler_impl.get_file_size")
@patch("dags.impl.handler_impl.get_file_hash")
@patch("dags.impl.handler_impl.update_raw_file")
@patch("dags.impl.handler_impl.put_xcom")
def test_compute_checksum_one_file(  # noqa: PLR0913
    mock_put_xcom: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_get_file_size: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    file_info: dict[str, tuple[float, str]],
) -> None:
    """Test compute_checksum calls update with correct arguments for one file (e.g. Thermo)."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
    }
    mock_raw_file = MagicMock()
    mock_raw_file.file_info = file_info
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_file_size.return_value = 1000
    mock_get_file_hash.return_value = "some_hash"
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_copy.return_value = {
        Path("/path/to/instrument/test_file.raw"): Path("/path/to/backup/test_file.raw")
    }
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.target_folder_path = Path(
        "/path/to/backup/"
    )

    mock_main_file_path = MagicMock()
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.main_file_path.return_value = mock_main_file_path

    # when
    continue_downstream_tasks = compute_checksum(ti, **kwargs)

    # then
    assert continue_downstream_tasks
    mock_update_raw_file.assert_has_calls(
        [
            call("test_file.raw", new_status=RawFileStatus.CHECKSUMMING),
            call(
                "test_file.raw",
                new_status=RawFileStatus.CHECKSUMMING_DONE,
                size=1000,
                file_info={
                    "test_file.raw": (
                        1000,
                        "some_hash",
                    )
                },
            ),
        ]
    )
    mock_put_xcom.assert_has_calls(
        [
            call(
                ti,
                "files_size_and_hashsum",
                {"/path/to/instrument/test_file.raw": (1000, "some_hash")},
            ),
            call(
                ti,
                "files_dst_paths",
                {"/path/to/instrument/test_file.raw": "/path/to/backup/test_file.raw"},
            ),
        ]
    )


@pytest.mark.parametrize(
    "file_info",
    [
        {
            "test_file.wiff": (1000, "some_hash"),
            "test_file.wiff2": (
                2000,
                "some_other_hash",
            ),
        },
        {},
    ],
)
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.RawFileWrapperFactory")
@patch("dags.impl.handler_impl.get_file_size")
@patch("dags.impl.handler_impl.get_file_hash")
@patch("dags.impl.handler_impl.update_raw_file")
@patch("dags.impl.handler_impl.put_xcom")
def test_compute_checksum_multiple_files(  # noqa: PLR0913
    mock_put_xcom: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_get_file_size: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    file_info: dict[str, tuple[float, str]],
) -> None:
    """Test compute_checksum calls update with correct arguments for multiple files (e.g. Sciex)."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.wiff"},
    }
    mock_raw_file = MagicMock()
    mock_raw_file.file_info = file_info
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_file_size.side_effect = [1000, 2000]
    mock_get_file_hash.side_effect = ["some_hash", "some_other_hash"]
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_copy.return_value = {
        Path("/path/to/instrument/test_file.wiff"): Path(
            "/path/to/backup/test_file.wiff"
        ),
        Path("/path/to/instrument/test_file.wiff2"): Path(
            "/path/to/backup/test_file.wiff2"
        ),
    }
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.target_folder_path = Path(
        "/path/to/backup/"
    )

    mock_main_file_path = MagicMock()
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.main_file_path.return_value = mock_main_file_path

    # when
    continue_downstream_tasks = compute_checksum(ti, **kwargs)

    # then
    assert continue_downstream_tasks
    mock_update_raw_file.assert_has_calls(
        [
            call("test_file.wiff", new_status=RawFileStatus.CHECKSUMMING),
            call(
                "test_file.wiff",
                new_status=RawFileStatus.CHECKSUMMING_DONE,
                size=3000,
                file_info={
                    "test_file.wiff": (
                        1000,
                        "some_hash",
                    ),
                    "test_file.wiff2": (
                        2000,
                        "some_other_hash",
                    ),
                },
            ),
        ]
    )
    mock_put_xcom.assert_has_calls(
        [
            call(
                ti,
                "files_size_and_hashsum",
                {
                    "/path/to/instrument/test_file.wiff": (1000, "some_hash"),
                    "/path/to/instrument/test_file.wiff2": (2000, "some_other_hash"),
                },
            ),
            call(
                ti,
                "files_dst_paths",
                {
                    "/path/to/instrument/test_file.wiff": "/path/to/backup/test_file.wiff",
                    "/path/to/instrument/test_file.wiff2": "/path/to/backup/test_file.wiff2",
                },
            ),
        ]
    )


@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.RawFileWrapperFactory")
@patch("dags.impl.handler_impl.get_file_size")
@patch("dags.impl.handler_impl.get_file_hash")
@patch("dags.impl.handler_impl.update_raw_file")
def test_compute_checksum_different_file_info(
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_file_hash: MagicMock,
    mock_get_file_size: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test compute_checksum raises if file_info mismatch."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
    }
    mock_raw_file = MagicMock()
    mock_raw_file.file_info = {"test_file.raw": (1001, "some_other_hash")}
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_file_size.return_value = 1000
    mock_get_file_hash.return_value = "some_hash"
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_copy.return_value = {
        Path("/path/to/instrument/test_file.raw"): Path("/path/to/backup/test_file.raw")
    }
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.target_folder_path = Path(
        "/path/to/backup/"
    )

    mock_main_file_path = MagicMock()
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.main_file_path.return_value = mock_main_file_path

    # when
    with pytest.raises(
        AirflowFailException, match="File info mismatch for test_file.raw"
    ):
        compute_checksum(ti, **kwargs)


@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.RawFileWrapperFactory")
@patch("dags.impl.handler_impl.get_file_size")
@patch("dags.impl.handler_impl.get_file_hash")
@patch("dags.impl.handler_impl.update_raw_file")
@patch("dags.impl.handler_impl.get_airflow_variable", return_value="test_file.raw")
@patch("dags.impl.handler_impl.put_xcom")
def test_compute_checksum_different_file_info_overwrite(  # noqa: PLR0913
    mock_put_xcom: MagicMock,
    mock_get_airflow_variable: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_file_hash: MagicMock,
    mock_get_file_size: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
) -> None:
    """Test compute_checksum continues on file_info mismatch if airflow variable is set."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
    }
    mock_raw_file = MagicMock()
    mock_raw_file.id = "test_file.raw"
    mock_raw_file.file_info = {"test_file.raw": (1000, "some_other_hash")}
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_get_file_size.return_value = 1000
    mock_get_file_hash.return_value = "some_hash"
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.get_files_to_copy.return_value = {
        Path("/path/to/instrument/test_file.raw"): Path("/path/to/backup/test_file.raw")
    }
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.target_folder_path = Path(
        "/path/to/backup/"
    )

    mock_main_file_path = MagicMock()
    mock_raw_file_wrapper_factory.create_write_wrapper.return_value.main_file_path.return_value = mock_main_file_path

    # when
    continue_downstream_tasks = compute_checksum(ti, **kwargs)

    # then
    assert continue_downstream_tasks

    mock_get_airflow_variable.assert_called_once_with("checksum_overwrite_file_id", "")

    mock_update_raw_file.assert_has_calls(
        [
            call("test_file.raw", new_status=RawFileStatus.CHECKSUMMING),
            call(
                "test_file.raw",
                new_status=RawFileStatus.CHECKSUMMING_DONE,
                size=1000,
                file_info={
                    "test_file.raw": (
                        1000,
                        "some_hash",
                    )
                },
            ),
        ]
    )
    mock_put_xcom.assert_has_calls(
        [
            call(
                ti,
                "files_size_and_hashsum",
                {"/path/to/instrument/test_file.raw": (1000, "some_hash")},
            ),
            call(
                ti,
                "files_dst_paths",
                {"/path/to/instrument/test_file.raw": "/path/to/backup/test_file.raw"},
            ),
        ]
    )


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.update_raw_file")
def test_compute_checksum_file_got_renamed(
    mock_update_raw_file: MagicMock,
    mock_get_raw_file_by_id: MagicMock,  # noqa: ARG001
    mock_get_xcom: MagicMock,
) -> None:
    """Test compute_checksum calls correctly handles failed acquisitions due to file renaming."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
    }

    mock_get_xcom.return_value = [AcquisitionMonitorErrors.FILE_GOT_RENAMED]

    # when
    continue_downstream_tasks = compute_checksum(ti, **kwargs)

    # then
    assert not continue_downstream_tasks
    mock_update_raw_file.assert_has_calls(
        [
            call(
                "test_file.raw",
                new_status=RawFileStatus.ACQUISITION_FAILED,
                status_details=AcquisitionMonitorErrors.FILE_GOT_RENAMED,
                backup_status="skipped",
            )
        ]
    )


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch(
    "dags.impl.handler_impl.get_backup_base_path",
    return_value=Path("some_backup_folder"),
)
@patch("dags.impl.handler_impl._handle_file_copying")
@patch("dags.impl.handler_impl._verify_copied_files")
@patch("dags.impl.handler_impl.update_raw_file")
def test_copy_raw_file_calls_update_with_correct_args(  # noqa: PLR0913
    mock_update_raw_file: MagicMock,
    mock_verify_copied_files: MagicMock,
    mock_handle_file_copying: MagicMock,
    mock_get_backup_base_path: MagicMock,  # noqa: ARG001
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test copy_raw_file calls update with correct arguments."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
        "instrument_id": "instrument1",
    }
    src_path = "/path/to/instrument/test_file.raw"
    dst_path = "/opt/airflow/mounts/backup/test_file.raw"

    mock_get_xcom.side_effect = [
        {src_path: dst_path},
        {src_path: (1000, "some_hash")},
    ]

    mock_raw_file = MagicMock()
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_handle_file_copying.return_value = {Path(src_path): (1000, "some_hash")}

    # when
    copy_raw_file(ti, **kwargs)

    # then
    mock_handle_file_copying.assert_called_once_with(
        {Path(src_path): Path(dst_path)},
        {Path(src_path): (1000, "some_hash")},
        overwrite=False,
    )
    mock_update_raw_file.assert_has_calls(
        [
            call(
                "test_file.raw",
                new_status=RawFileStatus.COPYING,
                backup_base_path="some_backup_folder",
                backup_status="copying_in_progress",
            ),
            call(
                "test_file.raw",
                new_status=RawFileStatus.COPYING_DONE,
                backup_status="copying_done",
            ),
        ]
    )
    mock_verify_copied_files.assert_called_once_with(
        {Path(src_path): (1000, "some_hash")},
        {Path(src_path): Path(dst_path)},
        {Path(src_path): (1000, "some_hash")},
    )


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch(
    "dags.impl.handler_impl.get_backup_base_path",
    return_value=Path("some_backup_folder"),
)
@patch("dags.impl.handler_impl._handle_file_copying")
@patch("dags.impl.handler_impl._verify_copied_files")
@patch("dags.impl.handler_impl.update_raw_file")
def test_copy_raw_file_verify_fails(  # noqa: PLR0913
    mock_update_raw_file: MagicMock,
    mock_verify_copied_files: MagicMock,
    mock_handle_file_copying: MagicMock,  # noqa: ARG001
    mock_get_backup_base_path: MagicMock,  # noqa: ARG001
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test copy_raw_file calls update with correct arguments in case verification fails."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
        "instrument_id": "instrument1",
    }
    src_path = "/path/to/instrument/test_file.raw"
    dst_path = "/opt/airflow/mounts/backup/test_file.raw"

    mock_get_xcom.side_effect = [
        {src_path: dst_path},
        {src_path: (1000, "some_hash")},
    ]

    mock_raw_file = MagicMock()
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_verify_copied_files.side_effect = ValueError("File copy failed with errors")

    # when
    with pytest.raises(AirflowFailException, match="File copy failed with errors"):
        copy_raw_file(ti, **kwargs)

    # then
    mock_update_raw_file.assert_has_calls(
        [
            call(
                "test_file.raw",
                new_status=RawFileStatus.COPYING,
                backup_base_path="some_backup_folder",
                backup_status="copying_in_progress",
            ),
            call(
                "test_file.raw",
                backup_status="copying_failed",
            ),
        ]
    )


@patch("dags.impl.handler_impl.get_xcom")
@patch("dags.impl.handler_impl.get_raw_file_by_id")
@patch("dags.impl.handler_impl.get_airflow_variable", return_value="test_file.raw")
@patch(
    "dags.impl.handler_impl.get_backup_base_path",
    return_value=Path("some_backup_folder"),
)
@patch("dags.impl.handler_impl._handle_file_copying")
@patch("dags.impl.handler_impl.update_raw_file")
def test_copy_raw_file_calls_update_with_correct_args_overwrite(  # noqa: PLR0913
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_handle_file_copying: MagicMock,
    mock_get_backup_base_path: MagicMock,  # noqa: ARG001
    mock_get_airflow_variable: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_get_xcom: MagicMock,
) -> None:
    """Test copy_raw_file calls update with correct arguments in case overwrite is requested."""
    ti = MagicMock()
    kwargs = {
        "params": {"raw_file_id": "test_file.raw"},
        "instrument_id": "instrument1",
    }
    mock_get_xcom.side_effect = [
        {
            "/path/to/instrument/test_file.raw": "/opt/airflow/mounts/backup/test_file.raw"
        },
        {"/path/to/instrument/test_file.raw": (1000, "some_hash")},
    ]

    mock_raw_file = MagicMock()
    mock_raw_file.id = "test_file.raw"
    mock_get_raw_file_by_id.return_value = mock_raw_file

    mock_handle_file_copying.return_value = {
        Path("/path/to/instrument/test_file.raw"): (1000, "some_hash")
    }

    # when
    copy_raw_file(ti, **kwargs)

    # then
    mock_handle_file_copying.assert_called_once_with(
        {
            Path("/path/to/instrument/test_file.raw"): Path(
                "/opt/airflow/mounts/backup/test_file.raw"
            )
        },
        {Path("/path/to/instrument/test_file.raw"): (1000, "some_hash")},
        overwrite=True,
    )
    mock_get_airflow_variable.assert_called_once_with("backup_overwrite_file_id", "")

    # not repeating the checks of test_copy_raw_file_calls_update_with_correct_args


def test_verify_copied_files_raises_exception_on_size_mismatch() -> None:
    """Test _verify_copied_files raises exception on size mismatch."""
    copied_files = {Path("file1"): (100, "hash1")}
    files_dst_paths = {Path("file1"): Path("dest1")}
    files_size_and_hashsum = {Path("file1"): (200, "hash1")}
    with pytest.raises(ValueError, match="File copy failed with errors"):
        # when
        _verify_copied_files(copied_files, files_dst_paths, files_size_and_hashsum)


def test_verify_copied_files_raises_exception_on_hash_mismatch() -> None:
    """Test _verify_copied_files raises exception on hash mismatch."""
    copied_files = {Path("file1"): (100, "hash1")}
    files_dst_paths = {Path("file1"): Path("dest1")}
    files_size_and_hashsum = {Path("file1"): (100, "hash2")}
    with pytest.raises(ValueError, match="File copy failed with errors"):
        # when
        _verify_copied_files(copied_files, files_dst_paths, files_size_and_hashsum)


def test_verify_copied_files_raises_exception_on_length_mismatch() -> None:
    """Test _verify_copied_files raises exception on length mismatch."""
    copied_files = {Path("file1"): (100, "hash1")}
    files_dst_paths = {Path("file1"): Path("dest1")}
    files_size_and_hashsum = {
        Path("file1"): (100, "hash1"),
        Path("file2"): (200, "hash2"),
    }
    with pytest.raises(ValueError, match="File copy failed with errors"):
        # when
        _verify_copied_files(copied_files, files_dst_paths, files_size_and_hashsum)


def test_verify_copied_files_succeeds_when_all_files_match() -> None:
    """Test _verify_copied_files succeeds when all files match."""
    copied_files = {Path("file1"): (100, "hash1"), Path("file2"): (200, "hash2")}
    files_dst_paths = {Path("file1"): Path("dest1"), Path("file2"): Path("dest2")}
    files_size_and_hashsum = {
        Path("file1"): (100, "hash1"),
        Path("file2"): (200, "hash2"),
    }
    # when
    _verify_copied_files(copied_files, files_dst_paths, files_size_and_hashsum)
    # no exception => ok


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
        "file_mover.instrument1",
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


@patch("dags.impl.handler_impl.get_xcom", return_value=[])
@patch(
    "dags.impl.handler_impl.get_raw_file_by_id",
    return_value=MagicMock(original_name="some_file.raw"),
)
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


@patch(
    "dags.impl.handler_impl.get_xcom",
    return_value=[AcquisitionMonitorErrors.MAIN_FILE_MISSING],
)
@patch("dags.impl.handler_impl.get_raw_file_by_id", return_value=MagicMock())
@patch("dags.impl.handler_impl.update_raw_file")
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
def test_decide_processing_returns_false_if_acquisition_errors_present(
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_update_raw_file: MagicMock,
    mock_get_raw_file_by_id: MagicMock,  # noqa:ARG001
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
        status_details=AcquisitionMonitorErrors.MAIN_FILE_MISSING,
    )


@patch("dags.impl.handler_impl.get_xcom", return_value=[])
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


@patch("dags.impl.handler_impl.get_xcom", return_value=[])
@patch("dags.impl.handler_impl.get_raw_file_by_id", return_value=[])
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=True)
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_skip_quanting_is_set(
    mock_update_raw_file: MagicMock,
    mock_get_instrument_settings: MagicMock,
    mock_get_raw_file_by_id: MagicMock,  # noqa:ARG001
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


@patch("dags.impl.handler_impl.get_xcom", return_value=[])
@patch("dags.impl.handler_impl.get_raw_file_by_id", return_value=[])
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_dda(
    mock_update_raw_file: MagicMock,
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_get_raw_file_by_id: MagicMock,  # noqa:ARG001
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


@patch("dags.impl.handler_impl.get_xcom", return_value=[])
@patch("dags.impl.handler_impl.get_raw_file_by_id", return_value=[])
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
@patch("dags.impl.handler_impl._count_special_characters", return_value=1)
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_special_characters(
    mock_update_raw_file: MagicMock,
    mock_count_special_characters: MagicMock,  # noqa:ARG001
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_get_raw_file_by_id: MagicMock,  # noqa:ARG001
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


@patch("dags.impl.handler_impl.get_xcom", return_value=[])
@patch("dags.impl.handler_impl.get_raw_file_by_id", return_value=MagicMock())
@patch("dags.impl.handler_impl.get_instrument_settings", return_value=False)
@patch("dags.impl.handler_impl._count_special_characters", return_value=0)
@patch("dags.impl.handler_impl.ThermoRawFileMonitorWrapper", return_value=MagicMock())
@patch("dags.impl.handler_impl.update_raw_file")
def test_decide_processing_returns_false_if_corrupted_file(  # noqa: PLR0913
    mock_update_raw_file: MagicMock,
    mock_raw_file_monitor_wrapper: MagicMock,
    mock_count_special_characters: MagicMock,  # noqa:ARG001
    mock_get_instrument_settings: MagicMock,  # noqa:ARG001
    mock_get_raw_file_by_id: MagicMock,  # noqa:ARG001
    mock_get_xcom: MagicMock,  # noqa:ARG001
) -> None:
    """Test decide_processing returns False if the raw file name indicates a failed acquisition."""
    ti = MagicMock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }

    mock_raw_file_monitor_wrapper.return_value.is_corrupted_file_name.return_value = (
        True
    )

    # when
    assert decide_processing(ti, **kwargs) is False
    mock_update_raw_file.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ACQUISITION_FAILED,
        status_details="File name indicates failed acquisition.",
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


@patch("dags.impl.handler_impl._decide_if_copy_required")
@patch("dags.impl.handler_impl.copy_file")
@patch("dags.impl.handler_impl.get_file_size")
def test_handle_file_copying_success(
    mock_get_file_size: MagicMock,
    mock_copy_file: MagicMock,
    mock_decide_if_copy_required: MagicMock,
) -> None:
    """Test _handle_file_copying successfully copies files when copy is required."""
    # given
    src_path = Path("/src/file1.raw")
    dst_path = Path("/dst/file1.raw")
    files_dst_paths = {src_path: dst_path}
    files_size_and_hashsum = {src_path: (1000.0, "src_hash")}

    mock_decide_if_copy_required.return_value = True
    mock_copy_file.return_value = (1000.0, "dst_hash")

    # when
    result = _handle_file_copying(
        files_dst_paths, files_size_and_hashsum, overwrite=False
    )

    # then
    assert result == {src_path: (1000.0, "dst_hash")}
    mock_decide_if_copy_required.assert_called_once_with(
        src_path, dst_path, "src_hash", overwrite=False
    )
    mock_copy_file.assert_called_once_with(src_path, dst_path, "src_hash")
    mock_get_file_size.assert_not_called()


@patch("dags.impl.handler_impl._decide_if_copy_required")
@patch("dags.impl.handler_impl.copy_file")
@patch("dags.impl.handler_impl.get_file_size")
def test_handle_file_copying_copy_not_required(
    mock_get_file_size: MagicMock,
    mock_copy_file: MagicMock,
    mock_decide_if_copy_required: MagicMock,
) -> None:
    """Test _handle_file_copying when copy is not required (file already exists and is identical)."""
    # given
    src_path = Path("/src/file1.raw")
    dst_path = Path("/dst/file1.raw")
    files_dst_paths = {src_path: dst_path}
    files_size_and_hashsum = {src_path: (1000.0, "src_hash")}

    mock_decide_if_copy_required.return_value = False
    mock_get_file_size.return_value = 1000.0

    # when
    result = _handle_file_copying(
        files_dst_paths, files_size_and_hashsum, overwrite=False
    )

    # then
    assert result == {src_path: (1000.0, "src_hash")}
    mock_decide_if_copy_required.assert_called_once_with(
        src_path, dst_path, "src_hash", overwrite=False
    )
    mock_copy_file.assert_not_called()
    mock_get_file_size.assert_called_once_with(dst_path)


@patch("dags.impl.handler_impl._decide_if_copy_required")
@patch("dags.impl.handler_impl.move_existing_file")
@patch("dags.impl.handler_impl.copy_file")
def test_handle_file_copying_overwrite_move(
    mock_copy_file: MagicMock,
    mock_move_existing_file: MagicMock,
    mock_decide_if_copy_required: MagicMock,
) -> None:
    """Test _handle_file_copying moves file before overwriting."""
    # given
    src_path = Path("/src/file1.raw")
    dst_path = MagicMock()
    dst_path.exists.return_value = True

    files_dst_paths = {src_path: dst_path}
    files_size_and_hashsum = {src_path: (1000.0, "src_hash")}

    mock_decide_if_copy_required.return_value = True
    mock_copy_file.return_value = (1000.0, "dst_hash")

    # when
    result = _handle_file_copying(
        files_dst_paths, files_size_and_hashsum, overwrite=True
    )

    # then
    assert result == {src_path: (1000.0, "dst_hash")}

    mock_decide_if_copy_required.assert_called_once_with(
        src_path, dst_path, "src_hash", overwrite=True
    )

    mock_move_existing_file.assert_called_once_with(dst_path)


@patch("dags.impl.handler_impl._decide_if_copy_required")
@patch("dags.impl.handler_impl.copy_file")
@patch("dags.impl.handler_impl.get_file_size")
def test_handle_file_copying_multiple_files(
    mock_get_file_size: MagicMock,
    mock_copy_file: MagicMock,
    mock_decide_if_copy_required: MagicMock,
) -> None:
    """Test _handle_file_copying with multiple files having different copy requirements."""
    # given
    src_path1 = Path("/src/file1.raw")
    dst_path1 = Path("/dst/file1.raw")
    src_path2 = Path("/src/file2.wiff")
    dst_path2 = Path("/dst/file2.wiff")

    files_dst_paths = {src_path1: dst_path1, src_path2: dst_path2}
    files_size_and_hashsum = {
        src_path1: (1000.0, "hash1"),
        src_path2: (2000.0, "hash2"),
    }

    # file1 needs copying, file2 doesn't
    mock_decide_if_copy_required.side_effect = [True, False]
    mock_copy_file.return_value = (1000.0, "copied_hash1")
    mock_get_file_size.return_value = 2000.0

    # when
    result = _handle_file_copying(
        files_dst_paths, files_size_and_hashsum, overwrite=False
    )

    # then
    assert result == {src_path1: (1000.0, "copied_hash1"), src_path2: (2000.0, "hash2")}
    mock_decide_if_copy_required.assert_has_calls(
        [
            call(src_path1, dst_path1, "hash1", overwrite=False),
            call(src_path2, dst_path2, "hash2", overwrite=False),
        ]
    )
    mock_copy_file.assert_called_once_with(src_path1, dst_path1, "hash1")
    mock_get_file_size.assert_called_once_with(dst_path2)


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
