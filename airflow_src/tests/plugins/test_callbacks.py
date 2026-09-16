"""Tests for the plugins.callbacks module."""

from unittest.mock import MagicMock, patch

import pytest
from impl.processor_impl import QuantingFailedException
from plugins.callbacks import on_failure_callback
from plugins.s3.s3_utils import S3UploadFailedException

from shared.db.models import BackupStatus, RawFileStatus


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_other_exception(mock_update: MagicMock) -> None:
    """Test that on_failure_callback updates the raw file status to error."""
    ex = Exception("Some error")
    context = {
        "task_instance": MagicMock(
            task_id="task1",
            dag_id="dag1.instrument1",
            xcom_pull=MagicMock(return_value=["some_file.raw"]),
        ),
        "exception": ex,
    }

    # when
    on_failure_callback(context)

    mock_update.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ERROR,
        status_details="[dag1.task1] Some error",
    )


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_s3_exception(mock_update: MagicMock) -> None:
    """Test that on_failure_callback updates the raw file status to error."""
    ex = S3UploadFailedException("Some error")
    context = {
        "task_instance": MagicMock(
            task_id="task1",
            dag_id="dag1.instrument1",
            xcom_pull=MagicMock(return_value=["some_file.raw"]),
        ),
        "exception": ex,
    }

    # when
    on_failure_callback(context)

    mock_update.assert_called_once_with(
        "some_file.raw",
        backup_status=BackupStatus.UPLOAD_FAILED,
    )


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_no_rawfile_in_xcom_but_dag_context(
    mock_update: MagicMock,
) -> None:
    """Test that on_failure_callback does update status when the raw file name is not in XCom but Dag context."""
    context = {
        "task_instance": MagicMock(
            task_id="task1",
            dag_id="dag1.instrument1",
            xcom_pull=MagicMock(side_effect=KeyError),
        ),
        "exception": Exception("Some error"),
        "params": {"raw_file_id": "some_file.raw"},
    }

    # when
    on_failure_callback(context)

    mock_update.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ERROR,
        status_details="[dag1.task1] Some error",
    )


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_rawfile_in_xcom_of_one_task(
    mock_update: MagicMock,
) -> None:
    """on_failure_callback takes the raw file id from XCom when exactly one task pushed it."""
    context = {
        "task_instance": MagicMock(
            task_id="task1",
            dag_id="dag1.instrument1",
            xcom_pull=MagicMock(return_value=[None, "some_file.raw", None]),
        ),
        "exception": Exception("Some error"),
    }

    # when
    on_failure_callback(context)

    mock_update.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ERROR,
        status_details="[dag1.task1] Some error",
    )


@pytest.mark.parametrize(
    "xcom_values",
    [
        [None, None],
        ["some_file.raw", "other_file.raw"],
    ],
)
@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_no_unique_rawfile_in_xcom_nor_dag(
    mock_update: MagicMock, xcom_values: list[str | None]
) -> None:
    """on_failure_callback raises when the raw file id is not in the Dag context and XCom holds no unique value."""
    context = {
        "task_instance": MagicMock(
            task_id="task1",
            dag_id="dag1",
            xcom_pull=MagicMock(return_value=xcom_values),
        ),
        "exception": Exception("Some error"),
    }

    # when
    with pytest.raises(ValueError, match="Expected exactly one raw file id"):
        on_failure_callback(context)

    mock_update.assert_not_called()


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_quanting_failed_exception(
    mock_update: MagicMock,
) -> None:
    """update_raw_file is not called when QuantingFailedException is raised, because the status is already set."""
    context = {
        "task_instance": MagicMock(
            task_id="task1",
            dag_id="dag1.instrument1",
            xcom_pull=MagicMock(return_value=["some_file.raw"]),
        ),
        "exception": QuantingFailedException("Quanting failed"),
    }

    # when
    on_failure_callback(context)

    mock_update.assert_not_called()
