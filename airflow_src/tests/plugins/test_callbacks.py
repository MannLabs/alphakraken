"""Tests for the plugins.callbacks module."""

from unittest.mock import MagicMock, patch

from db.models import RawFileStatus
from plugins.callbacks import on_failure_callback


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_other_exception(mock_update: MagicMock) -> None:
    """Test that on_failure_callback updates the raw file status to error."""
    ex = Exception("Some error")
    context = {
        "task_instance": MagicMock(
            task_id="task1",
            dag_id="dag1",
            xcom_pull=MagicMock(return_value="some_file.raw"),
        ),
        "exception": ex,
    }

    # when
    on_failure_callback(context)

    mock_update.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ERROR,
        status_details="[task1] Some error",
    )


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_no_rawfile_in_xcom_but_dag_context(
    mock_update: MagicMock,
) -> None:
    """Test that on_failure_callback does update status when the raw file name is not in XCom but Dag context."""
    context = {
        "task_instance": MagicMock(
            task_id="task1", dag_id="dag1", xcom_pull=MagicMock(side_effect=KeyError)
        ),
        "exception": Exception("Some error"),
        "params": {"raw_file_id": "some_file.raw"},
    }

    # when
    on_failure_callback(context)

    mock_update.assert_called_once_with(
        "some_file.raw",
        new_status=RawFileStatus.ERROR,
        status_details="[task1] Some error",
    )


@patch("plugins.callbacks.update_raw_file")
def test_on_failure_callback_with_no_rawfile_in_xcom_nor_dag(
    mock_update: MagicMock,
) -> None:
    """on_failure_callback does not update the status when the raw file name is not in XCom nor Dag context."""
    context = {
        "task_instance": MagicMock(
            task_id="task1", dag_id="dag1", xcom_pull=MagicMock(side_effect=KeyError)
        ),
        "exception": Exception("Some error"),
    }

    # when
    on_failure_callback(context)

    mock_update.assert_not_called()
