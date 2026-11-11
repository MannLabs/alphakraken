"""Tests for the streamlit utilities."""

from unittest.mock import MagicMock, patch

from service.utils import toast_result


@patch("service.utils.show_success_toast")
@patch("service.utils.show_error_toast")
def test_toast_result_shows_exception_as_error_toast(
    mock_show_error_toast: MagicMock, mock_show_success_toast: MagicMock
) -> None:
    """An exception in the block is shown as error toast and not raised."""
    with toast_result("done"):
        raise ValueError("some error")

    mock_show_error_toast.assert_called_once_with("some error")
    mock_show_success_toast.assert_not_called()


@patch("service.utils.show_success_toast")
@patch("service.utils.show_error_toast")
def test_toast_result_shows_success_toast(
    mock_show_error_toast: MagicMock, mock_show_success_toast: MagicMock
) -> None:
    """A block that does not raise is followed by the success toast."""
    with toast_result("done"):
        pass

    mock_show_success_toast.assert_called_once_with("done")
    mock_show_error_toast.assert_not_called()
