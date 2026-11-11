"""Tests for the streamlit utilities."""

from unittest.mock import MagicMock, patch

from service.utils import toast_result

from shared.domain.errors import ValidationError


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


@patch("service.utils.show_error_toast")
def test_toast_result_shows_single_validation_error_as_it_is(
    mock_show_error_toast: MagicMock,
) -> None:
    """One validation error needs no list."""
    with toast_result("done"):
        raise ValidationError(["name: too long"])

    mock_show_error_toast.assert_called_once_with("name: too long")


@patch("service.utils.show_error_toast")
def test_toast_result_lists_several_validation_errors(
    mock_show_error_toast: MagicMock,
) -> None:
    """Several validation errors are shown as a counted list."""
    with toast_result("done"):
        raise ValidationError(["name: too long", "software: Cannot contain '..'"])

    mock_show_error_toast.assert_called_once_with(
        "Found 2 input validation errors:\n- name: too long\n- software: Cannot contain '..'"
    )
