"""Unit tests for the monitoring main loop."""

from unittest.mock import Mock, patch

import pytest

from monitoring.main import main


class _StopLoopError(Exception):
    """Raised to leave the endless main loop."""


@patch("monitoring.main.sleep", side_effect=_StopLoopError)
@patch("monitoring.main.send_special_alert")
@patch("monitoring.main.connect_db", side_effect=RuntimeError("no db"))
@patch("monitoring.main.AlertManager")
def test_main_should_skip_check_when_db_connection_fails(
    mock_alert_manager_cls: Mock,
    mock_connect_db: Mock,  # noqa: ARG001
    mock_send_special_alert: Mock,
    mock_sleep: Mock,
) -> None:
    """A failed DB connection sends one alert and skips the check."""
    # when
    with pytest.raises(_StopLoopError):
        main()

    # then
    mock_alert_manager_cls.return_value.check_for_issues.assert_not_called()
    alert_names = [c.args[1] for c in mock_send_special_alert.call_args_list]
    assert alert_names == ["startup", "db_connection_error"]
    mock_sleep.assert_called_once()
