"""Unit tests for the monitoring main loop."""

from unittest.mock import Mock, patch

import pytest
from pymongo.errors import ServerSelectionTimeoutError

from monitoring.main import main


class _StopLoopError(Exception):
    """Raised to leave the endless main loop."""


def _sent_keys(mock_alert_manager_cls: Mock) -> list[str]:
    return [
        c.args[0] for c in mock_alert_manager_cls.return_value.send_once.call_args_list
    ]


@pytest.mark.parametrize("is_active", [True, False])
@patch("monitoring.main.sleep", side_effect=_StopLoopError)
@patch("monitoring.main.connect_db")
@patch("monitoring.main.ActiveInstanceElection")
@patch("monitoring.main.AlertManager")
def test_main_runs_db_backed_checks_only_when_active(
    mock_alert_manager_cls: Mock,
    mock_election_cls: Mock,
    mock_connect_db: Mock,
    mock_sleep: Mock,
    *,
    is_active: bool,
) -> None:
    """Every instance runs the DB-free checks, only the active one the DB-backed checks."""
    # given
    mock_election_cls.return_value.try_become_active.return_value = is_active

    # when
    with pytest.raises(_StopLoopError):
        main()

    # then
    manager = mock_alert_manager_cls.return_value
    manager.check_db_free_alerts.assert_called_once()
    mock_connect_db.assert_called_once_with(raise_on_error=True)
    assert manager.check_for_issues.called is is_active
    mock_election_cls.return_value.try_become_active.assert_called_once()
    assert _sent_keys(mock_alert_manager_cls) == ["startup"]
    mock_sleep.assert_called_once()


@patch("monitoring.main.sleep", side_effect=_StopLoopError)
@patch("monitoring.main.connect_db", side_effect=ServerSelectionTimeoutError("no db"))
@patch("monitoring.main.ActiveInstanceElection")
@patch("monitoring.main.AlertManager")
def test_main_skips_check_when_db_connection_fails(
    mock_alert_manager_cls: Mock,
    mock_election_cls: Mock,  # noqa: ARG001
    mock_connect_db: Mock,  # noqa: ARG001
    mock_sleep: Mock,  # noqa: ARG001
) -> None:
    """A failed DB connection sends one alert and skips the DB-backed checks."""
    # when
    with pytest.raises(_StopLoopError):
        main()

    # then
    manager = mock_alert_manager_cls.return_value
    manager.check_db_free_alerts.assert_called_once()
    manager.check_for_issues.assert_not_called()
    assert _sent_keys(mock_alert_manager_cls) == ["startup", "db_connection_error"]


@patch("monitoring.main.sleep", side_effect=_StopLoopError)
@patch("monitoring.main.connect_db")
@patch("monitoring.main.ActiveInstanceElection")
@patch("monitoring.main.AlertManager")
def test_main_reports_exceptions_during_checks(
    mock_alert_manager_cls: Mock,
    mock_election_cls: Mock,  # noqa: ARG001
    mock_connect_db: Mock,  # noqa: ARG001
    mock_sleep: Mock,  # noqa: ARG001
) -> None:
    """An exception in the checks is reported and the loop goes on."""
    # given
    mock_alert_manager_cls.return_value.check_for_issues.side_effect = RuntimeError(
        "bug"
    )

    # when
    with pytest.raises(_StopLoopError):
        main()

    # then
    assert _sent_keys(mock_alert_manager_cls) == ["startup"]
    mock_alert_manager_cls.return_value.report_exception.assert_called_once()


@patch("monitoring.main.sleep", side_effect=_StopLoopError)
@patch("monitoring.main.connect_db")
@patch("monitoring.main.ActiveInstanceElection")
@patch("monitoring.main.AlertManager")
def test_main_runs_db_checks_when_a_db_free_check_fails(
    mock_alert_manager_cls: Mock,
    mock_election_cls: Mock,  # noqa: ARG001
    mock_connect_db: Mock,  # noqa: ARG001
    mock_sleep: Mock,  # noqa: ARG001
) -> None:
    """A failing DB-free check is reported and does not stop the DB-backed checks."""
    # given
    manager = mock_alert_manager_cls.return_value
    manager.check_db_free_alerts.side_effect = ValueError("bug")

    # when
    with pytest.raises(_StopLoopError):
        main()

    # then
    assert _sent_keys(mock_alert_manager_cls) == ["startup"]
    manager.report_exception.assert_called_once()
    manager.check_for_issues.assert_called_once()
