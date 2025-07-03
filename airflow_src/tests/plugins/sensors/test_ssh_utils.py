"""Unit tests for the ssh_utils module."""

from unittest.mock import MagicMock, call, patch

import pytest
from airflow.exceptions import AirflowFailException
from plugins.sensors.ssh_utils import ssh_execute


@patch("plugins.sensors.ssh_utils.get_cluster_ssh_hook")
def test_ssh_execute_returns_decoded_output(
    mock_get_cluster_ssh_hook: MagicMock,
) -> None:
    """Test that the ssh_execute function returns the decoded output of the command."""
    # given
    ssh_hook = MagicMock()
    ssh_hook.exec_ssh_client_command.return_value = (0, b"command output", b"")
    mock_get_cluster_ssh_hook.return_value = ssh_hook

    # when
    result = ssh_execute("my_command")

    # then
    assert result == "command output"
    ssh_hook.exec_ssh_client_command.assert_called_once_with(
        ssh_hook.get_conn.return_value,
        "my_command",
        timeout=60,
        get_pty=False,
        environment={},
    )


@patch("plugins.sensors.ssh_utils.get_cluster_ssh_hook")
@patch("plugins.sensors.ssh_utils.sleep")
def test_ssh_execute_multiple_tries(
    mock_sleep: MagicMock, mock_get_cluster_ssh_hook: MagicMock
) -> None:
    """ssh_execute returns the decoded output of the command SSH command returns 254 for some time."""
    # given
    ssh_hook = MagicMock()

    bad_return1 = (254, b"command output", b"")
    bad_return2 = (0, b"", b"")
    ssh_hook.exec_ssh_client_command.side_effect = (
        [bad_return1] * 2 + [bad_return2] * 2 + [(0, b"command output", b"")]
    )
    mock_get_cluster_ssh_hook.return_value = ssh_hook

    # when
    result = ssh_execute("my_command")

    # then
    assert result == "command output"
    assert mock_sleep.call_count == 5  # noqa: PLR2004
    assert ssh_hook.exec_ssh_client_command.call_count == 5  # noqa: PLR2004

    expected_call = call(
        ssh_hook.get_conn.return_value,
        "my_command",
        timeout=60,
        get_pty=False,
        environment={},
    )
    ssh_hook.exec_ssh_client_command.assert_has_calls(
        [expected_call, expected_call, expected_call, expected_call]
    )


@patch("plugins.sensors.ssh_utils.get_cluster_ssh_hook")
@patch("plugins.sensors.ssh_utils.sleep")
def test_ssh_execute_too_many_tries(
    mock_sleep: MagicMock, mock_get_cluster_ssh_hook: MagicMock
) -> None:
    """ssh_execute returns the decoded output of the command SSH command returns 254 for some time."""
    # given
    ssh_hook = MagicMock()
    bad_return = (254, b"command output", b"")
    ssh_hook.exec_ssh_client_command.side_effect = [bad_return] * 31
    mock_get_cluster_ssh_hook.return_value = ssh_hook

    # when
    with pytest.raises(AirflowFailException):
        ssh_execute("my_command")

    assert mock_sleep.call_count == 30  # noqa: PLR2004
    assert ssh_hook.exec_ssh_client_command.call_count == 30  # noqa: PLR2004
