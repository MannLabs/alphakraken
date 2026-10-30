"""Unit tests for the ssh_utils module."""

import base64
import json
from unittest.mock import MagicMock, call, patch

import pytest
from airflow.sdk.exceptions import AirflowFailException, AirflowNotFoundException
from paramiko.ssh_exception import SSHException
from plugins.common.constants import EXIT_CODE_FILE_NAME, LAUNCHER_SCRIPT_STEM
from plugins.sensors.ssh_utils import (
    _decode_powershell_command,
    _get_fake_ssh_response,
    get_cluster_ssh_hook,
    ssh_execute,
)

SSH_PREFIX = "some_cluster_ssh"


def _encoded(script: str) -> str:
    """Get the powershell command a windows handler sends to run `script`."""
    payload = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
    return f"powershell -NoProfile -EncodedCommand {payload}"


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
    result = ssh_execute("my_command", SSH_PREFIX)

    # then
    assert result == "command output"
    mock_get_cluster_ssh_hook.assert_called_once_with(
        attempt_no=0, ssh_connection_id_prefix=SSH_PREFIX
    )
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
    bad_return3 = (0, b"command output", b"Batch job submission failed")
    ok_return = (0, b"command output", b"")
    ssh_hook.exec_ssh_client_command.side_effect = (
        [bad_return1] * 2 + [bad_return2] * 2 + [bad_return3] * 2 + [ok_return]
    )
    mock_get_cluster_ssh_hook.return_value = ssh_hook

    # when
    result = ssh_execute("my_command", SSH_PREFIX)

    # then
    assert result == "command output"
    assert mock_sleep.call_count == 7
    assert ssh_hook.exec_ssh_client_command.call_count == 7

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
        ssh_execute("my_command", SSH_PREFIX)

    assert mock_sleep.call_count == 30
    assert ssh_hook.exec_ssh_client_command.call_count == 30


@patch("plugins.sensors.ssh_utils.get_cluster_ssh_hook")
def test_ssh_execute_closes_client_after_success(
    mock_get_cluster_ssh_hook: MagicMock,
) -> None:
    """Test that the SSH client is closed after a successful command."""
    ssh_hook = MagicMock()
    ssh_hook.exec_ssh_client_command.return_value = (0, b"command output", b"")
    mock_get_cluster_ssh_hook.return_value = ssh_hook

    # when
    ssh_execute("my_command", SSH_PREFIX)

    ssh_hook.client.close.assert_called_once_with()


@patch("plugins.sensors.ssh_utils.get_cluster_ssh_hook")
@patch("plugins.sensors.ssh_utils.sleep")
def test_ssh_execute_closes_client_after_exception(
    mock_sleep: MagicMock,  # noqa:ARG001
    mock_get_cluster_ssh_hook: MagicMock,
) -> None:
    """Test that the SSH client is closed on every attempt, also on a failed one."""
    ssh_hook = MagicMock()
    ssh_hook.exec_ssh_client_command.side_effect = [
        SSHException("Timeout opening channel."),
        (0, b"command output", b""),
    ]
    mock_get_cluster_ssh_hook.return_value = ssh_hook

    # when
    ssh_execute("my_command", SSH_PREFIX)

    assert ssh_hook.client.close.call_args_list == [call(), call()]


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        (
            f'nohup sh "/out/{LAUNCHER_SCRIPT_STEM}.sh" > /dev/null 2>&1 &\necho $!',
            "123",
        ),
        (f'if [ -f "/out/{EXIT_CODE_FILE_NAME}" ]; then', "COMPLETED 1"),
        ('export A="b"\npueue add --print-task-id -- "cmd"', "123"),
        (_encoded(f'cmd.exe /c call "/out/{LAUNCHER_SCRIPT_STEM}.cmd"'), "123"),
        (_encoded("$env:A = 'b'; pueue add --print-task-id -- \"cmd\""), "123"),
    ],
)
def test_get_fake_ssh_response_for_simple_ssh_commands(
    command: str, expected: str
) -> None:
    """Test that the simple_ssh start and status commands get a pid and a status line."""
    assert _get_fake_ssh_response(command) == expected


def test_decode_powershell_command_reveals_the_windows_script() -> None:
    """Test that the script is read back from the payload, which tells the windows engines apart."""
    assert _decode_powershell_command(_encoded("pueue add")) == "pueue add"
    assert _decode_powershell_command("pueue add") == "pueue add"


def test_get_fake_ssh_response_for_pueue_status_is_a_done_task() -> None:
    """Test that the fake pueue state holds the fake task id as a successful task."""
    state = json.loads(_get_fake_ssh_response("pueue status --json"))

    assert state["tasks"]["123"]["status"]["Done"]["result"] == "Success"


def _ssh_hooks(mock_ssh_hook: MagicMock, *conn_ids: str) -> dict[str, MagicMock]:
    """Make the SSHHook mock return a hook per existing connection id and raise otherwise."""
    hooks = {conn_id: MagicMock(ssh_conn_id=conn_id) for conn_id in conn_ids}

    def side_effect(ssh_conn_id: str, **_: int) -> MagicMock:
        if ssh_conn_id not in hooks:
            raise AirflowNotFoundException(f"The conn_id `{ssh_conn_id}` isn't defined")
        return hooks[ssh_conn_id]

    mock_ssh_hook.side_effect = side_effect
    return hooks


@patch("plugins.sensors.ssh_utils.SSHHook")
def test_get_cluster_ssh_hook_returns_valid_ssh_hook(mock_ssh_hook: MagicMock) -> None:
    """Test that get_cluster_ssh_hook probes the numbered connections and returns the first one."""
    hooks = _ssh_hooks(mock_ssh_hook, "some_prefix_1", "some_prefix_2")

    # when
    hook = get_cluster_ssh_hook(attempt_no=0, ssh_connection_id_prefix="some_prefix")

    assert hook == hooks["some_prefix_1"]
    assert [c.kwargs for c in mock_ssh_hook.call_args_list] == [
        {"ssh_conn_id": f"some_prefix_{n}", "conn_timeout": 60, "cmd_timeout": 60}
        for n in (1, 2, 3)
    ]


@patch("plugins.sensors.ssh_utils.SSHHook")
def test_get_cluster_ssh_hook_raises_exception_when_no_connections_found(
    mock_ssh_hook: MagicMock,
) -> None:
    """Test that get_cluster_ssh_hook raises an exception when `<prefix>_1` does not exist."""
    _ssh_hooks(mock_ssh_hook)

    with pytest.raises(AirflowFailException, match="No SSH connections found"):
        get_cluster_ssh_hook(attempt_no=0, ssh_connection_id_prefix="some_prefix")


@patch(
    "airflow.providers.ssh.hooks.ssh.SSHHook.__init__",
    side_effect=AirflowNotFoundException("Not found"),
)
def test_get_cluster_ssh_hook_raises_exception_when_connection_not_found(
    mock_ssh_hook: MagicMock,  # noqa:ARG001
) -> None:
    """Test that the real SSHHook constructor raising AirflowNotFoundException ends the probing."""
    with pytest.raises(AirflowFailException, match="No SSH connections found"):
        get_cluster_ssh_hook(attempt_no=0, ssh_connection_id_prefix="some_prefix")


@patch("plugins.sensors.ssh_utils.SSHHook")
def test_get_cluster_ssh_hook_cycles_through_connections_on_multiple_attempts(
    mock_ssh_hook: MagicMock,
) -> None:
    """Test that get_cluster_ssh_hook cycles through available connections on multiple attempts."""
    _ssh_hooks(mock_ssh_hook, "some_prefix_1", "some_prefix_2")

    # when
    conn_ids = [
        get_cluster_ssh_hook(
            attempt_no=attempt_no, ssh_connection_id_prefix="some_prefix"
        ).ssh_conn_id
        for attempt_no in (0, 1, 2)
    ]

    assert conn_ids == ["some_prefix_1", "some_prefix_2", "some_prefix_1"]


@patch("plugins.sensors.ssh_utils.SSHHook")
def test_get_cluster_ssh_hook_selects_connections_by_prefix(
    mock_ssh_hook: MagicMock,
) -> None:
    """Test that two prefixes select disjoint connection sets."""
    _ssh_hooks(mock_ssh_hook, "cluster_a_1", "cluster_b_1")

    # when
    hook_a = get_cluster_ssh_hook(attempt_no=0, ssh_connection_id_prefix="cluster_a")
    hook_b = get_cluster_ssh_hook(attempt_no=0, ssh_connection_id_prefix="cluster_b")

    assert (hook_a.ssh_conn_id, hook_b.ssh_conn_id) == ("cluster_a_1", "cluster_b_1")


@patch("plugins.sensors.ssh_utils.SSHHook")
def test_get_cluster_ssh_hook_stops_at_first_gap(mock_ssh_hook: MagicMock) -> None:
    """Test that a gap in the numbering hides the connections after it (documented contract)."""
    _ssh_hooks(mock_ssh_hook, "some_prefix_1", "some_prefix_3")

    # when
    hook = get_cluster_ssh_hook(attempt_no=1, ssh_connection_id_prefix="some_prefix")

    assert hook.ssh_conn_id == "some_prefix_1"
