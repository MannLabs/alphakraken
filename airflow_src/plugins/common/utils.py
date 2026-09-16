"""Shared utils."""

import logging
import os
from collections.abc import Iterable
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Any

import pytz
from airflow.exceptions import DagNotFound
from airflow.models import Connection, DagRun
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sdk import Variable
from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowNotFoundException,
    AirflowRuntimeError,
)
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import ErrorResponse, TriggerDagRun
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance as TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.types import DagRunType
from common.constants import (
    CLUSTER_SSH_COMMAND_TIMEOUT,
    CLUSTER_SSH_CONNECTION_TIMEOUT,
)

_xcom_types = str | list[str] | dict[str, Any] | int


def put_xcom(ti: TaskInstance, key: str, value: _xcom_types) -> None:
    """Push to XCom `key`=`value`."""
    if value is None:
        raise ValueError(f"No value found for {key}.")

    logging.info(f"Pushing to XCOM: '{key}'='{value}'")
    ti.xcom_push(key, value)


_NO_DEFAULT = object()


def get_xcom(
    ti: TaskInstance,
    key: str,
    *,
    task_ids: str | Iterable[str],
    default: _xcom_types | None = _NO_DEFAULT,
    map_indexes: int | Iterable[int] | None = None,
) -> _xcom_types | None:
    """Get the value of an XCom with `key`.

    :param task_ids: The task(s) that pushed the value.
    :param map_indexes: Pull from a specific map index in dynamically mapped tasks.
    :raises KeyError: If no value found and no default was provided.
    """
    pull_kwargs: dict[str, Any] = {"key": key, "task_ids": task_ids}

    if default is not _NO_DEFAULT:
        pull_kwargs["default"] = default
    if map_indexes is not None:
        pull_kwargs["map_indexes"] = map_indexes

    value = ti.xcom_pull(**pull_kwargs)

    # we handle the default ourselves, as passing it to xcom_pull is ignored when `map_indexes` is not given
    if value is None:
        if default is _NO_DEFAULT:
            raise KeyError(f"No value found for XCOM key {key}")
        return default

    logging.info(
        f"Pulled from XCOM: '{key}'='{value}' ({task_ids=} {default=} {map_indexes=})"
    )

    return value


def get_airflow_variable(
    key: str, default: str | float = "__DEFAULT_NOT_SET"
) -> str | int | float:
    """Get the value of an Airflow Variable with `key` with an optional default.

    Will return non-`str` types only in case the default is returned.
    """
    if default == "__DEFAULT_NOT_SET":
        value = Variable.get(key)
    else:
        value = Variable.get(key, default=default)

    logging.info(f"Got airflow variable: '{key}'='{value}' (default: '{default}')")

    return value


def get_env_variable(
    key: str, default: str | None = None, *, verbose: bool = True
) -> str:
    """Get the value of an environment variable with `key` with an optional default."""
    if (value := os.getenv(key, default=default)) is None:
        raise KeyError(f"Environment variable '{key}' not set.")

    if verbose:
        logging.info(
            f"Got environment variable: '{key}'='{value}' (default: '{default}')"
        )

    return value


def trigger_dag_run(
    dag_id: str, conf: dict[str, str], time_delay_minutes: int | None = None
) -> None:
    """Trigger a DAG run with the given configuration.

    :param time_delay_minutes: If given, the run does not start before now plus this delay.
    :raises DagNotFound: If no DAG with `dag_id` exists.
    """
    now = datetime.now(tz=pytz.utc)
    run_after = (
        now
        if time_delay_minutes is None
        else now + timedelta(minutes=time_delay_minutes)
    )
    run_id = DagRun.generate_run_id(
        run_type=DagRunType.MANUAL, logical_date=None, run_after=run_after
    )

    logging.info(f"Triggering DAG {dag_id} with {run_id=} {run_after=} {conf=}")

    # The Task Execution API is the same channel TriggerDagRunOperator uses: no API token needed.
    # The supervisor turns a 404 of the api-server into an AirflowRuntimeError.
    try:
        response = task_runner.SUPERVISOR_COMMS.send(
            TriggerDagRun(
                dag_id=dag_id,
                run_id=run_id,
                conf=conf,
                logical_date=None,
                run_after=run_after,
            )
        )
    except AirflowRuntimeError as e:
        if (e.error.detail or {}).get("status_code") == HTTPStatus.NOT_FOUND:
            raise DagNotFound(f"Dag id {dag_id} not found") from e
        raise

    if isinstance(response, ErrorResponse):
        raise AirflowFailException(
            f"Could not trigger DAG {dag_id}: {response.error.value} {response.detail}"
        )


def truncate_string(input_string: str | None, n: int = 200) -> str | None:
    """Truncate the input string to `n` characters."""
    return (
        input_string[: n // 2] + " ... " + input_string[-n // 2 :]
        if input_string is not None and len(input_string) > n
        else input_string
    )


def get_timestamp() -> float:
    """Get the current timestamp."""
    return datetime.now(tz=pytz.utc).timestamp()


def get_minutes_since_fixed_time_point() -> int:
    """Return the minutes since a given point in time as the priority weight.

    See https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/priority-weight.html


    Use minutes and baseline to avoid NumericValueOutOfRange error in the airflow DB.
    """
    current_epoch_time = get_timestamp()
    baseline = datetime(2024, 1, 1, tzinfo=pytz.utc).timestamp()

    return int((current_epoch_time - baseline) // 60)


@provide_session
def _get_cluster_ssh_connections(
    session: Any = None,
    *,
    ssh_connection_id_prefix: str,
) -> list[str]:
    """Get all SSH connection IDs that start with the given prefix.

    :param session: Database session (provided by decorator)
    :param ssh_connection_id_prefix: Prefix of the connection IDs to select

    :return: List of connection IDs matching the prefix, sorted by ID
    """
    assert session is not None
    connections = (
        session.query(Connection)
        .filter(Connection.conn_id.startswith(ssh_connection_id_prefix))
        .all()
    )
    conn_ids = [conn.conn_id for conn in connections]

    logging.info(
        f"Found {len(conn_ids)} SSH connections with prefix '{ssh_connection_id_prefix}': {conn_ids}"
    )
    return sorted(conn_ids)


def get_cluster_ssh_hook(
    ssh_connection_id_prefix: str,
    attempt_no: int,
    conn_timeout: int = CLUSTER_SSH_CONNECTION_TIMEOUT,
    cmd_timeout: int = CLUSTER_SSH_COMMAND_TIMEOUT,
) -> SSHHook | None:
    """Get an SSH hook for the compute cluster.

    :param ssh_connection_id_prefix: Prefix of the Airflow connection IDs to choose from.
    :param attempt_no: The attempt number to select the SSH connection ID. Will return a different connection ID on each attempt.
    :param conn_timeout: Connection timeout in seconds.
    :param cmd_timeout: Command execution timeout in seconds.

    The connection id needs to be defined in the Airflow UI and is obtained from get_cluster_ssh_connections().
    """
    error_details = (
        f"Please set up a connection starting with {ssh_connection_id_prefix} in the Airflow UI ('Admin -> Connections') "
        "or set the Airflow Variable 'debug_no_cluster_ssh=True'."
    )
    cluster_ssh_connections_ids = _get_cluster_ssh_connections(
        ssh_connection_id_prefix=ssh_connection_id_prefix
    )
    if not cluster_ssh_connections_ids:
        raise AirflowFailException(f"No SSH connections found.\n{error_details}")

    ssh_conn_id = cluster_ssh_connections_ids[
        attempt_no % len(cluster_ssh_connections_ids)
    ]

    logging.info(f"Using {ssh_conn_id=} for SSH connection (attempt {attempt_no})")
    try:
        return SSHHook(
            ssh_conn_id=ssh_conn_id, conn_timeout=conn_timeout, cmd_timeout=cmd_timeout
        )
    except AirflowNotFoundException as e:
        msg = (
            f"Could not find cluster SSH connection.\n"
            f"{error_details}\n"
            f"Original message: {e}"
        )
        raise AirflowFailException(msg) from e
