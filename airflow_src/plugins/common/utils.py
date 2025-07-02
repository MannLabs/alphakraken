"""Shared utils."""

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pytz
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection, DagRun, TaskInstance, Variable
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.db import provide_session
from airflow.utils.types import DagRunType
from common.constants import (
    CLUSTER_SSH_COMMAND_TIMEOUT,
    CLUSTER_SSH_CONNECTION_ID,
    CLUSTER_SSH_CONNECTION_TIMEOUT,
)

_xcom_types = str | list[str] | dict[str, Any] | int


def put_xcom(ti: TaskInstance, key: str, value: _xcom_types) -> None:
    """Push to XCom `key`=`value`."""
    if value is None:
        raise ValueError(f"No value found for {key}.")

    logging.info(f"Pushing to XCOM: '{key}'='{value}'")
    ti.xcom_push(key, value)


def get_xcom(
    ti: TaskInstance, key: str, default: _xcom_types | None = None
) -> _xcom_types:
    """Get the value of an XCom with `key`."""
    value = ti.xcom_pull(key=key, default=default)

    if value is None:
        raise KeyError(f"No value found for XCOM key {key}")

    logging.info(f"Pulled from XCOM: '{key}'='{value}'")

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
        value = Variable.get(key, default_var=default)

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
    """Trigger a DAG run with the given configuration."""
    now = datetime.now(tz=pytz.utc)
    run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date=now)

    execution_date = (
        None
        if time_delay_minutes is None
        else now + timedelta(minutes=time_delay_minutes)
    )

    logging.info(f"Triggering DAG {dag_id} with {run_id=} with {conf=}")

    trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=conf,
        execution_date=execution_date,
        replace_microseconds=False,
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
def get_cluster_ssh_connections(
    prefix: str = CLUSTER_SSH_CONNECTION_ID, session=None
) -> list[str]:
    """Get all SSH connection IDs that start with the given prefix.
    
    :param prefix: The connection ID prefix to search for
    :param session: Database session (provided by decorator)

    :return: List of connection IDs matching the prefix, sorted by ID
    """
    connections = (
        session.query(Connection)
        .filter(Connection.conn_id.startswith(prefix))
        .all()
    )
    conn_ids = [conn.conn_id for conn in connections]
    logging.info(f"Found {len(conn_ids)} SSH connections with prefix '{prefix}': {conn_ids}")
    return sorted(conn_ids)

cluster_ssh_connections_ids = get_cluster_ssh_connections()

def get_cluster_ssh_hook(
    attempt_no: int,
    conn_timeout: int = CLUSTER_SSH_CONNECTION_TIMEOUT,
    cmd_timeout: int = CLUSTER_SSH_COMMAND_TIMEOUT,
) -> SSHHook | None:
    """Get an SSH hook for the compute cluster.

    :param attempt_no: The attempt number to select the SSH connection ID. Will return a different connection ID on each attempt.
    :param conn_timeout: Connection timeout in seconds.
    :param cmd_timeout: Command execution timeout in seconds.

    The connection id needs to be defined in the Airflow UI and is obtained from get_cluster_ssh_connections().
    """

    ssh_conn_id = cluster_ssh_connections_ids[attempt_no % len(cluster_ssh_connections_ids)]

    logging.info(f"Using {ssh_conn_id=} for SSH connection (attempt {attempt_no})")
    try:
        return SSHHook(
            ssh_conn_id=ssh_conn_id, conn_timeout=conn_timeout, cmd_timeout=cmd_timeout
        )
    except AirflowNotFoundException as e:
        msg = (
            f"Could not find cluster SSH connection. Either set up the connection '{ssh_conn_id}' ('Admin -> Connections') "
            f"or set the Airflow Variable 'debug_no_cluster_ssh=True'.\n"
            f"Original message: {e}"
        )
        logging.warning(msg)
