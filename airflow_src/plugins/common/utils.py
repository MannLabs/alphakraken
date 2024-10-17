"""Shared utils."""

import logging
import os
from datetime import datetime, timedelta

import pytz
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun, TaskInstance, Variable
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.types import DagRunType

_xcom_types = str | list[str] | dict[str, str | bool] | int


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


def get_airflow_variable(key: str, default: str = "__DEFAULT_NOT_SET") -> str:
    """Get the value of an Airflow Variable with `key` with an optional default."""
    if default == "__DEFAULT_NOT_SET":
        value = Variable.get(key)
    else:
        value = Variable.get(key, default_var=default)

    logging.info(f"Got airflow variable: '{key}'='{value}' (default: '{default}')")

    return value


def get_env_variable(key: str, default: str | None = None) -> str:
    """Get the value of an environment variable with `key` with an optional default."""
    if (value := os.getenv(key, default=default)) is None:
        raise KeyError(f"Environment variable '{key}' not set.")

    logging.info(f"Got environment variable: '{key}'='{value}' (default: '{default}')")

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

    # TODO: handle occasional airflow.exceptions.DagNotFound: Dag id acquisition_handler.test4 not found
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


def get_cluster_ssh_hook() -> SSHHook:
    """Get the SSH hook for the cluster.

    The connection 'cluster_ssh_connection' needs to be defined in Airflow UI.
    """
    logging.info("Getting cluster SSH hook..")
    return SSHHook(
        ssh_conn_id="cluster_ssh_connection", conn_timeout=60, cmd_timeout=60
    )
