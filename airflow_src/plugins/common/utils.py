"""Shared utils."""

import logging
import os

from airflow.models import TaskInstance, Variable

_xcom_types = str | list[str] | dict[str, str | bool] | int


def put_xcom(ti: TaskInstance, key: str, value: _xcom_types) -> None:
    """Push to XCom `key`=`value`."""
    if value is None:
        raise ValueError(f"No value found for {key}.")

    logging.info(f"Pushing to XCOM: '{key}'='{value}'")
    ti.xcom_push(key, value)


def get_xcom(ti: TaskInstance, key: str) -> _xcom_types:
    """Get the value of an XCom with `key`."""
    value = ti.xcom_pull(key=key)

    if value is None:
        raise ValueError(f"No value found for XCOM key {key}")

    logging.info(f"Pulled from XCOM: '{key}'='{value}'")

    return value


def get_variable(key: str, default: str = "__DEFAULT_NOT_SET") -> str:
    """Get the value of an Airflow Variable with `key` with an optional default."""
    if default == "__DEFAULT_NOT_SET":
        value = Variable.get(key)
    else:
        value = Variable.get(key, default_var=default)

    logging.info(f"Got variable: '{key}'='{value}' (default: '{default}')")

    return value


def get_env_variable(key: str, default: str | None = None) -> str:
    """Get the value of an environment variable with `key` with an optional default."""
    if (value := os.getenv(key, default=default)) is None:
        raise KeyError(f"Environment variable '{key}' not set.")

    logging.info(f"Got environment variable: '{key}'='{value}' (default: '{default}')")

    return value
