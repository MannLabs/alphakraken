"""Shared utils."""

import logging
import os

from airflow.models import TaskInstance, Variable


def put_xcom(
    ti: TaskInstance, key: str, value: str | list[str] | dict[str, str]
) -> None:
    """Push to XCom `key`=`value`."""
    if value is None:
        raise ValueError(f"No value found for {key}.")

    logging.info(f"Pushing to XCOM: '{key}'='{value}'")
    ti.xcom_push(key, value)


def get_xcom(ti: TaskInstance, key: str) -> str | list[str] | dict[str, str]:
    """Get the value of an XCom with `key`."""
    value = ti.xcom_pull(key=key)

    if value is None:
        raise ValueError(f"No value found for XCOM key {key}")

    logging.info(f"Pulled from XCOM: '{key}'='{value}'")

    return value


def get_variable(key: str, default_value: str = "__DEFAULT_NOT_SET") -> str:
    """Get the value of an Airflow Variable with `key` and an optional default."""
    logging.info(f"Getting variable '{key}''")

    if default_value == "__DEFAULT_NOT_SET":
        value = Variable.get(key)
    else:
        value = Variable.get(key, default_var=default_value)

    logging.info(f"Got variable: '{key}'='{value}'")

    return value


def get_env_variable(key: str, default_value: str = "__DEFAULT_NOT_SET") -> str:
    """Get the value of an environment variable with `key` and an optional default."""
    logging.info(f"Getting environment variable '{key}''")

    if default_value == "__DEFAULT_NOT_SET":
        value = os.getenv(key)
    else:
        value = os.getenv(key, default=default_value)

    logging.info(f"Got environment variable: '{key}'='{value}'")

    return value
