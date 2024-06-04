"""Shared utils."""

import logging

from airflow.models import TaskInstance


def put_xcom(ti: TaskInstance, key: str, value: str | list) -> None:
    """Push to XCom `key`=`value`."""
    if value is None:
        raise ValueError(f"No value found for {key}.")

    logging.info(f"Pushing to XCOM: '{key}'='{value}'")
    ti.xcom_push(key, value)


def get_xcom(ti: TaskInstance, key: str) -> str:
    """Get the value of an XCom with `key`."""
    value = ti.xcom_pull(key=key)

    if value is None:
        raise ValueError(f"No value found for XCOM key {key}")

    logging.info(f"Pulled from XCOM: '{key}'='{value}'")

    return value
