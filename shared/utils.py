"""Shared utils."""

import logging

from airflow.models import TaskInstance


def put_xcom(ti: TaskInstance, data_to_store: dict[str, str]) -> None:
    """Push the values in `map` to XCom."""
    for key, value in data_to_store.items():
        if value is None:
            raise ValueError(f"No value found for {key}.")

        logging.info(f"Pushing to XCOM: '{key}'='{value}'")
        ti.xcom_push(key, value)


def get_xcom(ti: TaskInstance, keys: list[str]) -> dict[str, str]:
    """Get the value of an XCom with key `key`."""
    values = {}

    for key in keys:
        value = ti.xcom_pull(key=key)

        if value is None:
            raise ValueError(f"No value found for XCOM key {key}")

        logging.info(f"Pulled from XCOM: '{key}'='{value}'")
        values[key] = value

    return values
