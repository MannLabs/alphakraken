"""Shared utils."""

import logging
from pathlib import Path

from airflow.models import TaskInstance
from common.keys import InstrumentKeys
from common.settings import INSTRUMENTS, InternalPaths


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


def get_instrument_data_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument."""
    return (
        Path(InternalPaths.APC_PATH_PREFIX)
        / INSTRUMENTS[instrument_id][InstrumentKeys.RAW_DATA_PATH]
    )
