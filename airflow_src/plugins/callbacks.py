"""Callbacks for Airflow tasks."""

import logging
from typing import Any

# from common.exceptions import RawfileProcessingError
from shared.db.interface import update_raw_file_status
from shared.db.models import RawFileStatus


def on_failure_callback(context: dict[str, Any], **kwargs) -> None:
    """Set raw file status depending on exceptions."""
    logging.info(f"on_failure_callback {context}, {kwargs}")

    ti = context["task_instance"]
    logging.info(f"task {ti.task_id} failed in dag {ti.dag_id} ")

    ex = context["exception"]

    status_details = None
    # if isinstance(ex, RawfileProcessingError):
    #     status_details=ex.details

    update_raw_file_status(
        ex.raw_file_name, RawFileStatus.ERROR, status_details=status_details
    )
