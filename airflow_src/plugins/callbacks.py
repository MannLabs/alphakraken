"""Callbacks for Airflow tasks."""

import logging
from typing import Any

from common.keys import DagContext, DagParams, XComKeys
from common.utils import get_xcom

from shared.db.interface import update_raw_file
from shared.db.models import RawFileStatus


def on_failure_callback(context: dict[str, Any], **kwargs) -> None:
    """Set raw file status to error.

    Assumes that "raw_file_id" is in the XCom.
    """
    logging.info(f"on_failure_callback {context}, {kwargs}")

    ti = context["task_instance"]
    logging.info(f"task {ti.task_id} failed in dag {ti.dag_id} ")

    try:
        raw_file_id = context[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    except KeyError:
        try:
            raw_file_id = get_xcom(ti, key=XComKeys.RAW_FILE_ID)
        except KeyError:
            logging.warning(
                "could not find raw file id in dag params nor xcom. Not updating status in db."
            )
            return

    ex = context["exception"]

    update_raw_file(
        raw_file_id,
        new_status=RawFileStatus.ERROR,
        status_details=f"[{ti.task_id}] {ex!s}",
    )
