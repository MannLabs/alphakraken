"""Callbacks for Airflow tasks."""

import logging
from typing import Any

from common.keys import DAG_DELIMITER, DagContext, DagParams, XComKeys
from common.utils import get_xcom
from impl.processor_impl import (
    QuantingFailedException,
    QuantingFailedNewErrorException,
    QuantingFailedUnknownErrorException,
)
from plugins.s3.s3_utils import S3UploadFailedException

from shared.db.interface import update_raw_file
from shared.db.models import BackupStatus, RawFileStatus


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

    # TODO: introduce generic exceptions that tell the callback what to set in terms of fields (e.g. ex.field_updates = {..})

    if isinstance(ex, QuantingFailedException):
        # in this case, the status is already set so nothing to do here
        return
    if isinstance(ex, S3UploadFailedException):
        # failed s3 uploads should not change the overall raw file state as it is currently hard to recover
        update_args = {"backup_status": BackupStatus.UPLOAD_FAILED}

    else:
        cleaned_dag_id = ti.dag_id.split(DAG_DELIMITER)[0]
        status_details = f"[{cleaned_dag_id}.{ti.task_id}] {ex!s}"

        # Truncate to fit database field max_length of 1024
        max_status_length = 1024
        if len(status_details) > max_status_length:
            status_details = status_details[: max_status_length - 3] + "..."

        update_args = {
            "status_details": status_details,
        }

        # don't set the state to error during quanting checks
        if not isinstance(
            ex, (QuantingFailedNewErrorException, QuantingFailedUnknownErrorException)
        ):
            update_args["new_status"] = RawFileStatus.ERROR

    update_raw_file(
        raw_file_id,
        **update_args,  # type: ignore[invalid-argument-type]
    )
