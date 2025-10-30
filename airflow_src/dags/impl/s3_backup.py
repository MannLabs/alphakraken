"""Business logic for S3 backup operations."""

import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from botocore.exceptions import BotoCoreError, ClientError
from common.keys import DagContext, DagParams, XComKeys
from common.utils import get_xcom
from dags.impl.processor_impl import _get_project_id_or_fallback
from dags.impl.s3_utils import (
    _FILE_NOT_FOUND,
    bucket_exists,
    calculate_s3_etag,
    get_etag,
    get_s3_client,
    get_transfer_config,
    is_upload_needed,
    normalize_bucket_name,
    upload_file_to_s3,
)

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import BackupStatus
from shared.yamlsettings import YAMLSETTINGS

CHUNK_SIZE_MB = 500


class S3UploadFailedException(AirflowFailException):
    """Exception raised when S3 upload fails.

    Enables on_failure_callback to set specific backup_status.
    """


def upload_raw_file_to_s3(ti: TaskInstance, **kwargs) -> None:
    """Upload raw file to S3 bucket.

    This function:
    1. Checks if S3 backup is enabled in config
    2. Pre-validates bucket exists
    4. Sets backup status to IN_PROGRESS
    5. For each file in file_info:
       - Calculate local ETag
       - Skip if already uploaded with matching ETag
       - Upload to S3 using multipart upload
       - Verify remote ETag matches
    6. On success: set backup_status = DONE, save s3_backup_key and s3_etag
    7. On failure: set backup_status = FAILED, log error (non-blocking)

    Args:
        ti: TaskInstance from Airflow
        **kwargs: Contains raw_file_id in params

    """
    s3_config = YAMLSETTINGS.get("backup", {}).get("s3", {})
    region = s3_config.get("region")
    bucket_prefix = s3_config.get("bucket_prefix")
    if not region or not bucket_prefix:
        raise AirflowFailException(
            "S3 backup enabled but region or bucket_prefix not configured in yaml"
        )

    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    raw_file = get_raw_file_by_id(raw_file_id)

    files_dst_paths = {
        Path(k): Path(v) for k, v in get_xcom(ti, XComKeys.FILES_DST_PATHS).items()
    }
    tfp = get_xcom(ti, "tfp")

    # TODO: TBD organize fallback bucket by instrument & year_month?
    bucket_name = normalize_bucket_name(
        _get_project_id_or_fallback(raw_file.project_id, raw_file.instrument_id),
        bucket_prefix,
    )
    transfer_config = get_transfer_config(CHUNK_SIZE_MB)

    logging.info(f"Starting S3 upload for {raw_file_id} to {bucket_name=} {region=}")

    update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_IN_PROGRESS)

    try:
        s3_client = get_s3_client(region)

        bucket_exists_, error_msg = bucket_exists(bucket_name, s3_client)
        if not bucket_exists_:
            raise S3UploadFailedException(error_msg)  # noqa: TRY301

        for local_file_path in files_dst_paths.values():
            logging.info(f"Processing file for upload: {local_file_path=} {tfp=}")
            s3_key = str(local_file_path.relative_to(tfp))

            logging.info(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")

            # keep etag calculation close to upload as chunk size could change but add to file_info
            # TODO: need to store chunk size used for upload in DB, to be able to verify later then it can be done during file_info creation
            local_etag = calculate_s3_etag(local_file_path, chunk_size_mb=CHUNK_SIZE_MB)

            try:
                if not is_upload_needed(bucket_name, s3_key, local_etag, s3_client):
                    continue
            except ValueError as e:
                raise S3UploadFailedException(e) from e

            upload_file_to_s3(
                local_file_path, bucket_name, s3_key, transfer_config, s3_client
            )

            remote_etag = get_etag(bucket_name, s3_key, s3_client)

            if local_etag != remote_etag or remote_etag is _FILE_NOT_FOUND:
                msg = f"ETag mismatch for {s3_key}: local {local_etag} != remote {remote_etag}"
                raise S3UploadFailedException(msg)  # noqa: TRY301

            logging.info(
                f"Successfully uploaded {s3_key}, ETag verified: {remote_etag}"
            )

        update_raw_file(
            raw_file_id,
            backup_status=BackupStatus.UPLOAD_DONE,
            s3_backup_key=bucket_name,
        )

        logging.info(f"S3 backup completed for {raw_file_id}")

    except (BotoCoreError, ClientError, Exception) as e:
        msg = f"S3 upload failed for {raw_file_id}: {type(e).__name__} - {e}"
        raise S3UploadFailedException(msg) from e
