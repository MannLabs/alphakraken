"""Business logic for S3 backup operations."""

import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from botocore.exceptions import BotoCoreError, ClientError
from common.keys import DagContext, DagParams, OpArgs, XComKeys
from common.utils import get_xcom
from dags.impl.processor_impl import _get_project_id_or_fallback
from dags.impl.s3_utils import (
    calculate_s3_etag,
    get_s3_client,
    normalize_bucket_name,
)

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import BackupStatus
from shared.yamlsettings import YAMLSETTINGS

CHUNK_SIZE_MB = 500


def upload_raw_file_to_s3(ti: TaskInstance, **kwargs) -> None:  # noqa: PLR0915
    """Upload raw file to S3 bucket.

    This function:
    1. Checks if S3 backup is enabled in config
    2. Pre-validates bucket exists
    3. Checks for duplicate uploads (skip if already uploaded with matching ETag)
    4. Sets backup status to IN_PROGRESS
    5. For each file in file_info:
       - Calculate local ETag
       - Upload to S3 using multipart upload
       - Verify remote ETag matches
    6. On success: set backup_status = DONE, save s3_backup_key and s3_etag
    7. On failure: set backup_status = FAILED, log error (non-blocking)

    Args:
        ti: TaskInstance from Airflow
        **kwargs: Contains instrument_id and raw_file_id in params

    """
    s3_config = YAMLSETTINGS.get("backup", {}).get("s3", {})
    region = s3_config.get("region")
    bucket_prefix = s3_config.get("bucket_prefix")
    if not region or not bucket_prefix:
        raise AirflowFailException(
            "S3 backup enabled but region or bucket_prefix not configured in yaml"
        )

    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]  # TODOS3 get from raw_file
    raw_file = get_raw_file_by_id(raw_file_id)

    files_dst_paths = {
        Path(k): Path(v) for k, v in get_xcom(ti, XComKeys.FILES_DST_PATHS).items()
    }
    tfp = get_xcom(ti, "tfp")

    # TODO: TBD organize fallback bucket by instrument & year_month?
    bucket_name = normalize_bucket_name(
        _get_project_id_or_fallback(raw_file.project_id, instrument_id), bucket_prefix
    )

    logging.info(f"Starting S3 upload for {raw_file_id} to {bucket_name=} {region=}")

    try:
        s3_client = get_s3_client(region)

        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} exists and is accessible")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "n/a")
            if error_code == "404":
                msg = f"Bucket {bucket_name} does not exist. Please create it before uploading."
            else:
                msg = f"Cannot access bucket {bucket_name}: {error_code} - {e}"

            update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
            raise AirflowFailException(msg) from e

        update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_IN_PROGRESS)

        for dst_path in files_dst_paths.values():
            logging.info(f"Processing file for upload: {dst_path=} {tfp=}")
            s3_key = str(dst_path.relative_to(tfp))

            logging.info(f"Uploading {dst_path} to s3://{bucket_name}/{s3_key}")

            # keep etag calculation close to upload as chunk size could change but add to file_info
            # TODO: need to store chunk size used for upload in DB, to be able to verify later
            # then it can be done during file_info creation
            local_etag = calculate_s3_etag(dst_path, chunk_size_mb=CHUNK_SIZE_MB)

            try:
                head_response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                existing_etag = head_response.get("ETag", "").strip('"')

                if local_etag == existing_etag:
                    logging.info(
                        f"File {s3_key} already exists with matching ETag, skipping upload"
                    )
                    continue

                update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
                raise AirflowFailException(
                    f"File {s3_key} exists but ETag mismatch (local: {local_etag}, remote: {existing_etag})"  # , re-uploading"
                )

            # ClientError - 404 is actually a happy path 'file not uploaded yet'
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code != "404":
                    update_raw_file(
                        raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED
                    )
                    raise

            with dst_path.open("rb") as f:
                s3_client.upload_fileobj(
                    f,
                    bucket_name,
                    s3_key,
                    Config=_get_transfer_config(CHUNK_SIZE_MB),
                )

            remote_etag = (
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                .get("ETag", "")
                .strip('"')
            )

            if local_etag != remote_etag:
                msg = f"ETag mismatch for {s3_key}: local {local_etag} != remote {remote_etag}"
                update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
                raise AirflowFailException(msg)  # noqa: TRY301

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
        logging.exception(msg)
        update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
        raise AirflowFailException(msg) from e


def _get_transfer_config(chunk_size_mb: int):  # type: ignore[no-untyped-def] # noqa: ANN202
    """Get boto3 transfer config for multipart uploads.

    Arguments:
    ---------
        chunk_size_mb: Chunk size in MB

    Returns:
    -------
        TransferConfig with 500MB multipart threshold and chunk size

    """
    from boto3.s3.transfer import TransferConfig

    chunk_size_bytes = chunk_size_mb * 1024 * 1024

    return TransferConfig(
        multipart_threshold=chunk_size_bytes,
        multipart_chunksize=chunk_size_bytes,
        use_threads=True,
        max_concurrency=10,
    )
