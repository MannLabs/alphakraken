"""Business logic for S3 backup operations."""

import logging
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError
from common.keys import DagContext, DagParams, XComKeys
from common.utils import get_xcom
from dags.impl.processor_impl import _get_project_id_or_fallback
from plugins.file_handling import ETAG_SEPARATOR
from plugins.s3.s3_utils import (
    _FILE_NOT_FOUND,
    bucket_exists,
    get_etag,
    get_s3_client,
    get_transfer_config,
    is_upload_needed,
    normalize_bucket_name,
    upload_file_to_s3,
)

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import BackupStatus, RawFile, get_created_at_year_month
from shared.yamlsettings import get_s3_upload_config

S3_UPLOAD_CHUNK_SIZE_MB = 500


class S3UploadFailedException(AirflowFailException):
    """Exception raised when S3 upload fails.

    Enables on_failure_callback to take special action.
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
    6. On success: set backup_status = DONE, save s3_upload_path and s3_etag
    7. On failure: set backup_status = FAILED, log error (non-blocking)

    Args:
        ti: TaskInstance from Airflow
        **kwargs: Contains raw_file_id in params

    """
    s3_config = get_s3_upload_config()
    region = s3_config.get("region")
    bucket_prefix = s3_config.get("bucket_prefix")
    if not region or not bucket_prefix:
        raise S3UploadFailedException(
            "S3 backup enabled but region or bucket_prefix not configured in yaml"
        )

    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    raw_file = get_raw_file_by_id(raw_file_id)

    files_dst_paths = {
        Path(k): Path(v) for k, v in get_xcom(ti, XComKeys.FILES_DST_PATHS).items()
    }
    target_folder_path = get_xcom(ti, XComKeys.TARGET_FOLDER_PATH)

    bucket_name = normalize_bucket_name(
        _get_project_id_or_fallback(raw_file.project_id, raw_file.instrument_id),
        bucket_prefix,
    )
    transfer_config = get_transfer_config(S3_UPLOAD_CHUNK_SIZE_MB)

    logging.info(f"Starting S3 upload for {raw_file_id} to {bucket_name=} {region=}")

    update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_IN_PROGRESS)

    try:
        s3_client = get_s3_client(region)

        bucket_exists_, error_msg = bucket_exists(bucket_name, s3_client)
        if not bucket_exists_:
            raise S3UploadFailedException(error_msg)  # noqa: TRY301

        file_path_to_target_path_and_etag, key_prefix = _prepare_upload(
            files_dst_paths, raw_file, target_folder_path
        )

        _upload_all_files(
            file_path_to_target_path_and_etag,
            bucket_name,
            transfer_config,
            s3_client,
        )

        update_raw_file(
            raw_file_id,
            backup_status=BackupStatus.UPLOAD_DONE,
            s3_upload_path=f"s3://{bucket_name}"
            if not key_prefix
            else f"s3://{bucket_name}/{key_prefix}",
        )

        logging.info(f"S3 backup completed for {raw_file_id}")

    except (BotoCoreError, ClientError, Exception) as e:
        msg = f"S3 upload failed for {raw_file_id}: {type(e).__name__} - {e}"
        raise S3UploadFailedException(msg) from e


def _prepare_upload(
    files_dst_paths: dict[Path, Path],
    raw_file: RawFile,
    target_folder_path: str | list[str] | dict[str, Any] | int,
) -> tuple[dict[Path, tuple[str, str]], str]:
    """Prepare mapping of local file paths to S3 keys and local ETags."""
    file_path_to_target_path_and_etag: dict[Path, tuple[str, str]] = {}

    key_prefix = _get_key_prefix(raw_file)

    for local_file_path in files_dst_paths.values():
        logging.info(
            f"Preparing file info for upload: {local_file_path=} {target_folder_path=}"
        )
        local_file_path_relative = str(local_file_path.relative_to(target_folder_path))
        local_etag = _extract_etag_from_file_info(local_file_path_relative, raw_file)
        s3_key = key_prefix + local_file_path_relative
        file_path_to_target_path_and_etag[local_file_path] = (s3_key, local_etag)

    return file_path_to_target_path_and_etag, key_prefix


S3_KEY_SEPARATOR = "/"


def _get_key_prefix(raw_file: RawFile) -> str:
    """Get S3 key prefix based on raw file metadata.

    If raw file is not part of a project, use instrument ID and creation date.
    If raw file is a .wiff file, use the raw file ID as prefix.

    """
    key_prefixes = []
    if raw_file.project_id is None:  # TODO: centralize "no project given" logic
        # fallback buckets get organized by instrument and date
        key_prefixes.append(
            f"{raw_file.instrument_id}{S3_KEY_SEPARATOR}{get_created_at_year_month(raw_file)}"
        )

    if raw_file.id.endswith(".wiff"):  # TODO: use instrument_type here
        key_prefixes.append(raw_file.id)

    if key_prefixes:
        key_prefixes.append("")

    return S3_KEY_SEPARATOR.join(key_prefixes)


def _upload_all_files(
    file_path_to_target_path_and_etag: dict[Path, tuple[str, str]],
    bucket_name: str,
    transfer_config: TransferConfig,
    s3_client: BaseAwsConnection,
) -> None:
    """Upload all files to S3 with ETag verification."""
    for local_file_path, (
        s3_key,
        local_etag,
    ) in file_path_to_target_path_and_etag.items():
        logging.info(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")

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
            raise S3UploadFailedException(msg)

        logging.info(f"Successfully uploaded {s3_key}, ETag verified: {remote_etag}")


def _extract_etag_from_file_info(local_file_path: str, raw_file: RawFile) -> str:
    """Extract ETag from raw_file.file_info for a given local file path."""
    size_and_hashsum = raw_file.file_info[local_file_path]

    etag_and_chunk_size = size_and_hashsum[2]
    return etag_and_chunk_size.split(ETAG_SEPARATOR)[0]
