"""Business logic for S3 backup operations."""

import logging
from pathlib import Path

from airflow.models import TaskInstance
from botocore.exceptions import BotoCoreError, ClientError
from common.keys import DagContext, DagParams, OpArgs, XComKeys
from common.utils import get_xcom
from dags.impl.s3_utils import (
    build_s3_key,
    calculate_s3_etag,
    get_s3_client,
    normalize_bucket_name,
)

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import BackupStatus
from shared.yamlsettings import YAMLSETTINGS


def upload_raw_file_to_s3(ti: TaskInstance, **kwargs) -> None:  # noqa: C901, PLR0915
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
    # Check config: short-circuit if not S3 backup mode
    backup_type = YAMLSETTINGS.get("backup", {}).get("backup_type", "local")
    if backup_type != "s3":
        logging.info(
            f"Backup type is '{backup_type}', skipping S3 upload for this file."
        )
        return

    # Get config
    s3_config = YAMLSETTINGS.get("backup", {}).get("s3", {})
    region = s3_config.get("region")
    bucket_prefix = s3_config.get("bucket_prefix")

    if not region or not bucket_prefix:
        logging.error(
            "S3 backup enabled but region or bucket_prefix not configured in yaml"
        )
        return

    # Get file info
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    raw_file = get_raw_file_by_id(raw_file_id)

    # Get file paths from XCom (set by compute_checksum task)
    files_dst_paths = {
        Path(k): Path(v) for k, v in get_xcom(ti, XComKeys.FILES_DST_PATHS).items()
    }

    # Construct bucket name
    bucket_name = normalize_bucket_name(raw_file.project_id, bucket_prefix)

    logging.info(
        f"Starting S3 upload for {raw_file_id} to bucket {bucket_name} in region {region}"
    )

    try:
        # Get S3 client
        s3_client = get_s3_client(region)

        # Pre-validate bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} exists and is accessible")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                msg = f"Bucket {bucket_name} does not exist. Please create it before uploading."
            else:
                msg = f"Cannot access bucket {bucket_name}: {error_code} - {e}"
            logging.exception(msg)
            update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
            return

        # Set status to IN_PROGRESS
        update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_IN_PROGRESS)

        # Upload each file
        uploaded_files: dict[str, str] = {}  # relative_path -> etag
        s3_key_prefix = build_s3_key(
            raw_file.project_id, instrument_id, raw_file.id, ""
        )

        for src_path, dst_path in files_dst_paths.items():
            # Calculate relative path from backup_base_path
            relative_path = dst_path.relative_to(raw_file.backup_base_path)

            # Build S3 key
            s3_key = build_s3_key(
                raw_file.project_id, instrument_id, raw_file.id, str(relative_path)
            )

            logging.info(f"Uploading {src_path} to s3://{bucket_name}/{s3_key}")

            # Check if file already exists with matching ETag (skip duplicate uploads)
            try:
                head_response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                existing_etag = head_response.get("ETag", "").strip('"')

                # Calculate local ETag to compare
                local_etag = calculate_s3_etag(src_path, chunk_size_mb=500)

                if local_etag == existing_etag:
                    logging.info(
                        f"File {s3_key} already exists with matching ETag, skipping upload"
                    )
                    uploaded_files[str(relative_path)] = existing_etag
                    continue

                logging.warning(
                    f"File {s3_key} exists but ETag mismatch (local: {local_etag}, remote: {existing_etag}), re-uploading"
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                if error_code != "404":
                    # Unexpected error
                    raise

            # Calculate ETag before upload
            local_etag = calculate_s3_etag(src_path, chunk_size_mb=500)

            # Upload file using boto3 (handles multipart automatically)
            # Use 500MB chunks to match ETag calculation
            with src_path.open("rb") as f:
                s3_client.upload_fileobj(
                    f,
                    bucket_name,
                    s3_key,
                    Config=_get_transfer_config(),
                )

            # Verify uploaded ETag
            head_response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            remote_etag = head_response.get("ETag", "").strip('"')

            if local_etag != remote_etag:
                msg = f"ETag mismatch for {s3_key}: local {local_etag} != remote {remote_etag}"
                logging.error(msg)
                update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
                return

            logging.info(
                f"Successfully uploaded {s3_key}, ETag verified: {remote_etag}"
            )
            uploaded_files[str(relative_path)] = remote_etag

        # All files uploaded successfully
        # Store first file's ETag as representative (for single-file instruments)
        # For multi-file, this is just a reference
        representative_etag = (
            next(iter(uploaded_files.values())) if uploaded_files else ""
        )

        update_raw_file(
            raw_file_id,
            backup_status=BackupStatus.UPLOAD_DONE,
            s3_backup_key=s3_key_prefix,
            s3_etag=representative_etag,
        )

        logging.info(
            f"S3 backup completed for {raw_file_id}: uploaded {len(uploaded_files)} files to {bucket_name}"
        )

    except (BotoCoreError, ClientError) as e:
        msg = f"S3 upload failed for {raw_file_id}: {type(e).__name__} - {e}"
        logging.exception(msg)
        update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
        # Don't raise - non-blocking failure
    except Exception as e:
        msg = f"Unexpected error during S3 upload for {raw_file_id}: {type(e).__name__} - {e}"
        logging.exception(msg)
        update_raw_file(raw_file_id, backup_status=BackupStatus.UPLOAD_FAILED)
        # Don't raise - non-blocking failure


def _get_transfer_config():  # type: ignore[no-untyped-def] # noqa: ANN202
    """Get boto3 transfer config for multipart uploads.

    Returns:
        TransferConfig with 500MB multipart threshold and chunk size

    """
    from boto3.s3.transfer import TransferConfig

    # Use 500MB chunks to match ETag calculation
    chunk_size_mb = 500
    chunk_size_bytes = chunk_size_mb * 1024 * 1024

    return TransferConfig(
        multipart_threshold=chunk_size_bytes,
        multipart_chunksize=chunk_size_bytes,
        use_threads=True,
        max_concurrency=10,
    )
