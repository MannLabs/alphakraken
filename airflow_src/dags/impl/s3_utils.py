"""Utility functions for S3 operations."""

import hashlib
import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook, BaseAwsConnection
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

# S3 bucket name length limits
S3_MAX_BUCKET_NAME_LENGTH = 63


def get_s3_client(region: str, aws_conn_id: str = "aws_default") -> BaseAwsConnection:
    """Get boto3 S3 client using Airflow AWS connection.

    Args:
        region: AWS region name
        aws_conn_id: Airflow connection ID for AWS credentials (default: aws_default)

    Returns:
        boto3 S3 client

    """
    hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="s3", region_name=region)
    return hook.get_conn()


def get_transfer_config(chunk_size_mb: int) -> TransferConfig:
    """Get boto3 transfer config for multipart uploads.

    Arguments:
    ---------
        chunk_size_mb: Chunk size in MB

    Returns:
    -------
        TransferConfig with 500MB multipart threshold and chunk size

    """
    chunk_size_bytes = chunk_size_mb * 1024 * 1024

    return TransferConfig(
        multipart_threshold=chunk_size_bytes,
        multipart_chunksize=chunk_size_bytes,
        use_threads=True,
        max_concurrency=10,
    )


# TODO: -> shared
def normalize_bucket_name(project_id: str, bucket_prefix: str) -> str:
    """Normalize bucket name to be S3-compatible.

    S3 bucket naming rules:
    - Must be between 3 and 63 characters long
    - Can contain lowercase letters, numbers, hyphens
    - Must begin and end with a letter or number

    Args:
        project_id: Project identifier
        bucket_prefix: Bucket prefix from config

    Returns:
        Normalized bucket name: {bucket_prefix}-{normalized_project_id}

    """
    safe_project_id = _normalize_for_s3(project_id)
    bucket_name = f"{bucket_prefix}-{safe_project_id}".lower()

    # Ensure bucket name is valid length
    if len(bucket_name) > S3_MAX_BUCKET_NAME_LENGTH:
        raise AirflowFailException(
            f"Bucket name '{bucket_name}' exceeds {S3_MAX_BUCKET_NAME_LENGTH} characters."
        )

    return bucket_name


def _normalize_for_s3(identifier: str) -> str:
    """Normalize identifier for S3 compatibility.

    Converts to lowercase and replaces special characters with hyphens.
    """
    return identifier.lower().replace("_", "-").replace(".", "-").replace(" ", "-")


def bucket_exists(bucket_name: str, s3_client: BaseAwsConnection) -> tuple[bool, str]:
    """Check if S3 bucket exists and is accessible.

    Args:
        bucket_name: Name of the S3 bucket
        s3_client: Boto3 S3 client
    Returns:
        Tuple of (exists: bool, error_message: str)

    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} exists and is accessible")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "n/a")
        if error_code == "404":
            msg = f"Bucket {bucket_name} does not exist. Please create it before uploading."
        else:
            msg = f"Cannot access bucket {bucket_name}: {error_code} - {e}"

        return False, msg
    else:
        return True, ""


def is_upload_needed(
    bucket_name: str, s3_key: str, local_etag: str, s3_client: BaseAwsConnection
) -> bool:
    """Determine if upload to S3 is needed based on ETag comparison.

    Args:
        bucket_name: Name of the S3 bucket
        s3_key: S3 object key
        local_etag: ETag of the local file
        s3_client: Boto3 S3 client
    Returns:
        True if upload is needed, False if not
    Raises:
        ValueError if ETag mismatch occurs

    """
    existing_etag = get_etag(bucket_name, s3_key, s3_client)
    if existing_etag is _FILE_NOT_FOUND:
        logging.info(f"File {s3_key} does not exist in bucket, proceeding with upload")
        return True
    if local_etag == existing_etag:
        logging.info(
            f"File {s3_key} already exists with matching ETag, skipping upload"
        )
        return False

    raise ValueError(
        f"File {s3_key} exists but ETag mismatch (local: {local_etag}, remote: {existing_etag})"
    )


# sentinel value to indicate that a file was not found on s3
_FILE_NOT_FOUND = object()


def get_etag(
    bucket_name: str, s3_key: str, s3_client: BaseAwsConnection
) -> str | object:
    """Get ETag of an S3 object.

    Args:
        bucket_name: Name of the S3 bucket
        s3_key: S3 object key
        s3_client: Boto3 S3 client
    Returns:
        ETag string if object exists, None if not

    """
    try:
        existing_etag = (
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            .get("ETag", "")
            .strip('"')
        )
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code != "404":
            raise ValueError from e
        existing_etag = _FILE_NOT_FOUND

    return existing_etag


def calculate_s3_etag(file_path: Path, chunk_size_mb: int) -> str:
    """Calculate ETag for a file matching S3 multipart upload format.

    For files uploaded with multipart upload, S3 calculates the ETag as:
    - MD5 of each part
    - MD5 of concatenated MD5 hashes
    - Format: "{hash}-{part_count}"

    Args:
        file_path: Path to the file
        chunk_size_mb: Chunk size in MB (default 500MB to match upload chunk size)

    Returns:
        ETag string in format matching S3 multipart upload

    """
    chunk_size_bytes = chunk_size_mb * 1024 * 1024
    md5_hashes = []

    with file_path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size_bytes)
            if not chunk:
                break
            md5_hashes.append(hashlib.md5(chunk).digest())  # noqa: S324

    if len(md5_hashes) == 0:
        # Empty file
        return hashlib.md5(b"").hexdigest()  # noqa: S324
    if len(md5_hashes) == 1:
        # Single part - just MD5 without part count
        return md5_hashes[0].hex()
    # Multipart - MD5 of concatenated hashes with part count
    combined_hash = hashlib.md5(b"".join(md5_hashes)).hexdigest()  # noqa: S324
    return f"{combined_hash}-{len(md5_hashes)}"


def upload_file_to_s3(
    file_path: Path,
    bucket_name: str,
    s3_key: str,
    transfer_config: TransferConfig,
    s3_client: BaseAwsConnection,
) -> None:
    """Upload a file to S3 using the provided transfer config.

    Args:
        file_path: Path to the local file
        bucket_name: Name of the S3 bucket
        s3_key: S3 object key
        transfer_config: Boto3 TransferConfig for upload
        s3_client: Boto3 S3 client

    """
    with file_path.open("rb") as f:
        s3_client.upload_fileobj(
            f,
            bucket_name,
            s3_key,
            Config=transfer_config,
        )
