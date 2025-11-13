"""Client configuration and interface for S3 operations."""

import logging
from pathlib import Path

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook, BaseAwsConnection
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

# sentinel value to indicate that a file was not found on s3
S3_FILE_NOT_FOUND_ETAG = object()


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
        existing_etag = S3_FILE_NOT_FOUND_ETAG

    return existing_etag


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


def download_file_from_s3(  # noqa: PLR0913
    bucket_name: str,
    s3_key: str,
    local_path: Path,
    region: str,
    chunk_size_mb: int = 500,
    aws_conn_id: str = "aws_default",
) -> None:
    """Download a single file from S3 using multipart download.

    Args:
        bucket_name: S3 bucket name
        s3_key: S3 object key
        local_path: Local filesystem destination path
        region: AWS region
        chunk_size_mb: Chunk size for multipart download (default 500)
        aws_conn_id: Airflow connection ID for AWS credentials (default aws_default)

    Raises:
        ClientError: If S3 operation fails
        IOError: If local filesystem operation fails

    """
    s3_client = get_s3_client(region, aws_conn_id)
    transfer_config = get_transfer_config(chunk_size_mb)

    # Create parent directories if they don't exist
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Download using multipart download
    with local_path.open("wb") as f:
        s3_client.download_fileobj(
            bucket_name,
            s3_key,
            f,
            Config=transfer_config,
        )


def reconstruct_s3_paths(
    s3_upload_path: str, file_info: dict
) -> dict[str, tuple[str, str]]:
    """Reconstruct S3 bucket and key for each file in file_info.

    Args:
        s3_upload_path: From RawFile.s3_upload_path (e.g., "s3://bucket", "s3://bucket/prefix/")
        file_info: RawFile.file_info dict with relative paths as keys

    Returns:
        Dict mapping relative_path -> (bucket_name, s3_key)
        Example: {"file.raw": ("alphakraken-PRID001", "file.raw")}

    """
    # Parse s3_upload_path to extract bucket and optional prefix
    # Format: "s3://bucket" or "s3://bucket/" or "s3://bucket/prefix/"
    if not s3_upload_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path format: {s3_upload_path}")

    # Remove "s3://" prefix
    path_without_protocol = s3_upload_path[5:]

    # Split into bucket and prefix
    parts = path_without_protocol.split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 and parts[1] else ""

    # Construct full S3 key for each file
    result = {}
    for relative_path in file_info:
        s3_key = f"{prefix}/{relative_path}" if prefix else relative_path
        result[relative_path] = (bucket_name, s3_key)

    return result


def parse_etag_from_file_info(file_info_tuple: tuple | list) -> tuple[str, int]:
    """Extract etag value and chunk_size from file_info tuple.

    Args:
        file_info_tuple: The 3-tuple from file_info dict (size, md5_hash, etag_with_chunk)

    Returns:
        Tuple of (etag_value: str, chunk_size_mb: int)
        Example: ("d8e8fca2dc0f896fd7cb4cb0031ba249-5", 500)

    Raises:
        ValueError: If tuple format is invalid

    """
    if len(file_info_tuple) < 3:  # noqa: PLR2004
        raise ValueError(
            f"Invalid file_info tuple length {len(file_info_tuple)}. Expected at least 3 elements."
        )

    etag_with_chunk = file_info_tuple[2]
    if not etag_with_chunk:
        raise ValueError("ETag with chunk size is empty")

    # Split on ETAG_SEPARATOR ("__")
    parts = etag_with_chunk.split("__")
    if len(parts) != 2:  # noqa: PLR2004
        raise ValueError(
            f"Invalid etag format: {etag_with_chunk}. Expected format: 'etag__chunk_size'"
        )

    etag_value = parts[0]
    try:
        chunk_size_mb = int(parts[1])
    except ValueError as e:
        raise ValueError(
            f"Invalid chunk size in etag: {parts[1]}. Must be an integer."
        ) from e

    return etag_value, chunk_size_mb
