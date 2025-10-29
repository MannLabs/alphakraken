"""Utility functions for S3 operations."""

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

# S3 bucket name length limits
S3_MAX_BUCKET_NAME_LENGTH = 63


def _normalize_for_s3(identifier: str) -> str:
    """Normalize identifier for S3 compatibility.

    Converts to lowercase and replaces special characters with hyphens.
    """
    return identifier.lower().replace("_", "-").replace(".", "-").replace(" ", "-")


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


def get_s3_client(region: str, aws_conn_id: str = "aws_default") -> "S3Client":
    """Get boto3 S3 client using Airflow AWS connection.

    Args:
        region: AWS region name
        aws_conn_id: Airflow connection ID for AWS credentials (default: aws_default)

    Returns:
        boto3 S3 client

    """
    hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="s3", region_name=region)
    return hook.get_conn()


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
