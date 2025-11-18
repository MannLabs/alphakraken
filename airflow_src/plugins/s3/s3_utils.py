"""Utility functions for S3 operations."""

import logging

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
from s3.client import S3_FILE_NOT_FOUND_ETAG, get_etag

# S3 bucket name length limits
S3_MAX_BUCKET_NAME_LENGTH = 63


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
    if existing_etag is S3_FILE_NOT_FOUND_ETAG:
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
