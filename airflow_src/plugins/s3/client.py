"""Client configuration and interface for S3 operations."""

import logging
from pathlib import Path

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook, BaseAwsConnection
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError


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


# sentinel value to indicate that a file was not found on s3
S3_FILE_NOT_FOUND_ETAG = object()


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
