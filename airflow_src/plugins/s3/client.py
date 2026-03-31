"""Client configuration and interface for S3 operations."""

import logging
from pathlib import Path

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook, BaseAwsConnection
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# sentinel value to indicate that a file was not found on s3
S3_FILE_NOT_FOUND_ETAG = object()

S3_BUCKET_NOT_FOUND_ERROR_CODE = "404"
SSE_ALGORITHM_AES256 = "AES256"
BUCKET_TAG_MANAGED_BY_KEY = "ManagedBy"
BUCKET_TAG_MANAGED_BY_VALUE = "alphakraken"
BUCKET_CONFIG_MAX_RETRIES = 3


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


def bucket_exists(
    bucket_name: str, s3_client: BaseAwsConnection
) -> tuple[bool, str, str]:
    """Check if S3 bucket exists and is accessible.

    Args:
        bucket_name: Name of the S3 bucket
        s3_client: Boto3 S3 client
    Returns:
        Tuple of (exists, error_message, error_code)

    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)  # ty: ignore[unresolved-attribute]
        logging.info(f"Bucket {bucket_name} exists and is accessible")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "n/a")
        if error_code == S3_BUCKET_NOT_FOUND_ERROR_CODE:
            msg = f"Bucket {bucket_name} does not exist."
        else:
            msg = f"Cannot access bucket {bucket_name}: {error_code} - {e}"

        return False, msg, error_code
    else:
        return True, "", ""


def create_bucket(bucket_name: str, region: str, s3_client: BaseAwsConnection) -> None:
    """Create an S3 bucket with versioning, encryption, and public access block.

    Note: if one of the request fail, the bucket may be left in a partially configured state.
    This is not ideal but simplifies the code. In such cases, manually add the desired properties to the bucket.

    Args:
        bucket_name: Name of the S3 bucket to create
        region: AWS region for the bucket
        s3_client: Boto3 S3 client

    """
    create_kwargs: dict = {"Bucket": bucket_name}
    if region != "us-east-1":
        # cf. https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucketConfiguration.html -> "Valid Values" of LocationConstraint
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}

    try:
        s3_client.create_bucket(**create_kwargs)  # ty: ignore[unresolved-attribute]
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code != "BucketAlreadyOwnedByYou":
            raise

        logging.warning(
            f"Bucket {bucket_name} was already created by a concurrent process"
        )
    else:
        logging.info(f"Created bucket {bucket_name} in {region}")

    _configure_bucket(bucket_name, s3_client)


class BucketConfigurationError(Exception):
    """Raised when bucket configuration fails, with per-step status detail."""


@retry(
    stop=stop_after_attempt(BUCKET_CONFIG_MAX_RETRIES),
    wait=wait_exponential(multiplier=1, max=10),
    retry=retry_if_exception_type(BucketConfigurationError),
    reraise=True,
)
def _configure_bucket(bucket_name: str, s3_client: BaseAwsConnection) -> None:
    """Apply versioning, encryption, public access block, and tags to a bucket.

    All steps are idempotent, so the entire block is safe to retry.

    """
    status = {
        "put_bucket_versioning": "FAILED or SKIPPED",
        "put_bucket_encryption": "FAILED or SKIPPED",
        "put_public_access_block": "FAILED or SKIPPED",
        "put_bucket_tagging": "FAILED or SKIPPED",
    }

    logging.info(f"Configuring bucket {bucket_name} ..")
    try:
        logging.info("put_bucket_versioning ..")
        s3_client.put_bucket_versioning(  # ty: ignore[unresolved-attribute]
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        status["put_bucket_versioning"] = "OK"

        logging.info("put_bucket_encryption ..")
        s3_client.put_bucket_encryption(  # ty: ignore[unresolved-attribute]
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": SSE_ALGORITHM_AES256,
                        }
                    }
                ]
            },
        )
        status["put_bucket_encryption"] = "OK"

        logging.info("put_public_access_block ..")
        s3_client.put_public_access_block(  # ty: ignore[unresolved-attribute]
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        status["put_public_access_block"] = "OK"

        logging.info("put_bucket_tagging ..")
        s3_client.put_bucket_tagging(  # ty: ignore[unresolved-attribute]
            Bucket=bucket_name,
            Tagging={
                "TagSet": [
                    {
                        "Key": BUCKET_TAG_MANAGED_BY_KEY,
                        "Value": BUCKET_TAG_MANAGED_BY_VALUE,
                    },
                ]
            },
        )
        status["put_bucket_tagging"] = "OK"
    except ClientError as e:
        status_report = "\n -".join(f"{k}: {v}" for k, v in status.items())
        logging.warning(
            f"Please add the required bucket properties for {bucket_name} manually and ensure the bucket is properly configured."
        )
        raise BucketConfigurationError(
            f"Bucket {bucket_name} configuration failed: \n - {status_report}"
        ) from e

    status_report = "\n - ".join(f"{k}: {v}" for k, v in status.items())
    logging.info(f"Configured bucket {bucket_name}: \n - {status_report}")


def get_etag(
    bucket_name: str, s3_key: str, s3_client: BaseAwsConnection
) -> tuple[str | object, int]:
    """Get ETag and content length of an S3 object.

    Args:
        bucket_name: Name of the S3 bucket
        s3_key: S3 object key
        s3_client: Boto3 S3 client
    Returns:
        Tuple of (etag, content_length). On 404, returns (S3_FILE_NOT_FOUND_ETAG, -1).

    """
    try:
        response = s3_client.head_object(  # ty: ignore[unresolved-attribute]
            Bucket=bucket_name, Key=s3_key
        )
        etag = response.get("ETag", "").strip('"')
        content_length = response.get("ContentLength", -1)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code != "404":
            raise ValueError from e
        etag = S3_FILE_NOT_FOUND_ETAG
        content_length = -1

    return etag, content_length


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
    logging.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")

    with file_path.open("rb") as f:
        s3_client.upload_fileobj(  # ty: ignore[unresolved-attribute]
            f,
            bucket_name,
            s3_key,
            Config=transfer_config,
        )
