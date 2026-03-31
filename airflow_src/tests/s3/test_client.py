"""Unit tests for client.py."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from plugins.s3.client import (
    BUCKET_CONFIG_MAX_RETRIES,
    BUCKET_TAG_MANAGED_BY_KEY,
    BUCKET_TAG_MANAGED_BY_VALUE,
    S3_FILE_NOT_FOUND_ETAG,
    SSE_ALGORITHM_AES256,
    BucketConfigurationError,
    _configure_bucket,
    bucket_exists,
    create_bucket,
    get_etag,
    get_s3_client,
    get_transfer_config,
    upload_file_to_s3,
)


def test_get_s3_client_should_return_client_with_default_connection() -> None:
    """Test get_s3_client returns client with default AWS connection."""
    # given
    region = "us-east-1"
    mock_client = MagicMock()

    with patch("plugins.s3.client.AwsBaseHook") as mock_hook:
        mock_hook.return_value.get_conn.return_value = mock_client

        # when
        result = get_s3_client(region)

        # then
        mock_hook.assert_called_once_with(
            aws_conn_id="aws_default", client_type="s3", region_name=region
        )
        mock_hook.return_value.get_conn.assert_called_once()
        assert result == mock_client


def test_get_s3_client_should_return_client_with_custom_connection() -> None:
    """Test get_s3_client returns client with custom AWS connection."""
    # given
    region = "eu-west-1"
    aws_conn_id = "my_custom_connection"
    mock_client = MagicMock()

    with patch("plugins.s3.client.AwsBaseHook") as mock_hook:
        mock_hook.return_value.get_conn.return_value = mock_client

        # when
        result = get_s3_client(region, aws_conn_id)

        # then
        mock_hook.assert_called_once_with(
            aws_conn_id=aws_conn_id, client_type="s3", region_name=region
        )
        assert result == mock_client


def test_get_transfer_config_should_return_config_with_correct_chunk_size() -> None:
    """Test get_transfer_config returns config with correct chunk size."""
    # given
    chunk_size_mb = 100

    # when
    result = get_transfer_config(chunk_size_mb)

    # then
    expected_bytes = 100 * 1024 * 1024
    assert isinstance(result, TransferConfig)
    assert result.multipart_threshold == expected_bytes
    assert result.multipart_chunksize == expected_bytes
    assert result.use_threads is True
    assert result.max_concurrency == 10


def test_get_transfer_config_should_return_config_with_small_chunk_size() -> None:
    """Test get_transfer_config returns config with small chunk size."""
    # given
    chunk_size_mb = 5

    # when
    result = get_transfer_config(chunk_size_mb)

    # then
    expected_bytes = 5 * 1024 * 1024
    assert result.multipart_threshold == expected_bytes
    assert result.multipart_chunksize == expected_bytes


def test_bucket_exists_should_return_true_when_bucket_exists() -> None:
    """Test bucket_exists returns True when bucket exists."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.return_value = {}

    # when
    exists, error_msg, error_code = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is True
    assert error_msg == ""
    assert error_code == ""
    mock_s3_client.head_bucket.assert_called_once_with(Bucket=bucket_name)


def test_bucket_exists_should_return_false_when_bucket_not_found() -> None:
    """Test bucket_exists returns False when bucket does not exist."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "head_bucket"
    )

    # when
    exists, error_msg, error_code = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "does not exist" in error_msg
    assert bucket_name in error_msg
    assert error_code == "404"


def test_bucket_exists_should_return_false_when_access_denied() -> None:
    """Test bucket_exists returns False when access is denied."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "403"}}, "head_bucket"
    )

    # when
    exists, error_msg, error_code = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "Cannot access" in error_msg
    assert "403" in error_msg
    assert error_code == "403"


def test_bucket_exists_should_return_false_for_generic_client_error() -> None:
    """Test bucket_exists returns False for generic ClientError."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "500"}}, "head_bucket"
    )

    # when
    exists, error_msg, error_code = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "Cannot access" in error_msg
    assert error_code == "500"


def test_bucket_exists_should_handle_error_without_code() -> None:
    """Test bucket_exists handles error response without Code field."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = ClientError({"Error": {}}, "head_bucket")

    # when
    exists, error_msg, error_code = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "n/a" in error_msg
    assert error_code == "n/a"


def test_get_etag_should_return_etag_when_object_exists() -> None:
    """Test get_etag returns ETag when object exists."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    mock_s3_client = MagicMock()
    mock_s3_client.head_object.return_value = {
        "ETag": '"abc123"',
        "ContentLength": 1024,
    }

    # when
    result = get_etag(bucket_name, s3_key, mock_s3_client)

    # then
    assert result == ("abc123", 1024)
    mock_s3_client.head_object.assert_called_once_with(Bucket=bucket_name, Key=s3_key)


def test_get_etag_should_strip_quotes_from_etag() -> None:
    """Test get_etag strips quotes from ETag."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    mock_s3_client = MagicMock()
    mock_s3_client.head_object.return_value = {
        "ETag": '"def456"',
        "ContentLength": 2048,
    }

    # when
    result = get_etag(bucket_name, s3_key, mock_s3_client)

    # then
    assert result == ("def456", 2048)


def test_get_etag_should_return_sentinel_when_object_not_found() -> None:
    """Test get_etag returns sentinel when object does not exist."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    mock_s3_client = MagicMock()
    mock_s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "head_object"
    )

    # when
    result = get_etag(bucket_name, s3_key, mock_s3_client)

    # then
    assert result == (S3_FILE_NOT_FOUND_ETAG, -1)


def test_get_etag_should_raise_error_for_non_404_error() -> None:
    """Test get_etag raises error for non-404 ClientError."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    mock_s3_client = MagicMock()
    mock_s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "403"}}, "head_object"
    )

    # when / then
    with pytest.raises(ValueError):
        get_etag(bucket_name, s3_key, mock_s3_client)


def test_get_etag_should_handle_empty_etag() -> None:
    """Test get_etag handles empty ETag field."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    mock_s3_client = MagicMock()
    mock_s3_client.head_object.return_value = {}

    # when
    result = get_etag(bucket_name, s3_key, mock_s3_client)

    # then
    assert result == ("", -1)


def test_upload_file_to_s3_should_upload_file() -> None:
    """Test upload_file_to_s3 uploads file using transfer config."""
    # given
    file_path = Path("/tmp/test-file.txt")  # noqa: S108
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    transfer_config = TransferConfig()
    mock_s3_client = MagicMock()
    mock_file = MagicMock()

    with patch.object(Path, "open", return_value=mock_file):
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)

        # when
        upload_file_to_s3(
            file_path, bucket_name, s3_key, transfer_config, mock_s3_client
        )

        # then
        mock_s3_client.upload_fileobj.assert_called_once_with(
            mock_file, bucket_name, s3_key, Config=transfer_config
        )


def test_upload_file_to_s3_should_open_file_in_binary_mode() -> None:
    """Test upload_file_to_s3 opens file in binary read mode."""
    # given
    file_path = Path("/tmp/test-file.txt")  # noqa: S108
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    transfer_config = TransferConfig()
    mock_s3_client = MagicMock()
    mock_file = MagicMock()

    with patch.object(Path, "open", return_value=mock_file) as mock_open:
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)

        # when
        upload_file_to_s3(
            file_path, bucket_name, s3_key, transfer_config, mock_s3_client
        )

        # then
        mock_open.assert_called_once_with("rb")


def test_create_bucket_should_create_bucket_with_location_constraint() -> None:
    """Test create_bucket sets LocationConstraint for non-us-east-1 regions."""
    # given
    mock_s3_client = MagicMock()

    # when
    create_bucket("test-bucket", "eu-central-1", mock_s3_client)

    # then
    mock_s3_client.create_bucket.assert_called_once_with(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
    )


def test_create_bucket_should_set_no_location_constraint_for_us_east_1() -> None:
    """Test create_bucket sets no LocationConstraint for us-east-1."""
    # given
    mock_s3_client = MagicMock()

    # when
    create_bucket("test-bucket", "us-east-1", mock_s3_client)

    # then
    mock_s3_client.create_bucket.assert_called_once_with(
        Bucket="test-bucket",
    )


def test_create_bucket_should_enable_versioning() -> None:
    """Test create_bucket enables versioning on the bucket."""
    # given
    mock_s3_client = MagicMock()

    # when
    create_bucket("test-bucket", "eu-central-1", mock_s3_client)

    # then
    mock_s3_client.put_bucket_versioning.assert_called_once_with(
        Bucket="test-bucket",
        VersioningConfiguration={"Status": "Enabled"},
    )


def test_create_bucket_should_set_aes256_encryption() -> None:
    """Test create_bucket configures AES256 server-side encryption."""
    # given
    mock_s3_client = MagicMock()

    # when
    create_bucket("test-bucket", "eu-central-1", mock_s3_client)

    # then
    mock_s3_client.put_bucket_encryption.assert_called_once_with(
        Bucket="test-bucket",
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


def test_create_bucket_should_block_all_public_access() -> None:
    """Test create_bucket blocks all public access."""
    # given
    mock_s3_client = MagicMock()

    # when
    create_bucket("test-bucket", "eu-central-1", mock_s3_client)

    # then
    mock_s3_client.put_public_access_block.assert_called_once_with(
        Bucket="test-bucket",
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def test_create_bucket_should_tag_with_managed_by() -> None:
    """Test create_bucket sets ManagedBy tag."""
    # given
    mock_s3_client = MagicMock()

    # when
    create_bucket("test-bucket", "eu-central-1", mock_s3_client)

    # then
    mock_s3_client.put_bucket_tagging.assert_called_once_with(
        Bucket="test-bucket",
        Tagging={
            "TagSet": [
                {
                    "Key": BUCKET_TAG_MANAGED_BY_KEY,
                    "Value": BUCKET_TAG_MANAGED_BY_VALUE,
                },
            ]
        },
    )


def test_create_bucket_should_handle_bucket_already_owned_by_you() -> None:
    """Test create_bucket handles BucketAlreadyOwnedByYou from concurrent creation."""
    # given
    mock_s3_client = MagicMock()
    mock_s3_client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketAlreadyOwnedByYou"}}, "create_bucket"
    )

    # when
    create_bucket("test-bucket", "eu-central-1", mock_s3_client)

    # then: should attempt configuration
    mock_s3_client.put_bucket_versioning.assert_called_once()
    mock_s3_client.put_bucket_encryption.assert_called_once()
    mock_s3_client.put_public_access_block.assert_called_once()
    mock_s3_client.put_bucket_tagging.assert_called_once()


def test_create_bucket_should_raise_on_other_client_error() -> None:
    """Test create_bucket raises on non-BucketAlreadyOwnedByYou errors."""
    # given
    mock_s3_client = MagicMock()
    mock_s3_client.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied"}}, "create_bucket"
    )

    # when / then
    with pytest.raises(ClientError):
        create_bucket("test-bucket", "eu-central-1", mock_s3_client)


def test_configure_bucket_should_apply_all_settings() -> None:
    """Test _configure_bucket applies versioning, encryption, public access block, and tags."""
    # given
    mock_s3_client = MagicMock()

    # when
    _configure_bucket("test-bucket", mock_s3_client)

    # then
    mock_s3_client.put_bucket_versioning.assert_called_once()
    mock_s3_client.put_bucket_encryption.assert_called_once()
    mock_s3_client.put_public_access_block.assert_called_once()
    mock_s3_client.put_bucket_tagging.assert_called_once()


def test_configure_bucket_should_retry_on_failure_and_succeed() -> None:
    """Test _configure_bucket retries on failure and succeeds on next attempt."""
    # given
    mock_s3_client = MagicMock()
    mock_s3_client.put_bucket_encryption.side_effect = [
        ClientError({"Error": {"Code": "500"}}, "put_bucket_encryption"),
        None,
    ]

    # when
    _configure_bucket("test-bucket", mock_s3_client)

    # then: first attempt: versioning OK, encryption FAILED → retry
    # second attempt: all OK
    assert mock_s3_client.put_bucket_versioning.call_count == 2
    assert mock_s3_client.put_bucket_encryption.call_count == 2
    assert mock_s3_client.put_bucket_tagging.call_count == 1


def test_configure_bucket_should_raise_with_status_after_max_retries() -> None:
    """Test _configure_bucket raises BucketConfigurationError with completed steps."""
    # given
    mock_s3_client = MagicMock()
    mock_s3_client.put_public_access_block.side_effect = ClientError(
        {"Error": {"Code": "500"}}, "put_public_access_block"
    )

    # when / then
    with pytest.raises(BucketConfigurationError) as exc_info:
        _configure_bucket("test-bucket", mock_s3_client)

    error_msg = str(exc_info.value)
    assert "put_bucket_versioning: OK" in error_msg
    assert "put_bucket_encryption: OK" in error_msg
    assert "put_public_access_block: FAILED or SKIPPED" in error_msg
    assert "put_bucket_tagging: FAILED or SKIPPED" in error_msg
    assert (
        mock_s3_client.put_public_access_block.call_count == BUCKET_CONFIG_MAX_RETRIES
    )
