"""Unit tests for client.py."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from plugins.s3.client import (
    S3_FILE_NOT_FOUND_ETAG,
    bucket_exists,
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
    exists, error_msg = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is True
    assert error_msg == ""
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
    exists, error_msg = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "does not exist" in error_msg
    assert bucket_name in error_msg


def test_bucket_exists_should_return_false_when_access_denied() -> None:
    """Test bucket_exists returns False when access is denied."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "403"}}, "head_bucket"
    )

    # when
    exists, error_msg = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "Cannot access" in error_msg
    assert "403" in error_msg


def test_bucket_exists_should_return_false_for_generic_client_error() -> None:
    """Test bucket_exists returns False for generic ClientError."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "500"}}, "head_bucket"
    )

    # when
    exists, error_msg = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "Cannot access" in error_msg


def test_bucket_exists_should_handle_error_without_code() -> None:
    """Test bucket_exists handles error response without Code field."""
    # given
    bucket_name = "test-bucket"
    mock_s3_client = MagicMock()
    mock_s3_client.head_bucket.side_effect = ClientError({"Error": {}}, "head_bucket")

    # when
    exists, error_msg = bucket_exists(bucket_name, mock_s3_client)

    # then
    assert exists is False
    assert "n/a" in error_msg


def test_get_etag_should_return_etag_when_object_exists() -> None:
    """Test get_etag returns ETag when object exists."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    mock_s3_client = MagicMock()
    mock_s3_client.head_object.return_value = {"ETag": '"abc123"'}

    # when
    result = get_etag(bucket_name, s3_key, mock_s3_client)

    # then
    assert result == "abc123"
    mock_s3_client.head_object.assert_called_once_with(Bucket=bucket_name, Key=s3_key)


def test_get_etag_should_strip_quotes_from_etag() -> None:
    """Test get_etag strips quotes from ETag."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    mock_s3_client = MagicMock()
    mock_s3_client.head_object.return_value = {"ETag": '"def456"'}

    # when
    result = get_etag(bucket_name, s3_key, mock_s3_client)

    # then
    assert result == "def456"


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
    assert result is S3_FILE_NOT_FOUND_ETAG


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
    assert result == ""


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
