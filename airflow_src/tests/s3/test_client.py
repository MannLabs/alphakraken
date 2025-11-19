"""Unit tests for client.py."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from plugins.s3.client import (
    S3_FILE_NOT_FOUND_ETAG,
    bucket_exists,
    download_file_from_s3,
    get_etag,
    get_s3_client,
    get_transfer_config,
    parse_etag_from_file_info,
    reconstruct_s3_paths,
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


def test_download_file_from_s3_should_download_file() -> None:
    """Test download_file_from_s3 downloads file using transfer config."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    local_path = Path("/tmp/test-file.txt")  # noqa: S108
    region = "us-east-1"
    chunk_size_mb = 500
    mock_s3_client = MagicMock()
    mock_file = MagicMock()

    with (
        patch("dags.impl.s3_utils.get_s3_client") as mock_get_client,
        patch("dags.impl.s3_utils.get_transfer_config") as mock_get_config,
        patch.object(Path, "open", return_value=mock_file),
        patch.object(Path, "mkdir"),
    ):
        mock_get_client.return_value = mock_s3_client
        mock_transfer_config = TransferConfig()
        mock_get_config.return_value = mock_transfer_config
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)

        # when
        download_file_from_s3(bucket_name, s3_key, local_path, region, chunk_size_mb)

        # then
        mock_get_client.assert_called_once_with(region, "aws_default")
        mock_get_config.assert_called_once_with(chunk_size_mb)
        mock_s3_client.download_fileobj.assert_called_once_with(
            bucket_name, s3_key, mock_file, Config=mock_transfer_config
        )


def test_download_file_from_s3_should_create_parent_directories() -> None:
    """Test download_file_from_s3 creates parent directories if they don't exist."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    local_path = Path("/tmp/subdir/test-file.txt")  # noqa: S108
    region = "us-east-1"
    mock_s3_client = MagicMock()
    mock_file = MagicMock()

    with (
        patch("dags.impl.s3_utils.get_s3_client") as mock_get_client,
        patch("dags.impl.s3_utils.get_transfer_config"),
        patch.object(Path, "open", return_value=mock_file),
        patch.object(Path, "mkdir") as mock_mkdir,
    ):
        mock_get_client.return_value = mock_s3_client
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)

        # when
        download_file_from_s3(bucket_name, s3_key, local_path, region)

        # then
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)


def test_download_file_from_s3_should_open_file_in_binary_write_mode() -> None:
    """Test download_file_from_s3 opens file in binary write mode."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    local_path = Path("/tmp/test-file.txt")  # noqa: S108
    region = "us-east-1"
    mock_s3_client = MagicMock()
    mock_file = MagicMock()

    with (
        patch("dags.impl.s3_utils.get_s3_client") as mock_get_client,
        patch("dags.impl.s3_utils.get_transfer_config"),
        patch.object(Path, "open", return_value=mock_file) as mock_open,
        patch.object(Path, "mkdir"),
    ):
        mock_get_client.return_value = mock_s3_client
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)

        # when
        download_file_from_s3(bucket_name, s3_key, local_path, region)

        # then
        mock_open.assert_called_once_with("wb")


def test_download_file_from_s3_should_use_custom_aws_connection() -> None:
    """Test download_file_from_s3 uses custom AWS connection ID."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    local_path = Path("/tmp/test-file.txt")  # noqa: S108
    region = "eu-west-1"
    aws_conn_id = "my_custom_connection"
    mock_s3_client = MagicMock()
    mock_file = MagicMock()

    with (
        patch("dags.impl.s3_utils.get_s3_client") as mock_get_client,
        patch("dags.impl.s3_utils.get_transfer_config"),
        patch.object(Path, "open", return_value=mock_file),
        patch.object(Path, "mkdir"),
    ):
        mock_get_client.return_value = mock_s3_client
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)

        # when
        download_file_from_s3(
            bucket_name, s3_key, local_path, region, aws_conn_id=aws_conn_id
        )

        # then
        mock_get_client.assert_called_once_with(region, aws_conn_id)


def test_reconstruct_s3_paths_should_handle_bucket_only() -> None:
    """Test reconstruct_s3_paths handles s3://bucket format."""
    # given
    s3_upload_path = "s3://test-bucket"
    file_info = {
        "file1.raw": [1000, "hash1", "etag1__500"],
        "file2.raw": [2000, "hash2", "etag2__500"],
    }

    # when
    result = reconstruct_s3_paths(s3_upload_path, file_info)

    # then
    assert result == {
        "file1.raw": ("test-bucket", "file1.raw"),
        "file2.raw": ("test-bucket", "file2.raw"),
    }


def test_reconstruct_s3_paths_should_handle_bucket_with_trailing_slash() -> None:
    """Test reconstruct_s3_paths handles s3://bucket/ format."""
    # given
    s3_upload_path = "s3://test-bucket/"
    file_info = {"file1.raw": [1000, "hash1", "etag1__500"]}

    # when
    result = reconstruct_s3_paths(s3_upload_path, file_info)

    # then
    assert result == {"file1.raw": ("test-bucket", "file1.raw")}


def test_reconstruct_s3_paths_should_handle_bucket_with_prefix() -> None:
    """Test reconstruct_s3_paths handles s3://bucket/prefix format."""
    # given
    s3_upload_path = "s3://test-bucket/prefix"
    file_info = {
        "file1.raw": [1000, "hash1", "etag1__500"],
        "file2.raw": [2000, "hash2", "etag2__500"],
    }

    # when
    result = reconstruct_s3_paths(s3_upload_path, file_info)

    # then
    assert result == {
        "file1.raw": ("test-bucket", "prefix/file1.raw"),
        "file2.raw": ("test-bucket", "prefix/file2.raw"),
    }


def test_reconstruct_s3_paths_should_handle_bucket_with_prefix_trailing_slash() -> None:
    """Test reconstruct_s3_paths handles s3://bucket/prefix/ format."""
    # given
    s3_upload_path = "s3://test-bucket/prefix/"
    file_info = {"file1.raw": [1000, "hash1", "etag1__500"]}

    # when
    result = reconstruct_s3_paths(s3_upload_path, file_info)

    # then
    assert result == {"file1.raw": ("test-bucket", "prefix/file1.raw")}


def test_reconstruct_s3_paths_should_handle_nested_prefix() -> None:
    """Test reconstruct_s3_paths handles s3://bucket/path/to/prefix format."""
    # given
    s3_upload_path = "s3://test-bucket/path/to/prefix"
    file_info = {"file1.raw": [1000, "hash1", "etag1__500"]}

    # when
    result = reconstruct_s3_paths(s3_upload_path, file_info)

    # then
    assert result == {"file1.raw": ("test-bucket", "path/to/prefix/file1.raw")}


def test_reconstruct_s3_paths_should_raise_error_for_invalid_format() -> None:
    """Test reconstruct_s3_paths raises error for invalid S3 path format."""
    # given
    s3_upload_path = "invalid-path"
    file_info = {"file1.raw": [1000, "hash1", "etag1__500"]}

    # when / then
    with pytest.raises(ValueError) as exc_info:
        reconstruct_s3_paths(s3_upload_path, file_info)

    assert "Invalid S3 path format" in str(exc_info.value)


def test_parse_etag_from_file_info_should_extract_single_part_etag() -> None:
    """Test parse_etag_from_file_info extracts etag and chunk size from single-part file."""
    # given
    file_info_tuple = [1000, "hash123", "d8e8fca2dc0f896fd7cb4cb0031ba249__500"]

    # when
    etag_value, chunk_size_mb = parse_etag_from_file_info(file_info_tuple)

    # then
    assert etag_value == "d8e8fca2dc0f896fd7cb4cb0031ba249"
    assert chunk_size_mb == 500


def test_parse_etag_from_file_info_should_extract_multipart_etag() -> None:
    """Test parse_etag_from_file_info extracts etag and chunk size from multipart file."""
    # given
    file_info_tuple = [5000000, "hash456", "d8e8fca2dc0f896fd7cb4cb0031ba249-5__500"]

    # when
    etag_value, chunk_size_mb = parse_etag_from_file_info(file_info_tuple)

    # then
    assert etag_value == "d8e8fca2dc0f896fd7cb4cb0031ba249-5"
    assert chunk_size_mb == 500


def test_parse_etag_from_file_info_should_handle_different_chunk_sizes() -> None:
    """Test parse_etag_from_file_info handles different chunk sizes."""
    # given
    file_info_tuple = [1000, "hash789", "abc123__100"]

    # when
    etag_value, chunk_size_mb = parse_etag_from_file_info(file_info_tuple)

    # then
    assert etag_value == "abc123"
    assert chunk_size_mb == 100


def test_parse_etag_from_file_info_should_handle_tuple_format() -> None:
    """Test parse_etag_from_file_info handles tuple format (not just list)."""
    # given
    file_info_tuple = (1000, "hash123", "etag123__500")

    # when
    etag_value, chunk_size_mb = parse_etag_from_file_info(file_info_tuple)

    # then
    assert etag_value == "etag123"
    assert chunk_size_mb == 500


def test_parse_etag_from_file_info_should_raise_error_for_short_tuple() -> None:
    """Test parse_etag_from_file_info raises error when tuple has fewer than 3 elements."""
    # given
    file_info_tuple = [1000, "hash123"]

    # when / then
    with pytest.raises(ValueError) as exc_info:
        parse_etag_from_file_info(file_info_tuple)

    assert "Invalid file_info tuple length" in str(exc_info.value)
    assert "Expected at least 3 elements" in str(exc_info.value)


def test_parse_etag_from_file_info_should_raise_error_for_empty_etag() -> None:
    """Test parse_etag_from_file_info raises error when etag is empty."""
    # given
    file_info_tuple = [1000, "hash123", ""]

    # when / then
    with pytest.raises(ValueError) as exc_info:
        parse_etag_from_file_info(file_info_tuple)

    assert "ETag with chunk size is empty" in str(exc_info.value)


def test_parse_etag_from_file_info_should_raise_error_for_invalid_format() -> None:
    """Test parse_etag_from_file_info raises error when etag format is invalid."""
    # given
    file_info_tuple = [1000, "hash123", "invalid_etag_format"]

    # when / then
    with pytest.raises(ValueError) as exc_info:
        parse_etag_from_file_info(file_info_tuple)

    assert "Invalid etag format" in str(exc_info.value)
    assert "Expected format: 'etag__chunk_size'" in str(exc_info.value)


def test_parse_etag_from_file_info_should_raise_error_for_non_integer_chunk_size() -> (
    None
):
    """Test parse_etag_from_file_info raises error when chunk size is not an integer."""
    # given
    file_info_tuple = [1000, "hash123", "etag123__not_a_number"]

    # when / then
    with pytest.raises(ValueError) as exc_info:
        parse_etag_from_file_info(file_info_tuple)

    assert "Invalid chunk size in etag" in str(exc_info.value)
    assert "Must be an integer" in str(exc_info.value)


# List of additional test cases to implement:
# - test_get_s3_client_should_raise_error_when_connection_fails - tests AWS connection failure
# - test_get_transfer_config_should_handle_large_chunk_sizes - tests chunk size > 1GB
# - test_normalize_bucket_name_should_handle_empty_project_id - tests empty string handling
# - test_normalize_bucket_name_should_handle_only_special_characters - tests "___..." conversion
# - test_bucket_exists_should_handle_network_timeout - tests timeout scenarios
# - test_get_etag_should_handle_malformed_response - tests unexpected response format
# - test_is_upload_needed_should_handle_concurrent_modifications - tests race conditions
# - test_upload_file_to_s3_should_handle_file_not_found - tests missing file
# - test_upload_file_to_s3_should_handle_upload_failures - tests S3 upload errors
# - test_upload_file_to_s3_should_handle_permission_errors - tests file read permission issues
