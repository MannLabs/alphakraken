"""Unit tests for s3_utils.py."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.exceptions import AirflowFailException
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from plugins.s3.s3_utils import (
    S3_MAX_BUCKET_NAME_LENGTH,
    _normalize_for_s3,
    is_upload_needed,
    normalize_bucket_name,
)
from plugins.s3.client import S3_FILE_NOT_FOUND_ETAG


# TODO: fix
# ruff: noqa


def test_normalize_for_s3_should_convert_to_lowercase() -> None:
    """Test _normalize_for_s3 converts string to lowercase."""
    # given
    identifier = "MyProject"

    # when
    result = _normalize_for_s3(identifier)

    # then
    assert result == "myproject"


def test_normalize_for_s3_should_replace_underscores_with_hyphens() -> None:
    """Test _normalize_for_s3 replaces underscores with hyphens."""
    # given
    identifier = "my_project_name"

    # when
    result = _normalize_for_s3(identifier)

    # then
    assert result == "my-project-name"


def test_normalize_for_s3_should_replace_dots_with_hyphens() -> None:
    """Test _normalize_for_s3 replaces dots with hyphens."""
    # given
    identifier = "my.project.name"

    # when
    result = _normalize_for_s3(identifier)

    # then
    assert result == "my-project-name"


def test_normalize_for_s3_should_replace_spaces_with_hyphens() -> None:
    """Test _normalize_for_s3 replaces spaces with hyphens."""
    # given
    identifier = "my project name"

    # when
    result = _normalize_for_s3(identifier)

    # then
    assert result == "my-project-name"


def test_normalize_for_s3_should_handle_multiple_special_characters() -> None:
    """Test _normalize_for_s3 handles multiple special characters."""
    # given
    identifier = "My_Project.Name Test"

    # when
    result = _normalize_for_s3(identifier)

    # then
    assert result == "my-project-name-test"


def test_normalize_bucket_name_should_return_valid_bucket_name() -> None:
    """Test normalize_bucket_name returns valid bucket name."""
    # given
    project_id = "MyProject"
    bucket_prefix = "data"

    # when
    result = normalize_bucket_name(project_id, bucket_prefix)

    # then
    assert result == "data-myproject"


def test_normalize_bucket_name_should_handle_special_characters() -> None:
    """Test normalize_bucket_name handles special characters in project ID."""
    # given
    project_id = "My_Project.Test"
    bucket_prefix = "data"

    # when
    result = normalize_bucket_name(project_id, bucket_prefix)

    # then
    assert result == "data-my-project-test"


def test_normalize_bucket_name_should_raise_exception_when_name_too_long() -> None:
    """Test normalize_bucket_name raises exception when name exceeds 63 characters."""
    # given
    project_id = "a" * 60
    bucket_prefix = "prefix"

    # when / then
    with pytest.raises(AirflowFailException) as exc_info:
        normalize_bucket_name(project_id, bucket_prefix)

    assert f"exceeds {S3_MAX_BUCKET_NAME_LENGTH} characters" in str(exc_info.value)


def test_normalize_bucket_name_should_accept_maximum_valid_length() -> None:
    """Test normalize_bucket_name accepts name with maximum valid length."""
    # given
    # Create a bucket name that is exactly 63 characters
    bucket_prefix = "prefix"
    project_id = "a" * (S3_MAX_BUCKET_NAME_LENGTH - len(bucket_prefix) - 1)

    # when
    result = normalize_bucket_name(project_id, bucket_prefix)

    # then
    assert len(result) == S3_MAX_BUCKET_NAME_LENGTH
    assert result.startswith("prefix-")


def test_is_upload_needed_should_return_true_when_file_not_found() -> None:
    """Test is_upload_needed returns True when file does not exist on S3."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    local_etag = "abc123"
    mock_s3_client = MagicMock()

    with patch("plugins.s3.s3_utils.get_etag") as mock_get_etag:
        mock_get_etag.return_value = S3_FILE_NOT_FOUND_ETAG

        # when
        result = is_upload_needed(bucket_name, s3_key, local_etag, mock_s3_client)

        # then
        assert result is True
        mock_get_etag.assert_called_once_with(bucket_name, s3_key, mock_s3_client)


def test_is_upload_needed_should_return_false_when_etag_matches() -> None:
    """Test is_upload_needed returns False when ETags match."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    local_etag = "abc123"
    mock_s3_client = MagicMock()

    with patch("plugins.s3.s3_utils.get_etag") as mock_get_etag:
        mock_get_etag.return_value = "abc123"

        # when
        result = is_upload_needed(bucket_name, s3_key, local_etag, mock_s3_client)

        # then
        assert result is False


def test_is_upload_needed_should_raise_error_when_etag_mismatch() -> None:
    """Test is_upload_needed raises error when ETags mismatch."""
    # given
    bucket_name = "test-bucket"
    s3_key = "test-file.txt"
    local_etag = "abc123"
    mock_s3_client = MagicMock()

    with patch("plugins.s3.s3_utils.get_etag") as mock_get_etag:
        mock_get_etag.return_value = "different_etag"

        # when / then
        with pytest.raises(ValueError) as exc_info:
            is_upload_needed(bucket_name, s3_key, local_etag, mock_s3_client)

        assert "ETag mismatch" in str(exc_info.value)
        assert "abc123" in str(exc_info.value)
        assert "different_etag" in str(exc_info.value)


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
