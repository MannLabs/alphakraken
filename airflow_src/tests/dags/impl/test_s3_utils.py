"""Tests for the s3_utils module."""

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from dags.impl.s3_utils import (
    calculate_s3_etag,
    get_s3_client,
    normalize_bucket_name,
)


class TestCalculateS3Etag:
    """Tests for calculate_s3_etag function."""

    def test_calculate_etag_empty_file(self) -> None:
        """Test ETag calculation for empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            file_path = Path(f.name)

        try:
            etag = calculate_s3_etag(file_path, chunk_size_mb=1)
            # Empty file MD5
            assert etag == hashlib.md5(b"").hexdigest()  # noqa: S324
        finally:
            file_path.unlink()

    def test_calculate_etag_small_file(self) -> None:
        """Test ETag calculation for file smaller than chunk size."""
        content = b"test content for small file"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            file_path = Path(f.name)

        try:
            etag = calculate_s3_etag(file_path, chunk_size_mb=1)
            # Single part - just MD5
            expected = hashlib.md5(content).hexdigest()  # noqa: S324
            assert etag == expected
        finally:
            file_path.unlink()

    def test_calculate_etag_multipart_file(self) -> None:
        """Test ETag calculation for file requiring multipart upload."""
        # Create file larger than 1MB chunk size (use 2MB)
        chunk_size_mb = 1
        chunk_size_bytes = chunk_size_mb * 1024 * 1024

        # Create content slightly larger than 2 chunks
        part1 = b"a" * chunk_size_bytes
        part2 = b"b" * chunk_size_bytes
        part3 = b"c" * 100  # Small third part

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(part1 + part2 + part3)
            file_path = Path(f.name)

        try:
            etag = calculate_s3_etag(file_path, chunk_size_mb=chunk_size_mb)

            # Calculate expected multipart ETag
            md5_1 = hashlib.md5(part1).digest()  # noqa: S324
            md5_2 = hashlib.md5(part2).digest()  # noqa: S324
            md5_3 = hashlib.md5(part3).digest()  # noqa: S324
            combined_hash = hashlib.md5(md5_1 + md5_2 + md5_3).hexdigest()  # noqa: S324
            expected = f"{combined_hash}-3"

            assert etag == expected
            assert "-" in etag  # Verify multipart format
            assert etag.endswith("-3")  # Verify 3 parts
        finally:
            file_path.unlink()

    def test_calculate_etag_exactly_one_chunk(self) -> None:
        """Test ETag calculation for file exactly one chunk size."""
        chunk_size_mb = 1
        chunk_size_bytes = chunk_size_mb * 1024 * 1024
        content = b"x" * chunk_size_bytes

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            file_path = Path(f.name)

        try:
            etag = calculate_s3_etag(file_path, chunk_size_mb=chunk_size_mb)
            # Exactly one chunk - just MD5 without part count
            expected = hashlib.md5(content).hexdigest()  # noqa: S324
            assert etag == expected
            assert "-" not in etag  # Single part format
        finally:
            file_path.unlink()


class TestGetS3Client:
    """Tests for get_s3_client function."""

    @patch("dags.impl.s3_utils.AwsBaseHook")
    def test_get_s3_client_with_default_connection(
        self, mock_hook_class: MagicMock
    ) -> None:
        """Test getting S3 client with default connection ID."""
        mock_hook = MagicMock()
        mock_client = MagicMock()
        mock_hook.get_conn.return_value = mock_client
        mock_hook_class.return_value = mock_hook

        client = get_s3_client("eu-central-1")

        mock_hook_class.assert_called_once_with(
            aws_conn_id="aws_default", client_type="s3", region_name="eu-central-1"
        )
        mock_hook.get_conn.assert_called_once()
        assert client == mock_client

    @patch("dags.impl.s3_utils.AwsBaseHook")
    def test_get_s3_client_with_custom_connection(
        self, mock_hook_class: MagicMock
    ) -> None:
        """Test getting S3 client with custom connection ID."""
        mock_hook = MagicMock()
        mock_client = MagicMock()
        mock_hook.get_conn.return_value = mock_client
        mock_hook_class.return_value = mock_hook

        client = get_s3_client("us-west-2", aws_conn_id="my_aws_conn")

        mock_hook_class.assert_called_once_with(
            aws_conn_id="my_aws_conn", client_type="s3", region_name="us-west-2"
        )
        assert client == mock_client


class TestNormalizeBucketName:
    """Tests for normalize_bucket_name function."""

    def test_normalize_bucket_name_simple(self) -> None:
        """Test bucket name normalization with simple project ID."""
        bucket = normalize_bucket_name("PRJ001", "alphakraken")
        assert bucket == "alphakraken-prj001"

    def test_normalize_bucket_name_with_special_chars(self) -> None:
        """Test bucket name normalization with special characters."""
        bucket = normalize_bucket_name("PRJ_001", "alphakraken")
        assert bucket == "alphakraken-prj-001"

        bucket = normalize_bucket_name("PRJ.001", "alphakraken")
        assert bucket == "alphakraken-prj-001"

    def test_normalize_bucket_name_with_spaces(self) -> None:
        """Test bucket name normalization with spaces."""
        bucket = normalize_bucket_name("PRJ 001", "alphakraken")
        assert bucket == "alphakraken-prj-001"

    def test_normalize_bucket_name_all_lowercase(self) -> None:
        """Test bucket name is converted to lowercase."""
        bucket = normalize_bucket_name("PROJECT001", "AlphaKraken")
        assert bucket == "alphakraken-project001"
        assert bucket == bucket.lower()

    def test_normalize_bucket_name_truncates_long_names(self) -> None:
        """Test bucket name truncation when exceeding 63 characters."""
        long_project_id = "a" * 100
        bucket = normalize_bucket_name(long_project_id, "alphakraken")

        assert len(bucket) <= 63
        assert bucket.startswith("alphakraken-")

    def test_normalize_bucket_name_within_length_limit(self) -> None:
        """Test bucket name within S3 length limits."""
        bucket = normalize_bucket_name("PRJ001", "alphakraken")
        assert len(bucket) >= 3  # Minimum S3 bucket name length
        assert len(bucket) <= 63  # Maximum S3 bucket name length
