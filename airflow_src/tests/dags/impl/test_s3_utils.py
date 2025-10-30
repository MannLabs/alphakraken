"""Tests for the s3_utils module."""

from unittest.mock import MagicMock, patch

from dags.impl.s3_utils import (
    get_s3_client,
    get_transfer_config,
    normalize_bucket_name,
)


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


class TestGetTransferConfig:
    """Tests for _get_transfer_config function."""

    def test_returns_valid_transfer_config(self) -> None:
        """Test that transfer config is properly configured."""
        config = get_transfer_config()

        # Check multipart settings (500MB)
        assert config.multipart_threshold == 500 * 1024 * 1024
        assert config.multipart_chunksize == 500 * 1024 * 1024
        assert config.use_threads is True
        assert config.max_concurrency == 10
