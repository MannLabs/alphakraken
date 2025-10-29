"""Tests for the s3_backup module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from common.keys import DagContext, DagParams, OpArgs
from dags.impl.s3_backup import _get_transfer_config, upload_raw_file_to_s3

from shared.db.models import BackupStatus


@pytest.fixture
def mock_raw_file() -> MagicMock:
    """Fixture for a raw file."""
    mock = MagicMock()
    mock.id = "test_file_123"
    mock.project_id = "PRJ001"
    mock.original_name = "test_file.raw"
    mock.backup_base_path = "/backup/base/path"
    return mock


@pytest.fixture
def mock_task_instance() -> MagicMock:
    """Fixture for Airflow TaskInstance."""
    return MagicMock()


@pytest.fixture
def mock_kwargs() -> dict:
    """Fixture for task kwargs."""
    return {
        DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test_file_123"},
        OpArgs.INSTRUMENT_ID: "instr1",
    }


class TestUploadRawFileToS3:
    """Tests for upload_raw_file_to_s3 function."""

    @patch("shared.db.interface.connect_db")
    @patch("dags.impl.s3_backup.YAMLSETTINGS")
    def test_skips_upload_when_backup_type_is_local(
        self,
        mock_settings: MagicMock,
        mock_connect_db: MagicMock,  # noqa: ARG002
        mock_task_instance: MagicMock,
        mock_kwargs: dict,
    ) -> None:
        """Test that upload is skipped when backup_type is 'local'."""
        mock_settings.get.return_value = {"backup_type": "local"}

        upload_raw_file_to_s3(mock_task_instance, **mock_kwargs)

        # Should return early without attempting any uploads
        mock_task_instance.xcom_pull.assert_not_called()

    @patch("shared.db.interface.connect_db")
    @patch("dags.impl.s3_backup.get_raw_file_by_id")
    @patch("dags.impl.s3_backup.update_raw_file")
    @patch("dags.impl.s3_backup.get_xcom")
    @patch("dags.impl.s3_backup.YAMLSETTINGS")
    def test_returns_early_when_s3_config_missing(  # noqa: PLR0913
        self,
        mock_settings: MagicMock,
        mock_get_xcom: MagicMock,  # noqa: ARG002
        mock_update_raw_file: MagicMock,  # noqa: ARG002
        mock_get_raw_file_by_id: MagicMock,
        mock_connect_db: MagicMock,  # noqa: ARG002
        mock_task_instance: MagicMock,
        mock_kwargs: dict,
    ) -> None:
        """Test that function returns early when S3 config is incomplete."""
        mock_settings.get.side_effect = [
            {"backup_type": "s3"},  # First call for backup_type
            {"s3": {}},  # Second call for s3 config (missing region/bucket_prefix)
        ]

        upload_raw_file_to_s3(mock_task_instance, **mock_kwargs)

        # Should return early without attempting uploads
        mock_get_raw_file_by_id.assert_not_called()

    @patch("shared.db.interface.connect_db")
    @patch("dags.impl.s3_backup.get_raw_file_by_id")
    @patch("dags.impl.s3_backup.update_raw_file")
    @patch("dags.impl.s3_backup.get_xcom")
    @patch("dags.impl.s3_backup.get_s3_client")
    @patch("dags.impl.s3_backup.YAMLSETTINGS")
    def test_fails_gracefully_when_bucket_does_not_exist(  # noqa: PLR0913
        self,
        mock_settings: MagicMock,
        mock_get_s3_client: MagicMock,
        mock_get_xcom: MagicMock,
        mock_update_raw_file: MagicMock,
        mock_get_raw_file_by_id: MagicMock,
        mock_connect_db: MagicMock,  # noqa: ARG002
        mock_task_instance: MagicMock,
        mock_kwargs: dict,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test graceful failure when S3 bucket does not exist."""
        mock_settings.get.side_effect = [
            {"backup_type": "s3"},
            {"s3": {"region": "eu-central-1", "bucket_prefix": "alphakraken"}},
        ]
        mock_get_raw_file_by_id.return_value = mock_raw_file
        mock_get_xcom.return_value = {}

        # Mock S3 client to raise 404 on head_bucket
        mock_s3 = MagicMock()
        mock_s3.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadBucket"
        )
        mock_get_s3_client.return_value = mock_s3

        upload_raw_file_to_s3(mock_task_instance, **mock_kwargs)

        # Should set backup status to FAILED
        mock_update_raw_file.assert_called_with(
            "test_file_123", backup_status=BackupStatus.FAILED
        )

    @patch("shared.db.interface.connect_db")
    @patch("dags.impl.s3_backup.get_raw_file_by_id")
    @patch("dags.impl.s3_backup.update_raw_file")
    @patch("dags.impl.s3_backup.get_xcom")
    @patch("dags.impl.s3_backup.get_s3_client")
    @patch("dags.impl.s3_backup.calculate_s3_etag")
    @patch("dags.impl.s3_backup.YAMLSETTINGS")
    def test_skips_upload_when_file_already_exists_with_matching_etag(  # noqa: PLR0913
        self,
        mock_settings: MagicMock,
        mock_calculate_etag: MagicMock,
        mock_get_s3_client: MagicMock,
        mock_get_xcom: MagicMock,
        mock_update_raw_file: MagicMock,
        mock_get_raw_file_by_id: MagicMock,
        mock_connect_db: MagicMock,  # noqa: ARG002
        mock_task_instance: MagicMock,
        mock_kwargs: dict,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test that upload is skipped when file already exists with matching ETag."""
        mock_settings.get.side_effect = [
            {"backup_type": "s3"},
            {"s3": {"region": "eu-central-1", "bucket_prefix": "alphakraken"}},
        ]
        mock_get_raw_file_by_id.return_value = mock_raw_file

        # Mock file paths
        src_path = Path("/src/test_file.raw")
        dst_path = Path("/backup/base/path/test_file.raw")
        mock_get_xcom.return_value = {str(src_path): str(dst_path)}

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}  # Bucket exists
        mock_s3.head_object.return_value = {"ETag": '"abc123"'}  # File exists
        mock_get_s3_client.return_value = mock_s3

        # Mock ETag calculation to match existing
        mock_calculate_etag.return_value = "abc123"

        upload_raw_file_to_s3(mock_task_instance, **mock_kwargs)

        # Should NOT call upload_fileobj since file already exists
        mock_s3.upload_fileobj.assert_not_called()

        # Should set backup status to DONE
        assert any(
            call[1].get("backup_status") == BackupStatus.DONE
            for call in mock_update_raw_file.call_args_list
        )

    @patch("shared.db.interface.connect_db")
    @patch("dags.impl.s3_backup.get_raw_file_by_id")
    @patch("dags.impl.s3_backup.update_raw_file")
    @patch("dags.impl.s3_backup.get_xcom")
    @patch("dags.impl.s3_backup.get_s3_client")
    @patch("dags.impl.s3_backup.calculate_s3_etag")
    @patch("dags.impl.s3_backup.YAMLSETTINGS")
    def test_uploads_file_successfully(  # noqa: PLR0913
        self,
        mock_settings: MagicMock,
        mock_calculate_etag: MagicMock,
        mock_get_s3_client: MagicMock,
        mock_get_xcom: MagicMock,
        mock_update_raw_file: MagicMock,
        mock_get_raw_file_by_id: MagicMock,
        mock_connect_db: MagicMock,  # noqa: ARG002
        mock_task_instance: MagicMock,
        mock_kwargs: dict,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test successful file upload to S3."""
        mock_settings.get.side_effect = [
            {"backup_type": "s3"},
            {"s3": {"region": "eu-central-1", "bucket_prefix": "alphakraken"}},
        ]
        mock_get_raw_file_by_id.return_value = mock_raw_file

        # Mock file paths
        src_path = Path("/src/test_file.raw")
        dst_path = Path("/backup/base/path/test_file.raw")
        mock_get_xcom.return_value = {str(src_path): str(dst_path)}

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}  # Bucket exists
        mock_s3.upload_fileobj.return_value = None
        mock_get_s3_client.return_value = mock_s3

        # Mock ETag calculation
        mock_calculate_etag.return_value = "abc123"

        # After upload, mock head_object to return matching ETag
        def head_object_side_effect(*args, **kwargs) -> dict[str, str]:  # noqa: ARG001
            if mock_s3.upload_fileobj.called:
                return {"ETag": '"abc123"'}
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

        mock_s3.head_object.side_effect = head_object_side_effect

        # Mock Path.open for file reading
        with patch.object(Path, "open", create=True) as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            upload_raw_file_to_s3(mock_task_instance, **mock_kwargs)

        # Should call upload_fileobj
        mock_s3.upload_fileobj.assert_called_once()

        # Should set backup status to DONE
        assert any(
            call[1].get("backup_status") == BackupStatus.DONE
            for call in mock_update_raw_file.call_args_list
        )

    @patch("shared.db.interface.connect_db")
    @patch("dags.impl.s3_backup.get_raw_file_by_id")
    @patch("dags.impl.s3_backup.update_raw_file")
    @patch("dags.impl.s3_backup.get_xcom")
    @patch("dags.impl.s3_backup.get_s3_client")
    @patch("dags.impl.s3_backup.calculate_s3_etag")
    @patch("dags.impl.s3_backup.YAMLSETTINGS")
    def test_fails_gracefully_on_etag_mismatch(  # noqa: PLR0913
        self,
        mock_settings: MagicMock,
        mock_calculate_etag: MagicMock,
        mock_get_s3_client: MagicMock,
        mock_get_xcom: MagicMock,
        mock_update_raw_file: MagicMock,
        mock_get_raw_file_by_id: MagicMock,
        mock_connect_db: MagicMock,  # noqa: ARG002
        mock_task_instance: MagicMock,
        mock_kwargs: dict,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test graceful failure when ETag verification fails."""
        mock_settings.get.side_effect = [
            {"backup_type": "s3"},
            {"s3": {"region": "eu-central-1", "bucket_prefix": "alphakraken"}},
        ]
        mock_get_raw_file_by_id.return_value = mock_raw_file

        # Mock file paths
        src_path = Path("/src/test_file.raw")
        dst_path = Path("/backup/base/path/test_file.raw")
        mock_get_xcom.return_value = {str(src_path): str(dst_path)}

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        mock_s3.head_object.side_effect = [
            ClientError({"Error": {"Code": "404"}}, "HeadObject"),  # File doesn't exist
            {"ETag": '"wrong_etag"'},  # After upload, wrong ETag
        ]
        mock_get_s3_client.return_value = mock_s3

        # Mock ETag calculation
        mock_calculate_etag.return_value = "abc123"

        # Mock Path.open for file reading
        with patch.object(Path, "open", create=True) as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            upload_raw_file_to_s3(mock_task_instance, **mock_kwargs)

        # Should set backup status to FAILED
        assert any(
            call[1].get("backup_status") == BackupStatus.FAILED
            for call in mock_update_raw_file.call_args_list
        )

    @patch("shared.db.interface.connect_db")
    @patch("dags.impl.s3_backup.get_raw_file_by_id")
    @patch("dags.impl.s3_backup.update_raw_file")
    @patch("dags.impl.s3_backup.get_xcom")
    @patch("dags.impl.s3_backup.get_s3_client")
    @patch("dags.impl.s3_backup.YAMLSETTINGS")
    def test_handles_multifile_upload(  # noqa: PLR0913
        self,
        mock_settings: MagicMock,
        mock_get_s3_client: MagicMock,
        mock_get_xcom: MagicMock,
        mock_update_raw_file: MagicMock,
        mock_get_raw_file_by_id: MagicMock,
        mock_connect_db: MagicMock,  # noqa: ARG002
        mock_task_instance: MagicMock,
        mock_kwargs: dict,
        mock_raw_file: MagicMock,
    ) -> None:
        """Test upload of multiple files (e.g., .d folder)."""
        mock_settings.get.side_effect = [
            {"backup_type": "s3"},
            {"s3": {"region": "eu-central-1", "bucket_prefix": "alphakraken"}},
        ]
        mock_get_raw_file_by_id.return_value = mock_raw_file

        # Mock multiple file paths
        files = {
            "/src/test.d/Analysis.tdf": "/backup/base/path/test.d/Analysis.tdf",
            "/src/test.d/Analysis.tdf_bin": "/backup/base/path/test.d/Analysis.tdf_bin",
        }
        mock_get_xcom.return_value = files

        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.head_bucket.return_value = {}
        # All files already exist with matching ETags
        mock_s3.head_object.return_value = {"ETag": '"abc123"'}
        mock_get_s3_client.return_value = mock_s3

        # Mock calculate_s3_etag to always return matching ETag
        with patch("dags.impl.s3_backup.calculate_s3_etag", return_value="abc123"):
            upload_raw_file_to_s3(mock_task_instance, **mock_kwargs)

        # Should check both files
        assert mock_s3.head_object.call_count >= 2

        # Should set backup status to DONE
        assert any(
            call[1].get("backup_status") == BackupStatus.DONE
            for call in mock_update_raw_file.call_args_list
        )


class TestGetTransferConfig:
    """Tests for _get_transfer_config function."""

    def test_returns_valid_transfer_config(self) -> None:
        """Test that transfer config is properly configured."""
        config = _get_transfer_config()

        # Check multipart settings (500MB)
        assert config.multipart_threshold == 500 * 1024 * 1024
        assert config.multipart_chunksize == 500 * 1024 * 1024
        assert config.use_threads is True
        assert config.max_concurrency == 10
