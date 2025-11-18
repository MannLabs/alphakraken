"""Unit tests for s3_upload.py."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from botocore.exceptions import BotoCoreError, ClientError
from common.keys import DagContext, DagParams
from dags.impl.s3_upload import (
    S3UploadFailedException,
    _extract_etag_from_file_info,
    _get_key_prefix,
    _prepare_upload,
    _upload_all_files,
    upload_raw_file_to_s3,
)

from shared.db.models import BackupStatus

# TODO: fix
# ruff: noqa


def test_s3_upload_failed_exception_is_airflow_fail_exception() -> None:
    """Test S3UploadFailedException inherits from AirflowFailException."""
    exception = S3UploadFailedException("test error")

    assert isinstance(exception, AirflowFailException)


@patch("dags.impl.s3_upload.get_s3_upload_config")
def test_upload_raw_file_to_s3_should_raise_when_region_not_configured(
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 raises when region is not configured."""
    mock_get_s3_config.return_value = {"region": None, "bucket_prefix": "test-prefix"}
    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    with pytest.raises(
        S3UploadFailedException,
        match="S3 backup enabled but region or bucket_prefix not configured",
    ):
        upload_raw_file_to_s3(ti, **kwargs)


@patch("dags.impl.s3_upload.get_s3_upload_config")
def test_upload_raw_file_to_s3_should_raise_when_bucket_prefix_not_configured(
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 raises when bucket_prefix is not configured."""
    mock_get_s3_config.return_value = {"region": "us-west-2", "bucket_prefix": None}
    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    with pytest.raises(
        S3UploadFailedException,
        match="S3 backup enabled but region or bucket_prefix not configured",
    ):
        upload_raw_file_to_s3(ti, **kwargs)


@patch("dags.impl.s3_upload.get_s3_upload_config")
@patch("dags.impl.s3_upload.get_raw_file_by_id")
@patch("dags.impl.s3_upload.get_xcom")
@patch("dags.impl.s3_upload._get_project_id_or_fallback")
@patch("dags.impl.s3_upload.normalize_bucket_name")
@patch("dags.impl.s3_upload.get_transfer_config")
@patch("dags.impl.s3_upload.update_raw_file")
@patch("dags.impl.s3_upload.get_s3_client")
@patch("dags.impl.s3_upload.bucket_exists")
@patch("dags.impl.s3_upload._prepare_upload")
@patch("dags.impl.s3_upload._upload_all_files")
def test_upload_raw_file_to_s3_should_complete_successfully(  # noqa: PLR0913
    mock_upload_all: MagicMock,
    mock_prepare: MagicMock,
    mock_bucket_exists: MagicMock,
    mock_get_s3_client: MagicMock,
    mock_update: MagicMock,
    mock_get_transfer_config: MagicMock,
    mock_normalize_bucket: MagicMock,
    mock_get_project_id: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_raw_file: MagicMock,
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 completes successfully with valid configuration."""
    mock_get_s3_config.return_value = {
        "region": "us-west-2",
        "bucket_prefix": "test-prefix",
    }
    mock_raw_file = MagicMock(project_id="PID1", instrument_id="instrument1")
    mock_get_raw_file.return_value = mock_raw_file
    mock_get_xcom.side_effect = [
        {"/src/file.raw": "/dst/file.raw"},
        "/dst",
    ]
    mock_get_project_id.return_value = "PID1"
    mock_normalize_bucket.return_value = "test-prefix-pid1"
    mock_bucket_exists.return_value = (True, "")
    mock_prepare.return_value = (
        {Path("/dst/file.raw"): ("file.raw", "etag123")},
        "",
    )

    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    upload_raw_file_to_s3(ti, **kwargs)

    mock_update.assert_any_call(
        "test.raw", backup_status=BackupStatus.UPLOAD_IN_PROGRESS
    )
    mock_update.assert_any_call(
        "test.raw",
        backup_status=BackupStatus.UPLOAD_DONE,
        s3_upload_path="s3://test-prefix-pid1",
    )
    mock_upload_all.assert_called_once()


@patch("dags.impl.s3_upload.get_s3_upload_config")
@patch("dags.impl.s3_upload.get_raw_file_by_id")
@patch("dags.impl.s3_upload.get_xcom")
@patch("dags.impl.s3_upload._get_project_id_or_fallback")
@patch("dags.impl.s3_upload.normalize_bucket_name")
@patch("dags.impl.s3_upload.get_transfer_config")
@patch("dags.impl.s3_upload.update_raw_file")
@patch("dags.impl.s3_upload.get_s3_client")
@patch("dags.impl.s3_upload.bucket_exists")
@patch("dags.impl.s3_upload._prepare_upload")
@patch("dags.impl.s3_upload._upload_all_files")
def test_upload_raw_file_to_s3_should_include_key_prefix_in_s3_path(  # noqa: PLR0913
    mock_upload_all: MagicMock,
    mock_prepare: MagicMock,
    mock_bucket_exists: MagicMock,
    mock_get_s3_client: MagicMock,
    mock_update: MagicMock,
    mock_get_transfer_config: MagicMock,
    mock_normalize_bucket: MagicMock,
    mock_get_project_id: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_raw_file: MagicMock,
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 includes key prefix in s3_upload_path."""
    mock_get_s3_config.return_value = {
        "region": "us-west-2",
        "bucket_prefix": "test-prefix",
    }
    mock_raw_file = MagicMock(project_id=None, instrument_id="instrument1")
    mock_get_raw_file.return_value = mock_raw_file
    mock_get_xcom.side_effect = [
        {"/src/file.raw": "/dst/file.raw"},
        "/dst",
    ]
    mock_get_project_id.return_value = "_FALLBACK"
    mock_normalize_bucket.return_value = "test-prefix-fallback"
    mock_bucket_exists.return_value = (True, "")
    mock_prepare.return_value = (
        {Path("/dst/file.raw"): ("instrument1/2025_01/file.raw", "etag123")},
        "instrument1/2025_01/",
    )

    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    upload_raw_file_to_s3(ti, **kwargs)

    mock_update.assert_any_call(
        "test.raw",
        backup_status=BackupStatus.UPLOAD_DONE,
        s3_upload_path="s3://test-prefix-fallback/instrument1/2025_01/",
    )


@patch("dags.impl.s3_upload.get_s3_upload_config")
@patch("dags.impl.s3_upload.get_raw_file_by_id")
@patch("dags.impl.s3_upload.get_xcom")
@patch("dags.impl.s3_upload._get_project_id_or_fallback")
@patch("dags.impl.s3_upload.normalize_bucket_name")
@patch("dags.impl.s3_upload.get_transfer_config")
@patch("dags.impl.s3_upload.update_raw_file")
@patch("dags.impl.s3_upload.get_s3_client")
@patch("dags.impl.s3_upload.bucket_exists")
def test_upload_raw_file_to_s3_should_raise_when_bucket_does_not_exist(  # noqa: PLR0913
    mock_bucket_exists: MagicMock,
    mock_get_s3_client: MagicMock,
    mock_update: MagicMock,
    mock_get_transfer_config: MagicMock,
    mock_normalize_bucket: MagicMock,
    mock_get_project_id: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_raw_file: MagicMock,
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 raises when bucket does not exist."""
    mock_get_s3_config.return_value = {
        "region": "us-west-2",
        "bucket_prefix": "test-prefix",
    }
    mock_raw_file = MagicMock(project_id="PID1", instrument_id="instrument1")
    mock_get_raw_file.return_value = mock_raw_file
    mock_get_xcom.side_effect = [
        {"/src/file.raw": "/dst/file.raw"},
        "/dst",
    ]
    mock_get_project_id.return_value = "PID1"
    mock_normalize_bucket.return_value = "test-prefix-pid1"
    mock_bucket_exists.return_value = (False, "Bucket does not exist")

    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    with pytest.raises(S3UploadFailedException, match="Bucket does not exist"):
        upload_raw_file_to_s3(ti, **kwargs)

    mock_update.assert_called_once_with(
        "test.raw", backup_status=BackupStatus.UPLOAD_IN_PROGRESS
    )


@patch("dags.impl.s3_upload.get_s3_upload_config")
@patch("dags.impl.s3_upload.get_raw_file_by_id")
@patch("dags.impl.s3_upload.get_xcom")
@patch("dags.impl.s3_upload._get_project_id_or_fallback")
@patch("dags.impl.s3_upload.normalize_bucket_name")
@patch("dags.impl.s3_upload.get_transfer_config")
@patch("dags.impl.s3_upload.update_raw_file")
@patch("dags.impl.s3_upload.get_s3_client")
@patch("dags.impl.s3_upload.bucket_exists")
@patch("dags.impl.s3_upload._prepare_upload")
@patch("dags.impl.s3_upload._upload_all_files")
def test_upload_raw_file_to_s3_should_raise_on_boto_error(  # noqa: PLR0913
    mock_upload_all: MagicMock,
    mock_prepare: MagicMock,
    mock_bucket_exists: MagicMock,
    mock_get_s3_client: MagicMock,
    mock_update: MagicMock,
    mock_get_transfer_config: MagicMock,
    mock_normalize_bucket: MagicMock,
    mock_get_project_id: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_raw_file: MagicMock,
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 raises S3UploadFailedException on BotoCoreError."""
    mock_get_s3_config.return_value = {
        "region": "us-west-2",
        "bucket_prefix": "test-prefix",
    }
    mock_raw_file = MagicMock(project_id="PID1", instrument_id="instrument1")
    mock_get_raw_file.return_value = mock_raw_file
    mock_get_xcom.side_effect = [
        {"/src/file.raw": "/dst/file.raw"},
        "/dst",
    ]
    mock_get_project_id.return_value = "PID1"
    mock_normalize_bucket.return_value = "test-prefix-pid1"
    mock_bucket_exists.return_value = (True, "")
    mock_prepare.return_value = (
        {Path("/dst/file.raw"): ("file.raw", "etag123")},
        "",
    )
    mock_upload_all.side_effect = BotoCoreError()

    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    with pytest.raises(
        S3UploadFailedException, match="S3 upload failed for test.raw: BotoCoreError"
    ):
        upload_raw_file_to_s3(ti, **kwargs)


@patch("dags.impl.s3_upload.get_s3_upload_config")
@patch("dags.impl.s3_upload.get_raw_file_by_id")
@patch("dags.impl.s3_upload.get_xcom")
@patch("dags.impl.s3_upload._get_project_id_or_fallback")
@patch("dags.impl.s3_upload.normalize_bucket_name")
@patch("dags.impl.s3_upload.get_transfer_config")
@patch("dags.impl.s3_upload.update_raw_file")
@patch("dags.impl.s3_upload.get_s3_client")
@patch("dags.impl.s3_upload.bucket_exists")
@patch("dags.impl.s3_upload._prepare_upload")
@patch("dags.impl.s3_upload._upload_all_files")
def test_upload_raw_file_to_s3_should_raise_on_client_error(  # noqa: PLR0913
    mock_upload_all: MagicMock,
    mock_prepare: MagicMock,
    mock_bucket_exists: MagicMock,
    mock_get_s3_client: MagicMock,
    mock_update: MagicMock,
    mock_get_transfer_config: MagicMock,
    mock_normalize_bucket: MagicMock,
    mock_get_project_id: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_raw_file: MagicMock,
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 raises S3UploadFailedException on ClientError."""
    mock_get_s3_config.return_value = {
        "region": "us-west-2",
        "bucket_prefix": "test-prefix",
    }
    mock_raw_file = MagicMock(project_id="PID1", instrument_id="instrument1")
    mock_get_raw_file.return_value = mock_raw_file
    mock_get_xcom.side_effect = [
        {"/src/file.raw": "/dst/file.raw"},
        "/dst",
    ]
    mock_get_project_id.return_value = "PID1"
    mock_normalize_bucket.return_value = "test-prefix-pid1"
    mock_bucket_exists.return_value = (True, "")
    mock_prepare.return_value = (
        {Path("/dst/file.raw"): ("file.raw", "etag123")},
        "",
    )
    mock_upload_all.side_effect = ClientError(
        {"Error": {"Code": "500"}}, "upload_fileobj"
    )

    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    with pytest.raises(
        S3UploadFailedException, match="S3 upload failed for test.raw: ClientError"
    ):
        upload_raw_file_to_s3(ti, **kwargs)


@patch("dags.impl.s3_upload._get_key_prefix")
@patch("dags.impl.s3_upload._extract_etag_from_file_info")
def test_prepare_upload_should_create_correct_mapping(
    mock_extract_etag: MagicMock,
    mock_get_key_prefix: MagicMock,
) -> None:
    """Test _prepare_upload creates correct file path to S3 key mapping."""
    mock_raw_file = MagicMock()
    mock_get_key_prefix.return_value = "prefix/"
    mock_extract_etag.return_value = "etag123"

    files_dst_paths = {
        Path("/src/file1.raw"): Path("/dst/backup/file1.raw"),
        Path("/src/file2.raw"): Path("/dst/backup/file2.raw"),
    }
    target_folder_path = "/dst/backup"

    result, key_prefix = _prepare_upload(
        files_dst_paths, mock_raw_file, target_folder_path
    )

    assert key_prefix == "prefix/"
    assert result == {
        Path("/dst/backup/file1.raw"): ("prefix/file1.raw", "etag123"),
        Path("/dst/backup/file2.raw"): ("prefix/file2.raw", "etag123"),
    }


def test_get_key_prefix_should_return_empty_for_project_file() -> None:
    """Test _get_key_prefix returns empty string for file with project."""
    mock_raw_file = MagicMock()
    mock_raw_file.project_id = "PID1"
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.id = "test.raw"

    result = _get_key_prefix(mock_raw_file)

    assert result == ""


@patch("dags.impl.s3_upload.get_created_at_year_month")
def test_get_key_prefix_should_include_instrument_and_date_for_fallback(
    mock_get_year_month: MagicMock,
) -> None:
    """Test _get_key_prefix includes instrument and date for files without project."""
    mock_get_year_month.return_value = "2025_01"
    mock_raw_file = MagicMock()
    mock_raw_file.project_id = None
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.id = "test.raw"

    result = _get_key_prefix(mock_raw_file)

    assert result == "instrument1/2025_01/"


@patch("dags.impl.s3_upload.get_created_at_year_month")
def test_get_key_prefix_should_include_file_id_for_wiff_file(
    mock_get_year_month: MagicMock,
) -> None:
    """Test _get_key_prefix includes file ID for .wiff files."""
    mock_get_year_month.return_value = "2025_01"
    mock_raw_file = MagicMock()
    mock_raw_file.project_id = None
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.id = "test.wiff"

    result = _get_key_prefix(mock_raw_file)

    assert result == "instrument1/2025_01/test.wiff/"


def test_get_key_prefix_should_include_file_id_for_wiff_with_project() -> None:
    """Test _get_key_prefix includes file ID for .wiff files even with project."""
    mock_raw_file = MagicMock()
    mock_raw_file.project_id = "PID1"
    mock_raw_file.instrument_id = "instrument1"
    mock_raw_file.id = "test.wiff"

    result = _get_key_prefix(mock_raw_file)

    assert result == "test.wiff/"


@patch("dags.impl.s3_upload.is_upload_needed")
@patch("dags.impl.s3_upload.upload_file_to_s3")
@patch("dags.impl.s3_upload.get_etag")
def test_upload_all_files_should_upload_all_files(
    mock_get_etag: MagicMock,
    mock_upload_file: MagicMock,
    mock_is_upload_needed: MagicMock,
) -> None:
    """Test _upload_all_files uploads all files with ETag verification."""
    mock_is_upload_needed.return_value = True
    mock_get_etag.return_value = "etag123"
    mock_s3_client = MagicMock()
    mock_transfer_config = MagicMock()

    file_mapping = {
        Path("/dst/file1.raw"): ("key1.raw", "etag123"),
        Path("/dst/file2.raw"): ("key2.raw", "etag456"),
    }

    mock_get_etag.side_effect = ["etag123", "etag456"]

    _upload_all_files(file_mapping, "test-bucket", mock_transfer_config, mock_s3_client)

    assert mock_upload_file.call_count == 2


@patch("dags.impl.s3_upload.is_upload_needed")
@patch("dags.impl.s3_upload.upload_file_to_s3")
@patch("dags.impl.s3_upload.get_etag")
def test_upload_all_files_should_skip_when_upload_not_needed(
    mock_get_etag: MagicMock,
    mock_upload_file: MagicMock,
    mock_is_upload_needed: MagicMock,
) -> None:
    """Test _upload_all_files skips upload when file already exists with matching ETag."""
    mock_is_upload_needed.return_value = False
    mock_s3_client = MagicMock()
    mock_transfer_config = MagicMock()

    file_mapping = {Path("/dst/file1.raw"): ("key1.raw", "etag123")}

    _upload_all_files(file_mapping, "test-bucket", mock_transfer_config, mock_s3_client)

    mock_upload_file.assert_not_called()
    mock_get_etag.assert_not_called()


@patch("dags.impl.s3_upload.is_upload_needed")
def test_upload_all_files_should_raise_on_is_upload_needed_error(
    mock_is_upload_needed: MagicMock,
) -> None:
    """Test _upload_all_files raises S3UploadFailedException when is_upload_needed raises ValueError."""
    mock_is_upload_needed.side_effect = ValueError("ETag mismatch")
    mock_s3_client = MagicMock()
    mock_transfer_config = MagicMock()

    file_mapping = {Path("/dst/file1.raw"): ("key1.raw", "etag123")}

    with pytest.raises(S3UploadFailedException, match="ETag mismatch"):
        _upload_all_files(
            file_mapping, "test-bucket", mock_transfer_config, mock_s3_client
        )


@patch("dags.impl.s3_upload.is_upload_needed")
@patch("dags.impl.s3_upload.upload_file_to_s3")
@patch("dags.impl.s3_upload.get_etag")
def test_upload_all_files_should_raise_on_etag_mismatch(
    mock_get_etag: MagicMock,
    mock_upload_file: MagicMock,
    mock_is_upload_needed: MagicMock,
) -> None:
    """Test _upload_all_files raises S3UploadFailedException on ETag mismatch after upload."""
    mock_is_upload_needed.return_value = True
    mock_get_etag.return_value = "different_etag"
    mock_s3_client = MagicMock()
    mock_transfer_config = MagicMock()

    file_mapping = {Path("/dst/file1.raw"): ("key1.raw", "etag123")}

    with pytest.raises(
        S3UploadFailedException,
        match="ETag mismatch for key1.raw: local etag123 != remote different_etag",
    ):
        _upload_all_files(
            file_mapping, "test-bucket", mock_transfer_config, mock_s3_client
        )


@patch("dags.impl.s3_upload.is_upload_needed")
@patch("dags.impl.s3_upload.upload_file_to_s3")
@patch("dags.impl.s3_upload.get_etag")
@patch("dags.impl.s3_upload.S3_FILE_NOT_FOUND_ETAG")
def test_upload_all_files_should_raise_when_file_not_found_after_upload(
    mock_file_not_found: MagicMock,
    mock_get_etag: MagicMock,
    mock_upload_file: MagicMock,
    mock_is_upload_needed: MagicMock,
) -> None:
    """Test _upload_all_files raises when file not found on S3 after upload."""
    mock_is_upload_needed.return_value = True
    mock_get_etag.return_value = mock_file_not_found
    mock_s3_client = MagicMock()
    mock_transfer_config = MagicMock()

    file_mapping = {Path("/dst/file1.raw"): ("key1.raw", "etag123")}

    with pytest.raises(S3UploadFailedException, match="ETag mismatch for key1.raw"):
        _upload_all_files(
            file_mapping, "test-bucket", mock_transfer_config, mock_s3_client
        )


def test_extract_etag_from_file_info_should_return_etag() -> None:
    """Test _extract_etag_from_file_info extracts ETag from file_info."""
    mock_raw_file = MagicMock()
    mock_raw_file.file_info = {"file.raw": (1000, "hash123", "etag123__500")}

    result = _extract_etag_from_file_info("file.raw", mock_raw_file)

    assert result == "etag123"


def test_extract_etag_from_file_info_should_handle_etag_without_separator() -> None:
    """Test _extract_etag_from_file_info handles ETag without separator."""
    mock_raw_file = MagicMock()
    mock_raw_file.file_info = {"file.raw": (1000, "hash123", "etag123")}

    result = _extract_etag_from_file_info("file.raw", mock_raw_file)

    assert result == "etag123"


@patch("dags.impl.s3_upload.get_s3_upload_config")
@patch("dags.impl.s3_upload.get_raw_file_by_id")
@patch("dags.impl.s3_upload.get_xcom")
@patch("dags.impl.s3_upload._get_project_id_or_fallback")
@patch("dags.impl.s3_upload.normalize_bucket_name")
@patch("dags.impl.s3_upload.get_transfer_config")
@patch("dags.impl.s3_upload.update_raw_file")
@patch("dags.impl.s3_upload.get_s3_client")
@patch("dags.impl.s3_upload.bucket_exists")
@patch("dags.impl.s3_upload._prepare_upload")
@patch("dags.impl.s3_upload._upload_all_files")
def test_upload_raw_file_to_s3_should_handle_multiple_files(  # noqa: PLR0913
    mock_upload_all: MagicMock,
    mock_prepare: MagicMock,
    mock_bucket_exists: MagicMock,
    mock_get_s3_client: MagicMock,
    mock_update: MagicMock,
    mock_get_transfer_config: MagicMock,
    mock_normalize_bucket: MagicMock,
    mock_get_project_id: MagicMock,
    mock_get_xcom: MagicMock,
    mock_get_raw_file: MagicMock,
    mock_get_s3_config: MagicMock,
) -> None:
    """Test upload_raw_file_to_s3 handles multiple files correctly."""
    mock_get_s3_config.return_value = {
        "region": "us-west-2",
        "bucket_prefix": "test-prefix",
    }
    mock_raw_file = MagicMock(project_id="PID1", instrument_id="instrument1")
    mock_get_raw_file.return_value = mock_raw_file
    mock_get_xcom.side_effect = [
        {
            "/src/file1.raw": "/dst/file1.raw",
            "/src/file2.wiff": "/dst/file2.wiff",
            "/src/file3.wiff.scan": "/dst/file3.wiff.scan",
        },
        "/dst",
    ]
    mock_get_project_id.return_value = "PID1"
    mock_normalize_bucket.return_value = "test-prefix-pid1"
    mock_bucket_exists.return_value = (True, "")
    mock_prepare.return_value = (
        {
            Path("/dst/file1.raw"): ("file1.raw", "etag1"),
            Path("/dst/file2.wiff"): ("file2.wiff", "etag2"),
            Path("/dst/file3.wiff.scan"): ("file3.wiff.scan", "etag3"),
        },
        "",
    )

    ti = MagicMock()
    kwargs = {DagContext.PARAMS: {DagParams.RAW_FILE_ID: "test.raw"}}

    upload_raw_file_to_s3(ti, **kwargs)

    mock_upload_all.assert_called_once()
    call_args = mock_upload_all.call_args[0]
    assert len(call_args[0]) == 3


@patch("dags.impl.s3_upload._get_key_prefix")
@patch("dags.impl.s3_upload._extract_etag_from_file_info")
def test_prepare_upload_should_handle_nested_directory_structure(
    mock_extract_etag: MagicMock,
    mock_get_key_prefix: MagicMock,
) -> None:
    """Test _prepare_upload handles nested directory structures correctly."""
    mock_raw_file = MagicMock()
    mock_get_key_prefix.return_value = "prefix/"
    mock_extract_etag.return_value = "etag123"

    files_dst_paths = {
        Path("/src/file.raw"): Path("/dst/backup/subdir/nested/file.raw"),
    }
    target_folder_path = "/dst/backup"

    result, key_prefix = _prepare_upload(
        files_dst_paths, mock_raw_file, target_folder_path
    )

    assert key_prefix == "prefix/"
    assert result == {
        Path("/dst/backup/subdir/nested/file.raw"): (
            "prefix/subdir/nested/file.raw",
            "etag123",
        ),
    }


@patch("dags.impl.s3_upload.is_upload_needed")
@patch("dags.impl.s3_upload.upload_file_to_s3")
@patch("dags.impl.s3_upload.get_etag")
def test_upload_all_files_should_handle_mixed_upload_needs(
    mock_get_etag: MagicMock,
    mock_upload_file: MagicMock,
    mock_is_upload_needed: MagicMock,
) -> None:
    """Test _upload_all_files handles mix of files that need/don't need upload."""
    mock_is_upload_needed.side_effect = [True, False, True]
    mock_get_etag.side_effect = ["etag123", "etag789"]
    mock_s3_client = MagicMock()
    mock_transfer_config = MagicMock()

    file_mapping = {
        Path("/dst/file1.raw"): ("key1.raw", "etag123"),
        Path("/dst/file2.raw"): ("key2.raw", "etag456"),
        Path("/dst/file3.raw"): ("key3.raw", "etag789"),
    }

    _upload_all_files(file_mapping, "test-bucket", mock_transfer_config, mock_s3_client)

    assert mock_upload_file.call_count == 2
    assert mock_get_etag.call_count == 2


def test_extract_etag_from_file_info_should_raise_on_missing_file() -> None:
    """Test _extract_etag_from_file_info raises KeyError when file not in file_info."""
    mock_raw_file = MagicMock()
    mock_raw_file.file_info = {"other_file.raw": (1000, "hash123", "etag123__500")}

    with pytest.raises(KeyError):
        _extract_etag_from_file_info("missing_file.raw", mock_raw_file)


def test_extract_etag_from_file_info_should_raise_on_invalid_format() -> None:
    """Test _extract_etag_from_file_info raises IndexError when file_info has wrong format."""
    mock_raw_file = MagicMock()
    mock_raw_file.file_info = {"file.raw": (1000, "hash123")}

    with pytest.raises(IndexError):
        _extract_etag_from_file_info("file.raw", mock_raw_file)
