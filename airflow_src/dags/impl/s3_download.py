"""Business logic for S3 download/restore operations."""

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
from botocore.exceptions import BotoCoreError, ClientError
from common.keys import DagContext, DagParams
from common.paths import get_internal_output_path
from dags.impl.s3_utils import (
    _FILE_NOT_FOUND,
    download_file_from_s3,
    get_etag,
    get_s3_client,
    parse_etag_from_file_info,
    reconstruct_s3_paths,
)
from plugins.file_handling import get_file_hash_with_etag

from shared.db.interface import get_raw_file_by_id
from shared.yamlsettings import get_s3_upload_config


class S3DownloadFailedException(AirflowFailException):
    """Exception raised when S3 download fails.

    Enables on_failure_callback to take special action.
    """


def download_raw_files_from_s3(ti: TaskInstance, **kwargs) -> None:
    """Download multiple raw files from S3 to output locations with self-healing (BATCH MODE).

    Destination: output_location/project_id/ (per raw_file)

    Features:
    - Batch processing: Handles comma-separated list of raw_file_ids
    - Self-healing: Renames corrupted local files to .corrupted
    - Idempotent: Skips files with correct hashes (S3 etag)
    - Best-effort: Attempts all files even if some fail
    - Status reporting: Writes batch .txt file per destination

    Args:
        ti: TaskInstance (not used, Airflow signature requirement)
        kwargs: Must contain params with raw_file_ids (comma-separated string)

    Raises:
        S3DownloadFailedException: If any files failed

    """
    del ti  # Unused
    # Get S3 config
    s3_config = get_s3_upload_config()  # TODO: HERE: "upload" <-> "access"
    region = s3_config.get("region")
    if not region:
        raise S3DownloadFailedException(
            "S3 download requires region configured in yaml under backup.s3.region"
        )

    # Extract and parse raw_file_ids
    raw_file_ids_str = kwargs[DagContext.PARAMS].get(DagParams.RAW_FILE_IDS, "")
    raw_file_ids = [rid.strip() for rid in raw_file_ids_str.split(",") if rid.strip()]
    if not raw_file_ids:
        raise S3DownloadFailedException(
            f"No valid raw_file_ids provided: '{raw_file_ids_str}'"
        )

    logging.info(
        f"Creating download plan for {len(raw_file_ids)} raw file(s): {raw_file_ids}"
    )

    output_path = get_internal_output_path()
    download_plan = _build_download_plan(raw_file_ids, output_path)

    logging.info(f"Starting S3 download:\n{download_plan}")

    results_by_destination = _execute_download_plan(download_plan, region)

    total_failures = _write_results(results_by_destination)

    # Raise exception if any failures
    if total_failures > 0:
        raise S3DownloadFailedException(
            f"S3 download completed with {total_failures} failure(s).\n{results_by_destination}"
        )

    logging.info(
        f"S3 download completed successfully for all {len(raw_file_ids)} raw file(s)"
    )


def _write_results(results_by_destination: dict[str, list]) -> int:
    """Write batch status reports and count total failures."""
    # Write batch status reports per destination
    timestamp = datetime.now(timezone.utc)
    total_failures = 0

    for destination, raw_file_results in results_by_destination.items():
        status_file = _write_batch_status_report(
            Path(destination), raw_file_results, timestamp
        )
        logging.info(f"Wrote status report: {status_file}")

        # Count failures in this destination
        for raw_file_result in raw_file_results:
            if raw_file_result["error"]:
                total_failures += 1
            for file_result in raw_file_result.get("file_results", []):
                if file_result["status"].startswith("FAILURE"):
                    total_failures += 1
    return total_failures


def _execute_download_plan(download_plan: dict, region: str) -> dict[str, list]:
    """Execute the download plan."""
    results_by_destination = defaultdict(list)
    s3_client = get_s3_client(region)

    for raw_file_id, plan in download_plan.items():
        if plan["skipped"]:
            # Add error to results
            destination = plan.get("destination_base", "UNKNOWN")
            results_by_destination[destination].append(
                {
                    "raw_file_id": raw_file_id,
                    "project_id": plan.get("project_id", "UNKNOWN"),
                    "bucket_name": plan.get("bucket_name", "UNKNOWN"),
                    "error": plan["error"],
                    "file_results": [],
                }
            )
            continue

        # Process this raw_file
        file_results = []
        bucket_name = plan["bucket_name"]
        for file_info in plan["files"]:
            result = _download_and_verify_file(
                file_info, s3_client, region, bucket_name
            )
            file_results.append(result)

        # Group by destination
        destination = plan["destination_base"]
        results_by_destination[destination].append(
            {
                "raw_file_id": raw_file_id,
                "project_id": plan["project_id"],
                "bucket_name": plan["bucket_name"],
                "error": None,
                "file_results": file_results,
            }
        )
    return dict(results_by_destination)


def _build_download_plan(raw_file_ids: list[str], output_path: Path) -> dict:
    """Construct the complete download plan dictionary (PHASE 1).

    Args:
        raw_file_ids: List of raw_file_id strings
        output_path: Output location from YAML config

    Returns:
        Dict[raw_file_id, download_plan_entry] where each entry contains:
        - project_id, destination_base, bucket_name, files list, error, skipped

    """
    download_plan = {}

    for raw_file_id in raw_file_ids:
        try:
            # Fetch raw file from DB
            raw_file = get_raw_file_by_id(raw_file_id)

            # Validate raw_file
            if not raw_file.s3_upload_path:
                download_plan[raw_file_id] = {
                    "skipped": True,
                    "error": "No S3 backup path (s3_upload_path is null)",
                }
                logging.warning(f"Skipping {raw_file_id}: No S3 backup path")
                continue

            if not raw_file.project_id:  # TODO: HERE: fallback?
                download_plan[raw_file_id] = {
                    "skipped": True,
                    "error": "No project_id",
                }
                logging.warning(f"Skipping {raw_file_id}: No project_id")
                continue

            if not raw_file.file_info:
                download_plan[raw_file_id] = {
                    "skipped": True,
                    "error": "Empty file_info",
                }
                logging.warning(f"Skipping {raw_file_id}: Empty file_info")
                continue

            # Calculate destination
            destination_base = output_path / raw_file.project_id

            # Reconstruct S3 paths
            s3_paths = reconstruct_s3_paths(raw_file.s3_upload_path, raw_file.file_info)

            # Extract bucket name (all files use same bucket)
            bucket_name = next(iter(s3_paths.values()))[0]

            # Build file list with all needed info (this is single files!)
            files = []
            for relative_path, (_bucket, s3_key) in s3_paths.items():
                file_info_tuple = raw_file.file_info[relative_path]

                # Parse etag and chunk size
                expected_etag, chunk_size_mb = parse_etag_from_file_info(
                    file_info_tuple
                )
                expected_size = file_info_tuple[0]

                # Calculate absolute destination path
                absolute_dest_path = destination_base / relative_path

                files.append(
                    {
                        "relative_path": relative_path,
                        "s3_key": s3_key,
                        "expected_etag": expected_etag,
                        "chunk_size_mb": chunk_size_mb,
                        "expected_size": expected_size,
                        "absolute_dest_path": absolute_dest_path,
                    }
                )

            download_plan[raw_file_id] = {
                "project_id": raw_file.project_id,
                "destination_base": str(destination_base),
                "bucket_name": bucket_name,
                "files": files,
                "error": None,
                "skipped": False,
            }

            logging.info(
                f"Planned download for {raw_file_id}: {len(files)} file(s) to {destination_base}"
            )

        except Exception as e:
            download_plan[raw_file_id] = {
                "skipped": True,
                "error": f"Failed to build plan: {type(e).__name__} - {e}",
            }
            logging.exception(f"Error building plan for {raw_file_id}")

    return download_plan


def _download_and_verify_file(
    file_info: dict, s3_client: BaseAwsConnection, region: str, bucket_name: str
) -> dict:
    """Download and verify a single file.

    Args:
        file_info: Dict with file details from download plan
        s3_client: Boto3 S3 client
        region: AWS region
        bucket_name: S3 bucket name

    Returns:
        Dict with status and details

    """
    relative_path = file_info["relative_path"]
    s3_key = file_info["s3_key"]
    expected_etag = file_info[
        "expected_etag"
    ]  # TODO: HERE: here we need both, never recalculate the etag here? (memory, have md5sum as ground truth)
    chunk_size_mb = file_info["chunk_size_mb"]
    absolute_dest_path = Path(file_info["absolute_dest_path"])

    try:
        # S3 pre-check: verify S3 etag matches DB
        logging.info(f"Verifying S3 etag for {s3_key}")
        s3_etag = get_etag(bucket_name, s3_key, s3_client)

        if s3_etag is _FILE_NOT_FOUND:
            return {
                "relative_path": relative_path,
                "status": "FAILURE - S3 file not found",
                "details": None,
            }

        if s3_etag != expected_etag:
            return {
                "relative_path": relative_path,
                "status": f"FAILURE - S3 corruption detected (etag mismatch: S3={s3_etag}, DB={expected_etag})",
                "details": None,
            }

        # Local file check
        if absolute_dest_path.exists():  # TODO: HERE: should this be part of the plan?
            logging.info(f"Local file exists, calculating hash: {absolute_dest_path}")
            local_hash, local_etag = get_file_hash_with_etag(
                absolute_dest_path, chunk_size_mb, calculate_etag=True
            )

            if local_etag == expected_etag:
                return {
                    "relative_path": relative_path,
                    "status": "OK - Skipped (already exists)",
                    "details": None,
                }

            # Corrupted file - rename to .corrupted
            corrupted_path = Path(str(absolute_dest_path) + ".corrupted")
            logging.warning(
                f"Local file corrupted (etag mismatch), renaming: {absolute_dest_path} -> {corrupted_path}"
            )
            absolute_dest_path.rename(corrupted_path)

        # Download from S3
        start_time = time.time()
        file_size_mb = file_info["expected_size"] / (1024 * 1024)

        logging.info(f"Downloading {relative_path} ({file_size_mb:.1f} MB) from S3")
        download_file_from_s3(
            bucket_name, s3_key, absolute_dest_path, region, chunk_size_mb
        )

        duration = time.time() - start_time
        rate_mbps = (file_size_mb / duration) if duration > 0 else 0

        # Post-download verification
        logging.info(f"Verifying downloaded file: {absolute_dest_path}")
        local_hash, local_etag = get_file_hash_with_etag(
            absolute_dest_path, chunk_size_mb, calculate_etag=True
        )

        local_etag = local_etag.split("__")[0]  # Remove multipart suffix if present

        if local_etag != expected_etag:
            # Download verification failed - rename to .corrupted
            # TODO: HERE what if ".corrupted" file exists already? (same when other files are renamed)
            corrupted_path = Path(str(absolute_dest_path) + ".corrupted")
            logging.error(
                f"Download verification failed, renaming: {absolute_dest_path} -> {corrupted_path}"
            )
            absolute_dest_path.rename(corrupted_path)

            return {
                "relative_path": relative_path,
                "status": f"FAILURE - Download verification failed (etag mismatch: local={local_etag}, expected={expected_etag})",
                "details": f"{file_size_mb:.1f} MB in {duration:.1f}s",
            }

        # Success
        was_healed = absolute_dest_path.with_suffix(
            absolute_dest_path.suffix + ".corrupted"
        ).exists()  #  TODO: HERE ???
        status = "OK - Self-healed" if was_healed else "OK - Downloaded and verified"

        return {  # noqa: TRY300
            "relative_path": relative_path,
            "status": status,
            "details": f"{file_size_mb:.1f} MB in {duration:.1f}s ({rate_mbps:.1f} MB/s)",
        }

    except (BotoCoreError, ClientError, Exception) as e:
        logging.exception(f"Error downloading {relative_path}")
        return {
            "relative_path": relative_path,
            "status": f"FAILURE - {type(e).__name__}: {e}",
            "details": None,
        }


def _write_batch_status_report(
    destination_path: Path, raw_file_results: list[dict], timestamp: datetime
) -> Path:
    """Write batch status report for a destination.

    Args:
        destination_path: Path to output directory
        raw_file_results: List of raw_file result dicts
        timestamp: Report timestamp

    Returns:
        Path to written status file

    """
    # Create status report filename
    status_filename = (
        f"_download_status_batch_{timestamp.strftime('%Y%m%d_%H%M%S')}.txt"
    )
    status_file = destination_path / status_filename

    # Ensure destination exists
    destination_path.mkdir(parents=True, exist_ok=True)

    # Calculate summary statistics
    total_raw_files = len(raw_file_results)
    total_files = sum(len(r.get("file_results", [])) for r in raw_file_results)
    successful_files = sum(
        1
        for r in raw_file_results
        for f in r.get("file_results", [])
        if f["status"].startswith("OK")
    )
    failed_files = total_files - successful_files
    failed_raw_files = sum(1 for r in raw_file_results if r.get("error"))

    # Determine overall status
    if failed_files > 0 or failed_raw_files > 0:
        overall_status = "PARTIAL FAILURE"
    else:
        overall_status = "SUCCESS"

    # Write report
    with status_file.open("w") as f:
        f.write("S3 Download Batch Status Report\n")
        f.write("=" * 80 + "\n")
        f.write(f"Destination: {destination_path}\n")
        f.write(f"Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
        f.write(f"Raw Files Processed: {total_raw_files}\n")
        f.write("\n")
        f.write("Overall Summary:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total raw files: {total_raw_files}\n")
        f.write(f"Total files: {total_files}\n")
        f.write(f"Successful files: {successful_files}\n")
        f.write(f"Failed files: {failed_files}\n")
        f.write(f"Status: {overall_status}\n")
        f.write("\n")

        # Per-RawFile sections
        for result in raw_file_results:
            f.write("=" * 80 + "\n")
            f.write(f"Raw File: {result['raw_file_id']}\n")
            f.write(f"Project ID: {result['project_id']}\n")
            f.write(f"S3 Bucket: {result['bucket_name']}\n")

            if result.get("error"):
                f.write(f"ERROR: {result['error']}\n")
                f.write("\n")
                continue

            file_results = result.get("file_results", [])
            f.write(f"Files: {len(file_results)}\n")
            f.write("\n")
            f.write("Files:\n")
            f.write("-" * 80 + "\n")

            for file_result in file_results:
                status_line = f"{file_result['relative_path']}: {file_result['status']}"
                if file_result["details"]:
                    status_line += f" ({file_result['details']})"
                f.write(status_line + "\n")

            f.write("\n")

    return status_file
