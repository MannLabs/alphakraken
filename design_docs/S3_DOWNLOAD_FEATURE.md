# S3 Download Feature - Implementation Plan

## 1. Overview

Add S3 download capability to restore raw files from S3 to local filesystem. Downloads to `{locations.output}/{project_id}/` with self-healing and txt-based status reporting.

### Key Requirements
- **Trigger**: Manual operation (CLI/UI trigger)
- **Destination**: Auto-calculated from YAML `locations.output` + RawFile `project_id`
- **Self-healing**: Automatically overwrite corrupted local files (mismatched hashes)
- **Status tracking**: Write `.txt` file with OK/FAILURE for each file
- **Database access**: Read-only (NO writes to backup_status or any field)
- **Integration**: New standalone DAG

## 2. Architecture Overview

### 2.1 High-Level Flow
```
Manual Trigger (raw_file_id only)
    ↓
Validate (RawFile exists, has S3 backup, has project_id, output location writable)
    ↓
Calculate Destination (get_path(YamlKeys.Locations.OUTPUT) / project_id)
    ↓
Download Each File:
    - If exists with correct hash → Skip (record OK)
    - If exists with wrong hash → Delete & re-download (self-healing)
    - If doesn't exist → Download
    ↓
Write Status Report (.txt file)
    ↓
Raise exception if any failures
```

### 2.2 Destination Path Construction

**Calculation**:
```python
destination_base = get_path(YamlKeys.Locations.OUTPUT) / raw_file.project_id
# Example: /fs/pool/pool-0/alphakraken_sandbox/output/PRID001
```

**File paths**:
```python
for relative_path in file_info.keys():
    local_path = destination_base / relative_path
    # Example: /fs/pool/.../output/PRID001/file.raw
```

### 2.3 S3 Key Reconstruction

```python
bucket, prefix = parse_s3_upload_path(raw_file.s3_upload_path)
# s3_upload_path examples: "bucket", "bucket/", "bucket/prefix/"

for relative_path in file_info.keys():
    s3_key = f"{prefix}/{relative_path}" if prefix else relative_path
    # Download from: s3://bucket/s3_key
    # To: {destination_base}/{relative_path}
```

### 2.4 Self-Healing Logic

```python
if local_file.exists():
    local_hash = get_file_hash(local_file)
    expected_hash = file_info[relative_path][1]  # etag from file_info

    if local_hash == expected_hash:
        → Skip download
        → Record: "OK - Skipped (already exists)"
    else:
        → Delete corrupted file
        → Download from S3
        → Verify hash
        → Record: "OK - Self-healed" or "FAILURE - {error}"
else:
    → Download from S3
    → Verify hash
    → Record: "OK - Downloaded" or "FAILURE - {error}"
```

**Key principle**: Never leave corrupted files. Always overwrite.

### 2.5 Status Report File

**Location**: `{destination_base}/_download_status_{raw_file_id}_{timestamp}.txt`

**Example path**: `/fs/pool/output/PRID001/_download_status_20250115_sample_20250204_143022.txt`

**Format**:
```
S3 Download Status Report
=========================
Raw File ID: 20250115_sample
Project ID: PRID001
S3 Bucket: alphakraken-PRID001
Timestamp: 2025-02-04 14:30:22 UTC
Destination: /fs/pool/pool-0/alphakraken_sandbox/output/PRID001

Files:
------
sample.raw: OK - Downloaded and verified (1234.5 MB in 45.2s)
sample.raw/subfile1.bin: OK - Skipped (already exists)
sample.raw/subfile2.bin: OK - Self-healed (hash mismatch detected, re-downloaded)
sample.raw/subfile3.bin: FAILURE - Network error: Connection timeout

Summary:
--------
Total files: 4
Successful: 3
Failed: 1
Status: PARTIAL FAILURE
```

**This is the ONLY status tracking** - no database writes.

## 3. Implementation Stages

### Stage 1: Core S3 Download Utilities (~3 hours)

**File**: `airflow_src/dags/impl/s3_utils.py`

#### Function 1: `download_file_from_s3()`

```python
def download_file_from_s3(
    bucket_name: str,
    s3_key: str,
    local_path: Path,
    region: str,
    chunk_size_mb: int = 500,
    aws_conn_id: str = "aws_default",
) -> None:
    """Download a single file from S3 using multipart download.

    Args:
        bucket_name: S3 bucket name
        s3_key: S3 object key
        local_path: Local filesystem destination path
        region: AWS region
        chunk_size_mb: Chunk size for multipart download
        aws_conn_id: Airflow connection ID for AWS credentials

    Raises:
        ClientError: If S3 operation fails
        IOError: If local filesystem operation fails
    """
```

**Implementation**:
- Get S3 client: `get_s3_client(region, aws_conn_id)`
- Get transfer config: `get_transfer_config(chunk_size_mb)`
- Create parent dirs: `local_path.parent.mkdir(parents=True, exist_ok=True)`
- Download: `s3_client.download_file(bucket, s3_key, str(local_path), Config=config)`
- Log: file size, duration, transfer rate

**Pattern**: Mirror existing `upload_file_to_s3()` function

#### Function 2: `reconstruct_s3_paths()`

```python
def reconstruct_s3_paths(
    s3_upload_path: str,
    file_info: dict[str, list]
) -> dict[str, tuple[str, str]]:
    """Reconstruct S3 bucket and key for each file in file_info.

    Args:
        s3_upload_path: From RawFile.s3_upload_path
                       (e.g., "bucket", "bucket/", "bucket/prefix/")
        file_info: RawFile.file_info dict with relative paths as keys

    Returns:
        Dict mapping relative_path -> (bucket_name, s3_key)
        Example: {"file.raw": ("alphakraken-PRID001", "file.raw")}
    """
```

**Implementation**:
- Parse s3_upload_path to extract bucket and optional prefix
- Handle formats: "bucket", "bucket/", "bucket/prefix/"
- For each key in file_info: construct full S3 key

**Tests**:
- `test_download_file_from_s3()` - mock boto3
- `test_reconstruct_s3_paths_bucket_only()` - "bucket"
- `test_reconstruct_s3_paths_with_prefix()` - "bucket/prefix/"
- `test_reconstruct_s3_paths_trailing_slash()` - edge cases

---

### Stage 2: Constants Updates (~30 min)

**File**: `airflow_src/plugins/common/constants.py`

**Add to `class Dags`**:
```python
S3_DOWNLOADER = "s3_downloader"
```

**Add to `class Pools`**:
```python
S3_DOWNLOAD_POOL = "s3_download_pool"  # Default: 2 slots
```

**Tests**:
- Import test to verify constants accessible

---

### Stage 3: Download Implementation Logic (~6 hours)

**File**: `airflow_src/dags/impl/handler_impl.py`

#### Function 1: `validate_s3_download_parameters()`

```python
def validate_s3_download_parameters(ti: TaskInstance, **kwargs) -> None:
    """Validate prerequisites for S3 download operation.

    Checks:
    - S3 upload feature enabled in config
    - RawFile exists in database
    - RawFile has s3_upload_path (not None)
    - RawFile has project_id (not None/empty)
    - RawFile has non-empty file_info
    - Output location exists and is writable

    Raises:
        AirflowFailException: If any check fails
    """
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    # Check S3 feature enabled
    if not is_s3_upload_enabled():
        raise AirflowFailException("S3 not enabled in config")

    # Check RawFile exists
    raw_file = get_raw_file(raw_file_id)
    if not raw_file:
        raise AirflowFailException(f"RawFile not found: {raw_file_id}")

    # Check has S3 backup
    if not raw_file.s3_upload_path:
        raise AirflowFailException(f"No S3 backup for {raw_file_id}")

    # Check has project_id
    if not raw_file.project_id:
        raise AirflowFailException(f"No project_id for {raw_file_id}")

    # Check has file_info
    if not raw_file.file_info:
        raise AirflowFailException(f"Empty file_info for {raw_file_id}")

    # Check output location exists and writable
    output_path = get_path(YamlKeys.Locations.OUTPUT)
    if not output_path.exists():
        raise AirflowFailException(f"Output location missing: {output_path}")
    if not os.access(output_path, os.W_OK):
        raise AirflowFailException(f"Output location not writable: {output_path}")

    logging.info(f"Validation passed: {raw_file_id} -> {output_path}/{raw_file.project_id}")
```

#### Function 2: `download_raw_file_from_s3()`

```python
def download_raw_file_from_s3(ti: TaskInstance, **kwargs) -> None:
    """Download raw file from S3 to output location with self-healing.

    Destination: {output_location}/{project_id}/

    Features:
    - Self-healing: Overwrites corrupted local files
    - Idempotent: Skips files with correct hashes
    - Best-effort: Attempts all files even if some fail
    - Status reporting: Writes .txt file with results

    Args:
        ti: TaskInstance (not used, Airflow signature requirement)
        **kwargs: Must contain params with raw_file_id

    Raises:
        AirflowFailException: If any files failed

    Side effects:
        - Creates/overwrites files in {output}/{project_id}/
        - Creates status report .txt file
        - NO database writes
    """
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    # Fetch RawFile from DB (READ ONLY)
    raw_file = get_raw_file(raw_file_id)

    # Calculate destination from YAML + project_id
    destination_base = get_path(YamlKeys.Locations.OUTPUT) / raw_file.project_id
    destination_base.mkdir(parents=True, exist_ok=True)

    logging.info(f"Download destination: {destination_base}")

    # Get S3 config
    s3_config = get_s3_upload_config()
    region = s3_config.get("region", "eu-central-1")

    # Reconstruct S3 paths
    s3_paths = reconstruct_s3_paths(raw_file.s3_upload_path, raw_file.file_info)

    logging.info(f"Starting download: {len(s3_paths)} files from S3")

    # Track status for each file
    file_statuses = {}  # {relative_path: (status, message)}

    # Download each file with self-healing
    for relative_path, (bucket, s3_key) in s3_paths.items():
        local_path = destination_base / relative_path
        expected_size, expected_etag = parse_file_info_item(raw_file.file_info[relative_path])

        try:
            # Self-healing check
            if local_path.exists():
                logging.info(f"File exists: {relative_path}, checking hash...")
                local_hash = get_file_hash(local_path)

                if local_hash == expected_etag:
                    logging.info(f"Hash correct - skipping {relative_path}")
                    file_statuses[relative_path] = ("OK", "Skipped (already exists)")
                    continue
                else:
                    logging.warning(
                        f"Hash mismatch for {relative_path}: "
                        f"expected={expected_etag}, got={local_hash}. "
                        f"Deleting and re-downloading (self-healing)"
                    )
                    local_path.unlink()  # Delete corrupted file
                    status_suffix = "Self-healed (hash mismatch fixed)"
            else:
                status_suffix = "Downloaded"

            # Download file
            logging.info(f"Downloading {s3_key} from {bucket}...")
            start_time = datetime.now(tz=pytz.utc)

            download_file_from_s3(
                bucket_name=bucket,
                s3_key=s3_key,
                local_path=local_path,
                region=region,
                chunk_size_mb=500,
            )

            duration = (datetime.now(tz=pytz.utc) - start_time).total_seconds()

            # Verify hash after download
            actual_hash = get_file_hash(local_path)
            if actual_hash != expected_etag:
                local_path.unlink()  # Delete bad download
                raise ValueError(
                    f"Hash verification failed: expected={expected_etag}, got={actual_hash}"
                )

            # Optionally verify size
            actual_size = local_path.stat().st_size
            size_mb = actual_size / 1024**2

            file_statuses[relative_path] = (
                "OK",
                f"{status_suffix} ({size_mb:.1f} MB in {duration:.1f}s)"
            )
            logging.info(f"Success: {relative_path}")

        except Exception as e:
            logging.error(f"Failed: {relative_path}: {e}")
            file_statuses[relative_path] = ("FAILURE", str(e))
            # Continue with other files (best-effort)

    # Write status report
    timestamp = datetime.now(tz=pytz.utc).strftime("%Y%m%d_%H%M%S")
    status_file = destination_base / f"_download_status_{raw_file_id}_{timestamp}.txt"

    success_count = sum(1 for s, _ in file_statuses.values() if s == "OK")
    failure_count = sum(1 for s, _ in file_statuses.values() if s == "FAILURE")

    with open(status_file, 'w') as f:
        f.write("S3 Download Status Report\n")
        f.write("=========================\n")
        f.write(f"Raw File ID: {raw_file_id}\n")
        f.write(f"Project ID: {raw_file.project_id}\n")
        f.write(f"S3 Bucket: {raw_file.s3_upload_path}\n")
        f.write(f"Timestamp: {datetime.now(tz=pytz.utc).isoformat()}\n")
        f.write(f"Destination: {destination_base}\n\n")

        f.write("Files:\n")
        f.write("------\n")
        for rel_path in sorted(file_statuses.keys()):
            status, msg = file_statuses[rel_path]
            f.write(f"{rel_path}: {status}")
            if msg:
                f.write(f" - {msg}")
            f.write("\n")

        f.write("\nSummary:\n")
        f.write("--------\n")
        f.write(f"Total files: {len(file_statuses)}\n")
        f.write(f"Successful: {success_count}\n")
        f.write(f"Failed: {failure_count}\n")

        if failure_count == 0:
            f.write("Status: SUCCESS\n")
        elif success_count == 0:
            f.write("Status: COMPLETE FAILURE\n")
        else:
            f.write("Status: PARTIAL FAILURE\n")

    logging.info(f"Status report: {status_file}")

    # Fail task if any failures (operator must review txt)
    if failure_count > 0:
        raise AirflowFailException(
            f"{failure_count}/{len(file_statuses)} files failed. "
            f"See: {status_file}"
        )

    logging.info(f"All {success_count} files downloaded successfully")
```

**Tests**:
- `test_validate_s3_download_parameters_success()`
- `test_validate_s3_download_parameters_no_project_id()`
- `test_validate_s3_download_parameters_no_s3_backup()`
- `test_download_raw_file_from_s3_new_file()` - fresh download
- `test_download_raw_file_from_s3_skip_existing()` - correct hash
- `test_download_raw_file_from_s3_self_healing()` - wrong hash, overwrite
- `test_download_raw_file_from_s3_multi_file()` - .d folder
- `test_download_raw_file_from_s3_partial_failure()` - some fail
- `test_status_report_format()` - verify txt content

---

### Stage 4: S3 Downloader DAG (~3 hours)

**File**: `airflow_src/dags/s3_downloader.py` (new file)

```python
"""S3 Downloader DAG - Restore raw files from S3 to output location."""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.dates import days_ago

from common.constants import Dags, DagParams, Pools, AIRFLOW_QUEUE_PREFIX
from common.callbacks import on_failure_callback
from impl.handler_impl import (
    download_raw_file_from_s3,
    validate_s3_download_parameters
)

S3_DOWNLOAD_QUEUE = f"{AIRFLOW_QUEUE_PREFIX}s3_download"


def create_s3_downloader_dag() -> DAG:
    """Create S3 downloader DAG for manual file restoration.

    Downloads to: {output_location}/{project_id}/
    - Self-healing: Overwrites corrupted files
    - Idempotent: Safe to re-run
    - Status reporting: Creates .txt file
    """
    with DAG(
        Dags.S3_DOWNLOADER,
        schedule=None,  # Manual trigger only
        start_date=days_ago(1),
        default_args={
            "depends_on_past": False,
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "queue": S3_DOWNLOAD_QUEUE,
            "on_failure_callback": on_failure_callback,
        },
        description="Download raw files from S3 to output location (self-healing)",
        catchup=False,
        tags=["s3", "download", "manual", "restoration"],
        params={
            DagParams.RAW_FILE_ID: Param(
                type="string",
                minLength=3,
                description=(
                    "ID of the RawFile to download from S3. "
                    "Files downloaded to: {output_location}/{project_id}/"
                )
            ),
        },
    ) as dag:

        validate_parameters_ = PythonOperator(
            task_id="validate_parameters",
            python_callable=validate_s3_download_parameters,
            provide_context=True,
        )

        download_files_ = PythonOperator(
            task_id="download_files",
            python_callable=download_raw_file_from_s3,
            provide_context=True,
            execution_timeout=timedelta(hours=8),
            pool=Pools.S3_DOWNLOAD_POOL,
        )

        validate_parameters_ >> download_files_

    return dag


dag = create_s3_downloader_dag()
```

**Tests**:
- `test_s3_downloader_dag_exists()`
- `test_s3_downloader_dag_structure()`
- `test_s3_downloader_dag_params()` - only raw_file_id

---

### Stage 5: Documentation & Integration (~2 hours)

#### 5.1 Create Airflow Pool

In Airflow UI (Admin → Pools):
- **Name**: `s3_download_pool`
- **Slots**: `2`
- **Description**: "Limit concurrent S3 downloads"

#### 5.2 Update CLAUDE.md

Add after S3 backup section:

```markdown
### S3 Download Operations

**Trigger Download**:
```bash
# Download raw file from S3 to {output_location}/{project_id}/
airflow dags trigger s3_downloader --conf '{"raw_file_id": "20250115_sample"}'
```

**Behavior**:
- **Destination**: Automatically calculated as `{locations.output}/{project_id}/`
- **Self-healing**: Corrupted local files automatically overwritten
- **Idempotent**: Safe to re-run (skips correct files)
- **Status**: Creates `_download_status_{raw_file_id}_{timestamp}.txt`
- **Database**: Read-only (no backup_status updates)

**Check Results**:
```bash
# View status report
cat /fs/pool/output/{PROJECT_ID}/_download_status_*.txt
```

**Status Report Format**:
```
S3 Download Status Report
=========================
Raw File ID: {id}
Project ID: {project_id}
...


Summary:
--------
Total files: 3
Successful: 2
Failed: 1
Status: PARTIAL FAILURE


Files:
------
file.raw: OK - Downloaded
subfolder/data.bin: OK - Skipped (already exists)
subfolder/index.dat: FAILURE - Network timeout

```
```

#### 5.3 Update S3_BACKUP_DESIGN.md

Add before "Future Enhancements":

```markdown
## 12. S3 Download Feature

### Overview
Manual restoration of raw files from S3 to output location.

See `design_docs/S3_DOWNLOAD_FEATURE.md` for full details.

### Key Features
- **Destination**: `{output_location}/{project_id}/` (auto-calculated)
- **Self-healing**: Overwrites corrupted files automatically
- **Idempotent**: Safe to re-run
- **Status**: `.txt` report file (no DB updates)
- **Best-effort**: Tries all files even if some fail

### Usage
```bash
airflow dags trigger s3_downloader --conf '{"raw_file_id": "FILE_ID"}'
```

Check results: `/fs/pool/output/{PROJECT_ID}/_download_status_*.txt`
```

#### 5.4 Testing Checklist

**Unit Tests** (~3 hours):
- [ ] s3_utils functions (mock boto3)
- [ ] validate_s3_download_parameters()
- [ ] download_raw_file_from_s3() (mock S3, filesystem)
- [ ] Status txt writing
- [ ] Self-healing logic
- [ ] DAG structure

**Integration Tests** (manual):
- [ ] Fresh download from S3
- [ ] Idempotent: re-run, files skipped
- [ ] Self-healing: corrupt file, gets overwritten
- [ ] Multi-file: .d folder (10+ files)
- [ ] Partial failure: mix of success/failure
- [ ] Large file: >10 GB
- [ ] Status txt format correct
- [ ] **Verify NO DB writes** (check backup_status unchanged)

---

## 4. Error Handling

### 4.1 Error Scenarios

| Condition | Behavior |
|-----------|----------|
| RawFile not found | Validation fails |
| Missing project_id | Validation fails (can't determine destination) |
| No S3 backup | Validation fails |
| Output location not writable | Validation fails |
| S3 bucket doesn't exist | Record FAILURE for all files |
| Network error during download | Record FAILURE for that file, continue |
| Hash mismatch after download | Delete file, record FAILURE |
| Local file corrupted | Delete & re-download (self-healing) |
| Some files fail | Write status txt, task fails |
| All succeed | Write status txt, task succeeds |

### 4.2 Edge Cases

#### 4.2.1 Multi-File Downloads (.d folders)
- Iterate all files in file_info
- Best-effort: continue even if some fail
- Record all outcomes in status txt
- Task fails if any failures (operator reviews txt)

#### 4.2.2 Idempotency
- Safe to re-run multiple times
- Skips files with correct hashes
- Re-downloads files with wrong hashes

#### 4.2.3 S3 Path Parsing
- Formats: "bucket", "bucket/", "bucket/prefix/"
- Handle trailing slashes, multiple slashes, empty prefixes

#### 4.2.4 Missing Project ID
- Validation fails immediately
- Cannot calculate destination without project_id

### 4.3 Monitoring

#### Logging
- Download start/end for each file
- File sizes, durations, transfer rates
- Skipped files (already correct)
- Self-healing actions (deleted corrupted files)
- Hash verification results

#### Status Tracking
- Status txt file is the ONLY source of truth
- Each file: OK or FAILURE with reason
- Summary: success/failure counts
- NO database updates

---

## 5. Security

### 5.1 Access Control
- Reuse existing AWS credentials
- IAM permissions: `s3:GetObject`, `s3:ListBucket`

### 5.2 Path Safety
- Destination always calculated from trusted sources
- No user-provided paths
- All file_info paths are relative (no `..`)

### 5.3 Data Integrity
- Verify etag after download
- Delete files that fail verification
- Never leave corrupted files

---

## 6. Performance

### 6.1 Download Speed
- 500 MB chunks
- 50 GB file ≈ 3.3 hours (100 Mbps)
- Bottleneck: Network bandwidth

### 6.2 Concurrency
- Pool limit: 2 slots (prevents network saturation)
- Sequential downloads within DAG
- Multiple DAG runs can run concurrently

---

## 7. Rollout

### 7.1 Phased Deployment
1. Deploy to sandbox
2. Test small files (<1 GB)
3. Test large files (>10 GB)
4. Test .d folders
5. Test self-healing
6. Monitor 1 week
7. Production rollout

### 7.2 Rollback
- No config changes needed
- To disable: don't trigger DAG
- No DB changes (read-only)
- No risk to existing workflows

---

## 8. Known Limitations (v1)

- Manual operation only
- No progress reporting in UI
- Sequential downloads (one file at a time)
- No resume capability
- No automatic bucket validation

---

## 9. Future Enhancements

- Automatic fallback during processing
- Parallel file downloads
- Resume capability
- Progress reporting
- Batch operations
- Selective downloads
- Compression support
- Notifications

---

## 10. Estimated Effort

| Stage | Time |
|-------|------|
| S3 Utilities | 3 hours |
| Constants | 30 min |
| Download Logic | 6 hours |
| DAG Creation | 3 hours |
| Documentation | 2 hours |
| **Total** | **~14.5 hours (~2 days)** |

With contingency: **~18 hours (~2-3 days)**

---

## 11. Summary

Clean, reliable S3 download implementation:
- ✅ Single parameter (raw_file_id)
- ✅ Auto-destination (YAML + project_id)
- ✅ Self-healing (overwrites corrupted files)
- ✅ Idempotent (safe to re-run)
- ✅ Status tracking via txt (NO DB writes)
- ✅ Best-effort (tries all files)
- ✅ Clear operator feedback
- ✅ Multi-file support
- ✅ Hash verification

**Files to create/modify**:
1. `dags/impl/s3_utils.py` - 2 functions
2. `dags/impl/handler_impl.py` - 2 functions
3. `dags/s3_downloader.py` - New DAG
4. `plugins/common/constants.py` - 2 constants
5. `CLAUDE.md` - Usage docs
6. Tests

**Critical principles**:
- Read-only DB access (NO writes to backup_status)
- Self-healing over error handling
- Status txt is single source of truth
- Destination always derived from config
