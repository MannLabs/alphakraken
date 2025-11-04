# S3 Download Feature - Implementation Plan

## 1. Overview

Add S3 download capability to restore raw files from S3 to local filesystem. Downloads to output location based on project ID with self-healing and txt-based status reporting.

### Key Requirements
- **Trigger**: Manual operation (CLI/UI trigger)
- **Destination**: Auto-calculated from YAML locations.output + RawFile project_id
- **Self-healing**: Automatically overwrite corrupted local files (mismatched hashes)
- **Status tracking**: Write .txt file with OK/FAILURE for each file
- **Database access**: Read-only (NO writes to backup_status or any field)
- **Integration**: New standalone DAG

## 2. Architecture Overview

### 2.1 High-Level Flow

Manual Trigger (comma-separated list of raw_file_ids)
    ↓
Parse raw_file_ids (split by comma, trim whitespace)
    ↓
For each raw_file_id (sequential, best-effort):
    ↓
    Fetch RawFile from DB (if fails: record error, continue to next)
    ↓
    Validate (has S3 backup, has project_id, output location writable)
    (if fails: record error, continue to next)
    ↓
    Calculate Destination (from YAML output location / project_id)
    ↓
    Download Each File in file_info:
        - Get S3 etag → Compare to DB etag (MANDATORY pre-check)
          - If S3 mismatch → Record FAILURE, skip to next file
        - If local file exists with correct hash → Skip (record OK)
        - If local file exists with wrong hash → Delete & re-download (self-healing)
        - If doesn't exist → Download from S3
        - Calculate local etag → Compare to DB etag (verify download)
          - If mismatch → Delete file, record FAILURE
    ↓
    Record all results for this raw_file
    ↓
Group Results by Destination Directory
    ↓
Write Status Report per Destination (.txt file for each destination)
    ↓
Raise exception if ANY raw_files had failures

### 2.2 Destination Path Construction

**Calculation**:
- Use YAML locations.output path combined with RawFile project_id
- Example result: /fs/pool/pool-0/alphakraken_sandbox/output/PRID001

**File paths**:
- Each file from file_info is placed relative to destination base
- Maintains directory structure from S3

### 2.3 S3 Key Reconstruction

- Parse s3_upload_path from RawFile (formats: "bucket", "bucket/", "bucket/prefix/")
- For each relative path in file_info: construct full S3 key
- Download from S3 bucket/key to local destination/relative_path

### 2.4 Self-Healing Logic & Verification

**Per-File Processing**:

For each file in file_info:

1. **Pre-Download S3 Verification** (see Section 2.6):
   - Get S3 etag using `get_etag(bucket, key, s3_client)`
   - Parse expected etag from DB (file_info[path][2])
   - **If S3 etag ≠ DB etag**: Record FAILURE ("S3 corruption"), skip to next file
   - **Purpose**: Detect S3 corruption before downloading

2. **Local File Check**:
   - If local file exists:
     - Calculate local etag using `get_file_hash_with_etag()` with correct chunk_size
     - Extract expected etag from file_info
     - If etags match → Skip download, record "OK - Skipped (already exists)"
     - If etags mismatch → Delete corrupted file, proceed to download
   - If local file doesn't exist → Proceed to download

3. **Download**:
   - Use `download_file_from_s3()` with chunk_size from etag string
   - Log file size, duration, transfer rate

4. **Post-Download Verification** (see Section 2.6):
   - Calculate local etag immediately after download
   - Compare to expected etag from DB
   - **If mismatch**: Delete corrupted file, record FAILURE ("Download verification failed")
   - **If match**: Record "OK - Downloaded" or "OK - Self-healed"

**Key principles**:
- Never leave corrupted files. Always delete on verification failure.
- Never skip verification. Always check S3 etag before download and local etag after.
- Use exact same chunk_size_mb as was used during upload (extracted from stored etag).

### 2.5 Status Report File

**Batch Mode Behavior**:
- When processing multiple raw_file_ids, they may belong to different projects
- Each project downloads to its own destination: output/{project_id}/
- **One combined report per destination directory** covering all raw_files in that directory
- If batch includes raw_files from 3 different projects → 3 separate status reports

**Location**: `{destination_base}/_download_status_batch_{timestamp}.txt`

**Example path**: `/fs/pool/output/PRID001/_download_status_batch_20250204_143022.txt`

**Format**:
- Header with: Batch summary, Timestamp, Destination
- Per-RawFile sections with: Raw File ID, Project ID, S3 Bucket, file results
- Overall summary section with: Total raw_files, total files, success/failure counts

**Example content** (batch with 2 raw_files in same project):
```
S3 Download Batch Status Report
================================
Destination: /fs/pool/pool-0/alphakraken_sandbox/output/PRID001
Timestamp: 2025-02-04 14:30:22 UTC
Raw Files Processed: 2

Overall Summary:
----------------
Total raw files: 2
Total files: 7
Successful files: 6
Failed files: 1
Status: PARTIAL FAILURE

================================================================================
Raw File: 20250115_sample
Project ID: PRID001
S3 Bucket: alphakraken-PRID001
Files: 4

Files:
------
sample.raw: OK - Downloaded and verified (1234.5 MB in 45.2s)
sample.raw/subfile1.bin: OK - Skipped (already exists)
sample.raw/subfile2.bin: OK - Self-healed (hash mismatch detected, re-downloaded)
sample.raw/subfile3.bin: FAILURE - S3 corruption detected (etag mismatch)

================================================================================
Raw File: 20250116_sample
Project ID: PRID001
S3 Bucket: alphakraken-PRID001
Files: 3

Files:
------
sample2.raw: OK - Downloaded and verified (2048.0 MB in 67.3s)
sample2.raw/subfile1.bin: OK - Downloaded and verified (512.0 MB in 15.1s)
sample2.raw/subfile2.bin: OK - Skipped (already exists)

```

**Cleanup Policy**:
- No automatic cleanup of status files
- Operators manually clean up old reports if needed
- All reports are preserved for audit trail

**This is the ONLY status tracking** - no database writes.

### 2.6 ETag Format & Verification

**Background**: The codebase stores ETags in a specific format that includes both the hash value and the chunk size used during upload.

**Storage Format** (in RawFile.file_info):
- Each file in file_info is a tuple: `(size, md5_hash, etag_with_chunk)`
- ETag format: `{etag_value}{ETAG_SEPARATOR}{chunk_size_mb}`
- ETAG_SEPARATOR constant: `"__"` (defined in plugins/file_handling.py:151)
- Example: `"d8e8fca2dc0f896fd7cb4cb0031ba249-5__500"`
  - ETag value: `d8e8fca2dc0f896fd7cb4cb0031ba249-5`
  - Chunk size: `500` MB

**ETag Value Interpretation**:
- **Single-part file**: Simple MD5 hex digest (e.g., `"d8e8fca2dc0f896fd7cb4cb0031ba249"`)
- **Multipart file**: `{MD5(concatenated_chunk_hashes)}-{num_parts}` (e.g., `"abc123...-5"` means 5 parts)
- **Empty file**: MD5 of empty string = `"d41d8cd98f00b204e9800998ecf8427e"`

**Three-Step Verification Process**:

1. **Pre-Download S3 Verification** (MANDATORY):
   - Call `get_etag(bucket_name, s3_key, s3_client)` to get current S3 ETag
   - Parse stored etag from file_info (split on ETAG_SEPARATOR, take first part)
   - Compare S3 ETag with stored ETag value
   - **If mismatch**: Record FAILURE ("S3 corruption detected"), skip file
   - **Purpose**: Detect S3-side corruption or wrong file before wasting bandwidth

2. **Download**:
   - Use `download_file_from_s3()` with same chunk_size_mb as stored in etag

3. **Post-Download Local Verification** (MANDATORY):
   - Call `get_file_hash_with_etag(local_path, chunk_size_mb, calculate_etag=True)`
   - Parse stored etag from file_info (extract etag value and chunk_size)
   - Compare calculated local ETag with stored ETag value
   - **If mismatch**: Delete corrupted file, record FAILURE ("Download verification failed")
   - **Purpose**: Detect download corruption or transmission errors

**Existing Utilities**:
- `get_file_hash_with_etag()` - plugins/file_handling.py:103
- `_md5hashes_to_etag()` - plugins/file_handling.py:154
- `get_etag()` - airflow_src/dags/impl/s3_utils.py:149
- `parse_file_info_item()` - shared/db/models.py:138 (extracts size and hash)

**Implementation Notes**:
- Must use the SAME chunk_size_mb during verification as was used during upload
- The chunk size is stored in the etag string after the separator
- S3's native ETag format matches this for multipart uploads
- The upload code uses S3_UPLOAD_CHUNK_SIZE_MB = 500

## 3. Implementation Stages

### Stage 1: Core S3 Download Utilities (~3 hours)

**File**: airflow_src/dags/impl/s3_utils.py

**Existing Utilities to Reuse**:
- `get_s3_client(region, aws_conn_id)` - line 15
- `get_transfer_config(chunk_size_mb)` - line 30
- `get_etag(bucket_name, s3_key, s3_client)` - line 149 (for S3 pre-check)
- `normalize_bucket_name(project_id, bucket_prefix)` - line 53 (if needed)

**File**: plugins/file_handling.py

**Existing Utilities to Reuse**:
- `get_file_hash_with_etag(file_path, chunk_size_mb, calculate_etag=True)` - line 103
- `ETAG_SEPARATOR` constant - line 151

#### New Function 1: download_file_from_s3()

**Purpose**: Download a single file from S3 using multipart download

**Parameters**:
- bucket_name: S3 bucket name
- s3_key: S3 object key
- local_path: Local filesystem destination path
- region: AWS region
- chunk_size_mb: Chunk size for multipart download (default 500)
- aws_conn_id: Airflow connection ID for AWS credentials (default "aws_default")

**Returns**: None

**Raises**:
- ClientError: If S3 operation fails
- IOError: If local filesystem operation fails

**Implementation steps**:
- Get S3 client using `get_s3_client(region, aws_conn_id)`
- Get transfer config using `get_transfer_config(chunk_size_mb)`
- Create parent directories if they don't exist (pathlib.Path.mkdir(parents=True, exist_ok=True))
- Download using `s3_client.download_fileobj()` with binary write mode
- Log: file size, duration, transfer rate

**Pattern**: Mirror existing `upload_file_to_s3()` function (line 177) but for download

#### New Function 2: reconstruct_s3_paths()

**Purpose**: Reconstruct S3 bucket and key for each file in file_info

**Parameters**:
- s3_upload_path: From RawFile.s3_upload_path (e.g., "bucket", "bucket/", "bucket/prefix/")
- file_info: RawFile.file_info dict with relative paths as keys

**Returns**:
- Dict mapping relative_path -> (bucket_name, s3_key)
- Example: {"file.raw": ("alphakraken-PRID001", "file.raw")}

**Implementation steps**:
- Parse s3_upload_path to extract bucket and optional prefix
- Handle formats: "bucket", "bucket/", "bucket/prefix/"
- Strip trailing slashes from prefix
- For each key in file_info: construct full S3 key = prefix + "/" + key (if prefix exists)

#### New Function 3: parse_etag_from_file_info()

**Purpose**: Extract etag value and chunk_size from file_info tuple

**Parameters**:
- file_info_tuple: The 3-tuple from file_info dict (size, md5_hash, etag_with_chunk)

**Returns**:
- Tuple of (etag_value: str, chunk_size_mb: int)
- Example: ("d8e8fca2dc0f896fd7cb4cb0031ba249-5", 500)

**Implementation steps**:
- Access file_info_tuple[2] to get etag_with_chunk string
- Split on ETAG_SEPARATOR ("__")
- Return (parts[0], int(parts[1]))

**Tests**:
- test_download_file_from_s3() - mock boto3
- test_reconstruct_s3_paths_bucket_only() - "bucket"
- test_reconstruct_s3_paths_with_prefix() - "bucket/prefix/"
- test_reconstruct_s3_paths_trailing_slash() - edge cases
- test_parse_etag_from_file_info() - verify etag and chunk_size extraction
- test_parse_etag_from_file_info_single_part() - no dash in etag

---

### Stage 2: Constants Updates (~30 min)

**File**: airflow_src/plugins/common/constants.py

**Add to class Dags**:
- S3_DOWNLOADER = "s3_downloader"

**Add to class Pools**:
- S3_DOWNLOAD_POOL = "s3_download_pool"  (Default: 2 slots)

**Tests**:
- Import test to verify constants accessible

---

### Stage 3: Download Implementation Logic (~8 hours)

**File**: airflow_src/dags/impl/handler_impl.py

**IMPORTANT IMPLEMENTATION NOTE**:
Separate the construction of WHAT should be done from the actual DOING. The implementation should follow a two-phase approach:

1. **Planning Phase**: Construct a complete data structure describing all downloads
   - Dictionary with keys: `raw_file_id`
   - Values: dict containing all information needed for download:
     - `project_id`: str
     - `destination_base`: Path (absolute)
     - `bucket_name`: str
     - `files`: list of dicts, each containing:
       - `relative_path`: str (key from file_info)
       - `s3_key`: str (full S3 key)
       - `expected_etag`: str (parsed etag value)
       - `chunk_size_mb`: int (parsed from etag)
       - `expected_size`: int (bytes)
       - `absolute_dest_path`: Path (destination_base / relative_path)

2. **Execution Phase**: Iterate the constructed dictionary and perform downloads
   - All path calculations done
   - All etag parsing done
   - All S3 keys constructed
   - Just execute downloads and verifications

**Benefits**:
- Clear separation of concerns
- Easier to test (can test planning separately)
- Easier to debug (inspect planned structure before execution)
- Simpler execution loop (just follow the plan)
- Better error messages (can reference the plan)

#### Function 1: download_raw_files_from_s3()

**Purpose**: Download multiple raw files from S3 to output locations with self-healing (BATCH MODE)

**Destination**: output_location/project_id/ (per raw_file)

**Features**:
- Batch processing: Handles comma-separated list of raw_file_ids
- Self-healing: Overwrites corrupted local files
- Idempotent: Skips files with correct hashes
- Best-effort: Attempts all files even if some fail
- S3 verification: Checks S3 etag before downloading
- Status reporting: Writes batch .txt file per destination

**Parameters**:
- ti: TaskInstance (not used, Airflow signature requirement)
- kwargs: Must contain params with raw_file_ids (comma-separated string)

**Raises**: AirflowFailException if any files failed

**Side effects**:
- Creates/overwrites files in output/project_id/
- Creates batch status report .txt file(s) per destination
- NO database writes

**Implementation steps**:

**PHASE 1: Planning - Construct Download Plan**

1. Extract raw_file_ids from parameters (comma-separated string)
2. Parse into list (split, strip whitespace)
3. Get S3 config (region, aws_conn_id)
4. **Build download_plan dictionary** (keys: raw_file_id):
   - **For each raw_file_id**:
     - Try to fetch RawFile from DB
       - If fails: record error in plan, mark as skipped, continue
     - Validate raw_file (has s3_upload_path, project_id, file_info)
       - If fails: record error in plan, mark as skipped, continue
     - Calculate destination_base from YAML config + project_id
     - Parse bucket_name from s3_upload_path
     - **For each file in file_info**:
       - Parse relative_path (key)
       - Parse expected_size, md5_hash, etag_with_chunk from tuple
       - Split etag to get expected_etag and chunk_size_mb
       - Construct s3_key using reconstruct_s3_paths()
       - Calculate absolute_dest_path = destination_base / relative_path
       - Add to files list with all parsed data
     - Store in download_plan[raw_file_id] = {
         "project_id": ...,
         "destination_base": ...,
         "bucket_name": ...,
         "files": [...],
         "error": None or error_message,
         "skipped": True/False
       }
5. Log download plan summary (total raw_files, total files, destinations)

**PHASE 2: Execution - Execute Download Plan**

6. Initialize results tracking: dict[destination] -> list[raw_file_results]
7. Get S3 client once (reuse for all operations)
8. **For each raw_file_id in download_plan** (sequential, best-effort):
   - Get plan for this raw_file
   - If skipped: record error in results, continue to next
   - Initialize file_results for this raw_file
   - **For each file in plan["files"]** (sequential):
     a. **S3 Verification** (mandatory pre-check):
        - Call get_etag(bucket_name, s3_key, s3_client) using values from plan
        - Compare with expected_etag from plan
        - If mismatch: record FAILURE ("S3 corruption"), skip to next file
     b. **Local file check**:
        - Check if file exists at absolute_dest_path (from plan)
        - If exists: calculate etag with get_file_hash_with_etag(chunk_size_mb from plan)
        - If matches expected_etag: skip, record "OK - Skipped"
        - If mismatch: delete file, proceed to download
     c. **Download**:
        - Call download_file_from_s3(bucket_name, s3_key, absolute_dest_path, chunk_size_mb)
        - All parameters from plan
        - Log size, duration, rate
     d. **Post-download verification**:
        - Calculate etag with get_file_hash_with_etag(chunk_size_mb from plan)
        - Compare with expected_etag from plan
        - If mismatch: delete file, record FAILURE ("Verification failed")
        - If match: record "OK - Downloaded" or "OK - Self-healed"
     e. Continue to next file (best-effort)
   - Store all file_results for this raw_file
9. **Group results by destination directory**
10. **For each destination**: Write batch status report
    - Format: see Section 2.5
    - Include all raw_files for this destination
    - Overall summary with counts
11. Count total failures across all destinations
12. If any failures: raise AirflowFailException with paths to status files
13. Log success message

#### Helper Function: _build_download_plan()

**Purpose**: Construct the complete download plan dictionary (PHASE 1)

**Parameters**:
- raw_file_ids: List of raw_file_id strings
- kwargs: Contains YAML config access

**Returns**: Dict[raw_file_id, download_plan_entry] where each entry contains:
- project_id, destination_base, bucket_name, files list, error, skipped

**Implementation**: Follows step 4 from PHASE 1 above

**Benefits**:
- Testable separately from execution
- Can be logged/inspected before execution
- All parsing and path construction in one place

#### Helper Function: _write_batch_status_report()

**Purpose**: Write batch status report for a destination

**Parameters**:
- destination_path: Path to output directory
- raw_file_results: List of (raw_file_id, project_id, bucket, file_results) tuples
- timestamp: Report timestamp

**Returns**: Path to written status file

**Implementation**: See Section 2.5 for format

**Tests**:
- test_build_download_plan_single() - single raw_file, verify structure
- test_build_download_plan_batch() - multiple raw_files, verify all parsed
- test_build_download_plan_invalid_raw_file() - marks as skipped with error
- test_build_download_plan_etag_parsing() - verify etag and chunk_size extracted
- test_build_download_plan_paths() - verify absolute paths constructed correctly
- test_download_raw_files_batch_single_project() - 2 raw_files, same project
- test_download_raw_files_batch_multiple_projects() - different destinations
- test_download_raw_files_s3_etag_mismatch() - S3 corruption detected
- test_download_raw_files_new_file() - fresh download
- test_download_raw_files_skip_existing() - correct hash
- test_download_raw_files_self_healing() - wrong hash, overwrite
- test_download_raw_files_multi_file() - .d folder
- test_download_raw_files_partial_failure() - some files fail
- test_download_raw_files_raw_file_not_found() - skip invalid raw_file
- test_batch_status_report_format() - verify txt content
- test_batch_status_report_per_destination() - multiple reports

---

### Stage 4: S3 Downloader DAG (~3 hours)

**File**: airflow_src/dags/s3_downloader.py (new file)

**Purpose**: S3 Downloader DAG - Restore raw files from S3 to output location (BATCH MODE)

**DAG Configuration**:
- Name: From Dags.S3_DOWNLOADER constant
- Schedule: None (manual trigger only)
- Tags: ["s3", "download", "manual", "restoration", "batch"]
- Queue: kraken_queue_s3_download
- Retries: 2
- Retry delay: 5 minutes
- Execution timeout: 12 hours (for large batches or files)

**DAG Parameters**:
- raw_file_ids: String parameter (minimum 3 characters)
  - Description: Comma-separated list of RawFile IDs to download from S3 (e.g., "file1,file2,file3"). Files downloaded to output_location/project_id/ per raw_file. Multiple projects supported in single batch.
  - Examples:
    - Single: "20250115_sample"
    - Batch: "20250115_sample,20250116_sample,20250117_sample"

**Tasks**:
1. download_files: Runs download_raw_files_from_s3()
   - Uses pool: S3_DOWNLOAD_POOL
   - Processes each raw_file sequentially
   - Best-effort: continues on per-file errors
   - Timeout: 12 hours

**Task Dependencies**: None (single task)

**Tests**:
- test_s3_downloader_dag_exists()
- test_s3_downloader_dag_structure() - single task
- test_s3_downloader_dag_params() - raw_file_ids (plural)

---

### Stage 5: Documentation & Integration (~2 hours)

#### 5.1 Create Airflow Pool

In Airflow UI (Admin → Pools):
- **Name**: s3_download_pool
- **Slots**: 2
- **Description**: "Limit concurrent S3 downloads"

#### 5.2 Update CLAUDE.md

Add after S3 backup section:

**Section: S3 Download Operations**

**Trigger Download**:
- Single file: `airflow dags trigger s3_downloader --conf '{"raw_file_ids": "20250115_sample"}'`
- Batch: `airflow dags trigger s3_downloader --conf '{"raw_file_ids": "file1,file2,file3"}'`
- Downloads raw files from S3 to output_location/project_id/ (per raw_file)

**Behavior**:
- **Batch Mode**: Supports comma-separated list of raw_file_ids
- **Destination**: Automatically calculated as locations.output/project_id/ (per raw_file)
- **S3 Verification**: Checks S3 etag before downloading (detects S3 corruption)
- **Self-healing**: Corrupted local files automatically overwritten
- **Idempotent**: Safe to re-run (skips files with correct etags)
- **Best-effort**: Continues processing all raw_files even if some fail
- **Status**: Creates _download_status_batch_{timestamp}.txt per destination
- **Database**: Read-only (no backup_status updates)

**Check Results**:
- View status report: `cat /fs/pool/output/{PROJECT_ID}/_download_status_batch_*.txt`
- Multiple projects in batch → multiple status reports (one per destination)

**Status Report Format**:
- Header with batch metadata
- Per-RawFile sections with file results
- Overall summary with counts and status

#### 5.3 Update S3_BACKUP_DESIGN.md

Add before "Future Enhancements":

**Section: S3 Download Feature**

**Overview**:
- Manual restoration of raw files from S3 to output location
- Supports batch mode (multiple raw_files in one operation)
- See design_docs/S3_DOWNLOAD_FEATURE.md for full details

**Key Features**:
- **Batch Mode**: Comma-separated list of raw_file_ids
- **Destination**: output_location/project_id/ (auto-calculated per raw_file)
- **S3 Verification**: Checks S3 etag before download (detects corruption)
- **Self-healing**: Overwrites corrupted files automatically
- **Idempotent**: Safe to re-run (skips files with correct etags)
- **Status**: Batch .txt report per destination (no DB updates)
- **Best-effort**: Continues processing all raw_files even if some fail

**Usage**:
- Single: `airflow dags trigger s3_downloader --conf '{"raw_file_ids": "FILE_ID"}'`
- Batch: `airflow dags trigger s3_downloader --conf '{"raw_file_ids": "ID1,ID2,ID3"}'`
- Check results: `/fs/pool/output/{PROJECT_ID}/_download_status_batch_*.txt`

#### 5.4 Testing Checklist

**Unit Tests** (~5 hours):

**S3 Utilities**:
- [ ] test_download_file_from_s3() - mock boto3, verify download_fileobj called
- [ ] test_reconstruct_s3_paths_bucket_only() - "bucket"
- [ ] test_reconstruct_s3_paths_with_prefix() - "bucket/prefix/"

**ETag Verification**:
- [ ] test_s3_etag_verification_match() - S3 etag matches DB
- [ ] test_s3_etag_verification_mismatch() - S3 corruption detected
- [ ] test_local_etag_verification_match() - post-download success
- [ ] test_local_etag_verification_mismatch() - delete corrupted file

**Download Plan Construction** (PHASE 1 - Planning):
- [ ] test_build_download_plan_single() - single raw_file, verify structure
- [ ] test_build_download_plan_batch() - multiple raw_files, all fields populated
- [ ] test_build_download_plan_invalid_raw_file() - marks as skipped with error

**Download Execution** (PHASE 2 - Doing):
- [ ] test_download_raw_files_single() - single raw_file
- [ ] test_download_raw_files_new_file() - fresh download with verification
- [ ] test_download_raw_files_skip_existing() - correct etag, no download
- [ ] test_download_raw_files_partial_failure() - some files fail, continue
- [ ] test_download_raw_files_raw_file_not_found() - skip invalid, continue
- [ ] test_download_raw_files_missing_project_id() - record error, continue
- [ ] test_download_raw_files_s3_corruption() - S3 etag mismatch, skip file
- [ ] test_download_raw_files_uses_plan() - verify execution uses plan values

**Status Reporting**:
- [ ] test_batch_status_report_format() - verify txt content structure
- [ ] test_batch_status_report_per_destination() - multiple reports created
- [ ] test_batch_status_report_single_destination() - one report for batch

**DAG Structure**:
- [ ] test_s3_downloader_dag_exists()

**Integration Tests** (manual):

**Single RawFile Tests**:
- [ ] Fresh download from S3 (new file)
- [ ] Idempotent: re-run, files skipped (correct etags)
- [ ] Self-healing: corrupt local file, gets overwritten
- [ ] Multi-file: .d folder with 10+ files
- [ ] Large file: >10 GB
- [ ] S3 etag verification: tamper with S3 file, detect mismatch

**Batch Mode Tests**:
- [ ] Batch: 3 raw_files, same project (one report)
- [ ] Batch: 5 raw_files, 3 different projects (three reports)
- [ ] Batch: mix of existing and new files
- [ ] Batch: one raw_file fails validation, others succeed
- [ ] Batch: partial failures across multiple raw_files
- [ ] Batch: re-run failed batch (idempotent)

**Error Handling Tests**:
- [ ] Invalid raw_file_id: logs error, continues
- [ ] Missing project_id: logs error, continues
- [ ] Network error during download: records failure, continues
- [ ] S3 bucket doesn't exist: records failures
- [ ] Post-download verification failure: deletes file
- [ ] Disk full during download: handles gracefully (OS error)

**Status Report Tests**:
- [ ] Status txt format correct (all sections present)
- [ ] Multiple status reports created (different destinations)
- [ ] Status report includes all raw_files for destination
- [ ] Status report shows correct success/failure counts
- [ ] No automatic cleanup (old reports preserved)

**Critical Verification**:
- [ ] **Verify NO DB writes** (check backup_status unchanged after downloads)
- [ ] **Verify etag chunk_size used** (same as upload: 500 MB)
- [ ] **Verify S3 pre-check happens** (check logs for S3 etag retrieval)
- [ ] **Verify post-download check happens** (check logs for local etag calc)

---

## 4. Error Handling

### 4.1 Error Scenarios

| Condition | Behavior |
|-----------|----------|
| **Pre-Processing Errors** | |
| No raw_file_ids provided | Fail DAG (parameter validation) |
| **Per-RawFile Errors (Best-Effort)** | |
| RawFile not found in DB | Log error for this raw_file, record in status, continue to next |
| Missing project_id | Log error for this raw_file, record in status, continue to next |
| No S3 backup (s3_upload_path null) | Log error for this raw_file, record in status, continue to next |
| Empty file_info | Log error for this raw_file, record in status, continue to next |
| Output location not writable | Log error for this raw_file, record in status, continue to next |
| **Per-File Errors (Best-Effort)** | |
| S3 etag mismatch (pre-check) | Record FAILURE ("S3 corruption"), skip file, continue to next |
| S3 bucket doesn't exist | Record FAILURE for all files in this raw_file, continue to next raw_file |
| S3 key not found (404) | Record FAILURE for that file, continue to next |
| Network error during download | Record FAILURE for that file, continue to next |
| Download timeout | Record FAILURE for that file, continue to next |
| Post-download etag mismatch | Delete corrupted file, record FAILURE ("Verification failed"), continue |
| Local file corrupted (exists) | Delete & re-download (self-healing) |
| Disk full during download | Record FAILURE, may affect remaining files (OS error) |
| **Final Status** | |
| Some raw_files/files fail | Write status txt per destination, DAG task fails |
| All succeed | Write status txt per destination, DAG task succeeds |
| Multiple projects in batch | Each destination gets separate status report |

### 4.2 Edge Cases

#### 4.2.1 Batch Mode with Multiple Projects
- Parse comma-separated raw_file_ids correctly (trim whitespace)
- Each raw_file may have different project_id → different destinations
- Group status reports by destination directory
- Example: 5 raw_files across 3 projects → 3 separate status reports

#### 4.2.2 Multi-File Downloads (.d folders)
- Iterate all files in file_info
- Best-effort: continue even if some fail
- Record all outcomes in status txt
- Task fails if any failures (operator reviews txt)

#### 4.2.3 Idempotency
- Safe to re-run multiple times (batch or single)
- Skips files with correct etags (verified with S3 and locally)
- Re-downloads files with wrong etags
- Can re-run partial batches or full batches

#### 4.2.4 S3 Path Parsing
- Formats: "bucket", "bucket/", "bucket/prefix/"
- Handle trailing slashes, multiple slashes, empty prefixes

#### 4.2.5 Missing Project ID in Batch
- Log error for that raw_file
- Record in status report
- Continue to other raw_files (best-effort)
- Task still fails at end if any errors

#### 4.2.6 ETag Format Variations
- Single-part files: simple MD5 (e.g., "abc123__500")
- Multipart files: MD5-partcount (e.g., "abc123-5__500")
- Empty files: known MD5 of empty string
- Must parse chunk_size from stored etag string

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
- IAM permissions: s3:GetObject, s3:ListBucket

### 5.2 Path Safety
- Destination always calculated from trusted sources
- No user-provided paths
- All file_info paths are relative (no ..)

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

- **Disk space pre-check**: Upfront validation of available disk space before starting downloads
  - Calculate total size needed from all raw_files
  - Group by destination directory (different project_ids)
  - Fail fast if insufficient space (with 10% buffer)
  - Would require additional task in DAG or integration into planning phase
- Automatic fallback during processing
- Parallel file downloads
- Resume capability
- Progress reporting
- Selective downloads (specific files from file_info)
- Compression support
- Notifications (email/webhook on completion)

---

## 10. Estimated Effort

| Stage | Time | Notes |
|-------|------|-------|
| S3 Utilities | 3.5 hours | 3 new functions, reference existing utilities |
| Constants | 30 min | 2 constants |
| Download Logic | 9 hours | 3 functions: build plan, execute downloads, write status |
| DAG Creation | 2 hours | Single task DAG |
| Documentation | 2 hours | CLAUDE.md, S3_BACKUP_DESIGN.md updates |
| Unit Tests | 5 hours | Comprehensive coverage including plan construction tests |
| **Total** | **~22 hours (~3 days)** | |

With contingency: **~27 hours (~3-4 days)**

**Increased from original estimate due to**:
- Batch mode support (comma-separated raw_file_ids)
- Two-phase architecture (planning + execution separation)
- Mandatory S3 etag verification before download
- Grouped status reports per destination
- Additional error handling for best-effort processing
- Separate download plan construction and testing

---

## 11. Summary

Clean, reliable S3 download implementation with batch support:
- ✅ Batch mode (comma-separated raw_file_ids)
- ✅ Auto-destination (YAML + project_id per raw_file)
- ✅ S3 etag verification (detect S3 corruption before downloading)
- ✅ Self-healing (overwrites corrupted local files)
- ✅ Idempotent (safe to re-run batches)
- ✅ Status tracking via batch txt per destination (NO DB writes)
- ✅ Best-effort (continues on errors, tries all raw_files)
- ✅ Clear operator feedback (grouped status reports)
- ✅ Multi-file support (.d folders)
- ✅ Hash verification (pre-download S3 check + post-download local check)
- ✅ Multi-project batches (different destinations supported)

**Architecture**:
- Two-phase design: Planning phase (construct download plan) + Execution phase (execute plan)
- Planning phase: Parse all DB data, calculate all paths, extract all etags
- Execution phase: Simply iterate plan and download with verification
- Benefits: Easier testing, debugging, clearer code, better error messages

**Files to create/modify**:
1. dags/impl/s3_utils.py - 3 new functions (download, reconstruct paths, parse etag)
2. dags/impl/handler_impl.py - 3 functions (build plan, download batch, write status)
3. dags/s3_downloader.py - New DAG with single task
4. plugins/common/constants.py - 2 constants
5. CLAUDE.md - Usage docs (batch mode)
6. S3_BACKUP_DESIGN.md - Reference to download feature
7. Tests - Comprehensive unit + integration tests (including plan construction tests)

**Existing utilities to reuse**:
- `get_s3_client()`, `get_transfer_config()`, `get_etag()` - s3_utils.py
- `get_file_hash_with_etag()` - file_handling.py
- `ETAG_SEPARATOR` constant - file_handling.py
- RawFile model with file_info structure

**Critical principles**:
- Read-only DB access (NO writes to backup_status)
- Self-healing over error handling
- Batch status txt is single source of truth (one per destination)
- Destination always derived from config
- Mandatory etag verification (S3 pre-check + local post-check)
- Best-effort processing (continue on per-file/per-raw_file errors)
