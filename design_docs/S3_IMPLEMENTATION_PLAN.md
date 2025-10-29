# S3 Backup Implementation Plan

Based on the design document and codebase research, here's the step-by-step implementation plan.

## Stage 1: Foundation & Configuration (2-3 hours)

**Goal**: Set up database schema, configuration structure, and dependencies
**Success Criteria**: Config files updated, DB fields added, boto3 dependency added, tests pass
**Status**: Not Started

### 1.1 Add Database Fields
- Modify `shared/db/models.py` → RawFile model
- Add `s3_backup_key = StringField(max_length=1024, default=None)`
- Add `s3_etag = StringField(max_length=128, default=None)`

### 1.2 Add Configuration Structure
- Modify `shared/yamlsettings.py` → Add YamlKeys.Backup class with:
  - `TYPE = "backup.backup_type"`
  - `S3_REGION = "backup.s3.region"`
  - `S3_BUCKET_PREFIX = "backup.s3.bucket_prefix"`
- Update all 3 config files (`envs/alphakraken.*.yaml`):
  ```yaml
  backup:
    backup_type: local  # or 's3'
    s3:
      region: eu-central-1
      bucket_prefix: alphakraken
  ```

### 1.3 Add Dependencies
- Modify `airflow_src/requirements_airflow.txt` → Add `boto3>=1.28.0`
- Add task constant in `airflow_src/plugins/common/keys.py` → `UPLOAD_TO_S3 = "upload_to_s3"`

### 1.4 Write Tests (Foundation)
- Create test file structure in `airflow_src/tests/dags/impl/test_s3_backup.py`

---

## Stage 2: S3 Utility Functions (3-4 hours)

**Goal**: Create reusable S3 helper functions with full test coverage
**Success Criteria**: All utility functions tested and working correctly
**Status**: Not Started

### 2.1 Create S3 Helper Module
- Create `airflow_src/dags/impl/s3_utils.py` with:
  - `build_s3_key(project_id, instrument_id, relative_path)` - Construct S3 object keys with prefix pattern `project-{id}/instrument-{id}/path`
  - `calculate_s3_etag(file_path, chunk_size_mb)` - Multipart ETag calculation
  - `get_s3_client(region)` - Boto3 client

The implementation should rely on Airflow AWS connections, which holds credentials. E.g.
```python
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='s3')
        s3_client = hook.get_conn()

        response = s3_client.list_buckets()
```

### 2.2 Write Unit Tests
- Test `build_s3_key()` path construction with various project/instrument IDs
- Test `calculate_s3_etag()` against known values
- Mock boto3 client

### 2.3 Run Tests
- `pytest airflow_src/tests/dags/impl/test_s3_backup.py::test_s3_utils -v`

---

## Stage 3: Core Upload Logic (4-5 hours)

**Goal**: Implement main S3 upload function with error handling
**Success Criteria**: Upload function handles single/multi-file, validates ETags, fails gracefully
**Status**: Not Started

### 3.1 Implement Upload Function
- Create `upload_raw_file_to_s3()` in `airflow_src/dags/impl/s3_backup.py`:
  1. Check config: short-circuit if `backup_type != 's3'`
  2. Get file_info from XCom
  3. Pre-validate bucket exists with `head_bucket()`
  4. Check for duplicate: if S3 object exists with matching ETag, skip upload
  5. Set `backup_status = BackupStatus.IN_PROGRESS`
  6. For each file in file_info dict:
     - Calculate local ETag
     - Upload with `upload_file()` (boto3 handles multipart automatically)
     - Verify remote ETag matches
  7. On success: set `backup_status = DONE`, save `s3_backup_key` and `s3_etag`
  8. On ANY failure: set `backup_status = FAILED`, log error, don't raise (non-blocking)

### 3.2 Error Handling
- Wrap all S3 operations in try/except
- Handle: `NoCredentialsError`, `ClientError`, `BotoCoreError`
- Log detailed error messages with file paths and exception details
- Use `abort_multipart_upload()` in exception handler for cleanup

### 3.3 Write Integration Tests
- Test successful single-file upload (Thermo .raw)
- Test multi-file upload (Bruker .d folder structure)
- Test duplicate detection (re-run scenario)
- Test missing bucket error (should fail gracefully)
- Test ETag mismatch (corruption detection)
- Test partial upload failure (all-or-nothing behavior)
- Mock boto3 S3 client for all tests

### 3.4 Run Tests
- `pytest airflow_src/tests/dags/impl/test_s3_backup.py -v`

---

## Stage 4: DAG Integration (2-3 hours)

**Goal**: Integrate S3 upload task into Airflow DAG
**Success Criteria**: DAG loads successfully, task chain correct, local backup behavior unchanged
**Status**: Not Started

### 4.1 Modify Local Backup Task
- Update `copy_raw_file()` in `dags/impl/handler_impl.py`:
  - Check `backup_type` at start
  - If `backup_type == 's3'`: skip setting `backup_status` (leave for S3 task)
  - If `backup_type == 'local'`: current behavior (set `backup_status`)

### 4.2 Add S3 Upload Task to DAG
- Modify `dags/acquisition_handler.py`:
  - Import `upload_raw_file_to_s3`
  - Define task:
    ```python
    upload_to_s3_ = PythonOperator(
        task_id=Tasks.UPLOAD_TO_S3,
        python_callable=upload_raw_file_to_s3,
        op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        execution_timeout=timedelta(hours=6),
        retries=3,
        retry_delay=timedelta(minutes=5),
        pool=Pools.FILE_COPY_POOL,
        queue="kraken_queue_s3_uploader",  # Dedicated queue for S3 operations
    )
    ```
  - Update chain: `copy_raw_file_ >> [upload_to_s3_, start_file_mover_] >> start_file_mover_`
    - (Parallel execution: S3 upload + file_mover run simultaneously)

### 4.3 Worker Configuration
- Add to `docker-compose.yaml` or worker deployment config:
  - New worker service/container listening to `kraken_queue_s3_uploader` queue
  - Worker needs AWS credentials via environment variables
  - Consider separate scaling configuration for S3 workers vs. processing workers

### 4.4 Test DAG Structure
- Validate DAG loads: `python airflow_src/dags/acquisition_handler.py`
- Check task dependencies are correct

---

## Stage 5: Testing & Validation (3-4 hours)

**Goal**: Comprehensive testing and quality checks
**Success Criteria**: All tests pass, no linting errors, coverage acceptable
**Status**: Not Started

### 5.1 Run Full Test Suite
- `python -m pytest airflow_src/tests/ -v`
- Ensure all tests pass
- Check coverage: `pytest --cov=airflow_src/dags/impl/s3_backup --cov=airflow_src/dags/impl/s3_utils`

### 5.2 Pre-commit Checks
- `pre-commit run --all-files`
- Fix any linting/formatting issues

### 5.3 Manual Testing Checklist (documented in design doc)
- Deploy to sandbox with `backup_type: local` (no behavior change)
- Switch to `backup_type: s3`, test single instrument
- Test large file upload (>1GB)
- Test multi-file instrument (.d folder)
- Verify ETag verification works
- Test re-run scenario (duplicate detection)
- Test missing bucket error handling

---

## Stage 6: Documentation & Deployment (1-2 hours)

**Goal**: Update documentation and commit changes
**Success Criteria**: Documentation complete, changes committed
**Status**: Not Started

### 6.1 Update Documentation
- Add S3 configuration section to `CLAUDE.md`:
  - Bucket naming convention
  - AWS credentials setup
  - Config switch instructions
- Update with deployment prerequisites from design doc section 8.3

### 6.2 Commit Changes
- Commit with message linking to design doc
- Include all modified files in single atomic commit

---

## Success Criteria

✅ All tests passing
✅ `backup_type: local` works unchanged
✅ `backup_type: s3` uploads files to correct bucket
✅ ETag verification prevents corrupted uploads
✅ Multi-file instruments supported
✅ Failures are non-blocking (processing continues)
✅ Re-runs skip duplicate uploads
✅ Pre-commit checks pass

---

## Key Implementation Decisions

Based on clarifying questions answered during planning:

### 1. Execution Mode: Asynchronous Queue
**Decision**: Use dedicated Celery queue for S3 uploads (similar to file_mover/file_remover pattern)
- **Rationale**: Faster overall pipeline by running uploads in parallel with other tasks
- **Implementation**: Create `kraken_queue_s3_uploader` worker queue
- **Trade-off**: More complex than synchronous, but significantly better throughput
- S3 upload task runs **in parallel** with `start_file_mover_`
- Processing continues immediately after local copy completes
- S3 upload failures do not block downstream tasks
- DAG chain: `copy_raw_file_ >> [upload_to_s3_, start_file_mover_] >> start_file_mover_`

### 2. AWS Authentication: Access Key/Secret
**Decision**: Use explicit AWS credentials stored Airflow connection
- **Rationale**: More portable for testing across environments (local/sandbox/production)
- **Configuration**: Credentials Airflow connection by boto3

### 3. Failure Handling: Mark Failed, Continue
**Decision**: On S3 upload failure, set `backup_status = FAILED` but don't block processing pipeline
- **Rationale**: Processing pipeline should continue even if backup fails
- **Implementation**: Catch all exceptions, log error, update status, don't raise
- **Recovery**: Manual intervention or automated retry job can process FAILED backups later
- **Monitoring**: Failed backups visible in database queries and monitoring dashboards

### 4. Bucket Strategy: project-specific buckets with fixed Prefixes
**Decision**: project-specific buckets with fixed Prefixes

### 5. Multi-file Handling (Strict All-or-Nothing)
- If any file in a multi-file set fails to upload, mark entire backup as FAILED
- Stop immediately on first failure, don't continue with remaining files
- Leave partial uploads in S3 (S3 lifecycle policy will clean up incomplete multiparts)
- Simpler than atomic rollback, relies on retry mechanism for full re-upload

### 6. Bucket Validation (Pre-validate)
- Check bucket existence with `head_bucket()` before starting upload
- Fail fast with clear error message if bucket missing
- Better user experience than letting upload fail with cryptic boto3 error

### 7. Re-run Handling (Check and Skip)
- Before uploading, check if S3 object already exists
- If exists with matching ETag, skip upload and update status to DONE
- Prevents redundant uploads on task retries
- Similar to `_identical_copy_exists()` pattern in local backup

---

## Estimated Total Time: 15-21 hours

## Files Modified Summary

**Modified:**
1. `shared/db/models.py` - Add S3 fields
2. `shared/yamlsettings.py` - Add backup config keys
3. `envs/alphakraken.*.yaml` (all 3) - Add backup config section
4. `airflow_src/plugins/common/keys.py` - Add UPLOAD_TO_S3 task constant
5. `airflow_src/dags/impl/handler_impl.py` - Modify copy_raw_file() for S3 mode
6. `airflow_src/dags/acquisition_handler.py` - Add S3 upload task
7. `airflow_src/requirements_airflow.txt` - Add boto3

**Created:**
8. `airflow_src/dags/impl/s3_utils.py` - S3 helper functions
9. `airflow_src/dags/impl/s3_backup.py` - Main upload logic
10. `airflow_src/tests/dags/impl/test_s3_backup.py` - Tests
