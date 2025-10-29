# S3 Raw File Backup - High-Level Design Document

## 1. Overview

Add S3 backup capability as an alternative to local backup, configurable via YAML. When `backup_type: s3` in .yaml config, raw files are backed up to project-specific S3 buckets instead of local filesystem.

## 2. Architecture Decisions (Based on User Input)

### 2.1 Backup Flow
- **Sequential**: Local backup always runs first → S3 backup runs after (if enabled). This is to avoid transferring files from the instruments twice.
- **Status Field**: Reuse single `backup_status` field (not split)
  - When `backup_type: local` → `copy_raw_file_` writes to `backup_status`
  - When `backup_type: s3` → `copy_raw_file_` skips writing, to `backup_status` `upload_to_s3_` writes to `backup_status`

### 2.2 Failure Handling
- S3 upload failures are **non-blocking**
- Downstream tasks (file_mover, processing) continue regardless
- Failed S3 uploads: backup_status=FAILED, logged, require manual intervention

### 2.3 Task Dependencies - CLARIFIED
**Decision**: S3 upload runs **in parallel** with `start_file_mover_` (non-blocking)
- DAG chain: `copy_raw_file_ >> [upload_to_s3_, start_file_mover_] >> start_file_mover_`
- Processing continues immediately after local copy completes
- S3 upload happens independently in background
- Failures do not block downstream processing tasks

### 2.3 S3 Organization
- **One bucket per project**: `{bucket_prefix}-{project_id}` (e.g., `mpg-mpi-mann-alphakraken-PRID001`), with bucket_prefix taken from .yaml config
- **Key structure**:
  - `./{file_id}.raw` (Thermo)
  - ignore the Bruker or Sciex cases for now, but prepare the architecture to be extensible to them (multiple files)
- **Buckets**: Assumed to exist (created manually via console/IaC)

### 2.4 Upload Configuration
- **Chunk size**: 500 MB (suitable for 10-100 GB proteomics files)
- **Authentication**: AWS credentials via environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)

## 3. Database Schema Changes

### 3.1 New Fields in RawFile Model
```python
# In shared/db/models.py
s3_backup_key = StringField(max_length=1024, default=None)  # S3 object key prefix
s3_etag = StringField(max_length=128, default=None)         # For verification
```

**Note**: `backup_status` field remains unchanged, semantics depend on `backup_type` config.

## 4. Configuration Changes

### 4.1 YAML Configuration Structure
```yaml
# In envs/alphakraken.{env}.yaml
backup:
  backup_type: local  # or 's3'

  # S3-specific config (only used when backup_type: s3)
  s3:
    region: eu-central-1
    bucket_prefix: alphakraken  # Results in alphakraken-{project_id}
```

### 4.2 Environment Variables
```bash
# In .env file
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

## 5. Implementation Components

### 5.1 New Task: `upload_to_s3_`

**Location**: `dags/acquisition_handler.py`

**Function**: `backup_raw_file_to_s3()` in `dags/impl/handler_impl.py`

**Logic Flow**:
1. Check config: if `backup_type != 's3'`, short-circuit (skip)
2. Set status: `backup_status = BackupStatus.IN_PROGRESS`
3. For each file in `raw_file.file_info`:
   - Calculate local file ETag (MD5-based, matching S3 multipart)
   - Construct S3 key: `{file_id}/{relative_path}`
   - Upload using boto3 multipart (500 MB chunks)
   - Verify: compare local ETag with S3 response ETag
4. On success: set `backup_status = BackupStatus.DONE`, save `s3_backup_key` prefix
5. On failure: set `backup_status = BackupStatus.FAILED`, log error, **don't raise** (non-blocking)

### 5.2 Modified Task: `copy_raw_file_`

**Changes**:
- Check `backup_type` config at start
- If `backup_type == 's3'`: skip updating `backup_status` field (leave for S3 task)
- If `backup_type == 'local'`: current behavior (update `backup_status`)

### 5.3 New Module: `shared/s3_utils.py`

**Functions**:
- `calculate_s3_etag(file_path, chunk_size)` → str
  - Calculate MD5 of each chunk, then MD5 of concatenated hashes
  - Returns ETag format: `"{hash}"-{part_count}` for multipart
- `upload_file_multipart(file_path, bucket, key, chunk_size, region)` → str
  - Use boto3 S3 client multipart upload API
  - Return S3 ETag from response
  - see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html for documentation of boto3
- `verify_s3_upload(local_etag, s3_etag)` → bool
  - Compare ETags, accounting for format differences

### 5.4 DAG Changes

**Updated task chain** in `acquisition_handler.py`:
```python
(
    monitor_acquisition_
    >> compute_checksum_
    >> copy_raw_file_
    >> [upload_to_s3_, start_file_mover_]
    >> start_file_mover_
    >> decide_processing_
    >> start_acquisition_processor_
)
```

**Task definition**:
```python
upload_to_s3_ = PythonOperator(
    task_id=Tasks.UPLOAD_TO_S3,
    python_callable=backup_raw_file_to_s3,
    op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
    execution_timeout=timedelta(hours=6),  # Large files need time
    retries=3,
    retry_delay=timedelta(minutes=5),
)
```

## 6. Error Handling & Edge Cases

### 6.1 Happy Path Errors
| Error Condition                    | Behavior |
|------------------------------------|----------|
| Bucket doesn't exist               | Fail task, set `backup_status=FAILED`, log error. Manual bucket creation required. |
| Network interruption during upload | Boto3 retries internally. Task retries 3x (Airflow). Then fails gracefully. |
| ETag mismatch after upload         | Set `backup_status=FAILED`, log discrepancy, don't block downstream. |
| (aall other exceptions)            | Fail task immediately, set `backup_status=FAILED`. |

### 6.2 Edge Cases to Handle

#### 6.2.1 Multiple Files in `.d` Folder - CLARIFIED
- Iterate over `raw_file.file_info` dict (all files already inventoried)
- Upload each file maintaining directory structure
- **Failure strategy**: **STRICT ALL-OR-NOTHING** (v1 implementation)
  - If any file in the set fails to upload, stop immediately
  - Set entire backup status to FAILED
  - Leave partial uploads in S3 (S3 lifecycle policy cleans up incomplete multiparts after 7 days)
  - Retry mechanism will re-upload entire file set from scratch
  - **Future**: Add per-file status tracking if needed

#### 6.2.2 Multiple Files `.wiff*` Folder - CLARIFIED
- Iterate over `raw_file.file_info` dict (all files already inventoried)
- Upload each file to a single S3 key prefix, e.g. `raw_file_id/raw_file_id.wiff`, `raw_file_id/raw_file_id.wiff2`
- **Failure strategy**: **STRICT ALL-OR-NOTHING** (same as .d folders)
  - Stop on first failure, mark FAILED, rely on retry for full re-upload

#### 6.2.3 Partial Upload Cleanup
- If upload fails mid-way, orphaned parts remain in S3
- **Solution**: Use `abort_multipart_upload()` in exception handler
- **S3 Lifecycle**: Set bucket policy to auto-delete incomplete multiparts after 7 days

#### 6.2.4 Duplicate Uploads (Re-runs) - CLARIFIED
- If task is retried after partial success
- **CHECK AND SKIP** strategy (v1 implementation):
  - Before upload, check if S3 object exists using `head_object()`
  - If exists, verify ETag matches expected value
  - If verified → skip upload, just update status to DONE
  - If ETag mismatch → re-upload (data corruption detected)
- Similar to `_identical_copy_exists()` pattern in local backup

#### 6.2.5 Bucket Name Collisions
- Project IDs might conflict (e.g., `PRJ-001` vs `prj.001`)
- **Solution**: Normalize bucket names: lowercase, replace special chars with hyphens
- **Validation**: Add function `normalize_bucket_name(project_id)`

#### 6.2.6 Bucket Validation - CLARIFIED
- **PRE-VALIDATE** strategy (v1 implementation):
  - At start of upload task, check bucket exists using `head_bucket()`
  - Fail fast with clear error message if bucket missing
  - Better user experience than cryptic boto3 ClientError
  - Log bucket name and region for debugging


### 6.3 Monitoring & Observability

#### 6.3.1 Logging
- Log upload start/completion for each file
- Log total size, duration, transfer rate
- Log ETag calculation and verification results
- Use structured logging for easy parsing


## 7. Testing Strategy

### 7.1 Unit Tests
- `test_calculate_s3_etag()`: Verify ETag calculation matches AWS format
- `test_upload_file_multipart()`: Mock boto3, verify API calls
- `test_upload_to_s3_skip_when_local()`: Verify short-circuit logic
- `test_copy_raw_file_skip_status_when_s3()`: Verify conditional status update

### 7.2 Manual Testing Checklist
- [ ] Local → S3 migration: existing local backup, enable S3, verify works
- [ ] Fresh S3 backup: new acquisition with `backup_type: s3`
- [ ] Large file (>10 GB): monitor multipart upload progress
- [ ] Network interruption: kill connection mid-upload, verify retry
- [ ] Missing bucket: verify graceful failure and logging
- [ ] ETag mismatch: corrupt file post-upload, verify detection

## 8. Deployment & Migration

### 8.1 Phased Rollout
1. **Phase 1**: Deploy to sandbox environment with `backup_type: local` (no behavior change)
2. **Phase 2**: Test with single instrument in sandbox using `backup_type: s3`
3. **Phase 3**: Monitor for 1 week, verify stability
4. **Phase 4**: Roll out to production for low-priority instruments
5. **Phase 5**: Gradually migrate all instruments

### 8.2 Rollback Strategy
- Configuration-based: set `backup_type: local` to revert
- No data loss: local backups still available
- Database: `s3_backup_key` and `s3_etag` fields can remain null (no migration needed to roll back)

### 8.3 Prerequisites
- [ ] Create S3 buckets for all existing projects (manual/Terraform)
- [ ] Set bucket policies: private, no public access
- [ ] Configure IAM user with PutObject, GetObject, AbortMultipartUpload permissions
- [ ] Add AWS credentials to Airflow environment
- [ ] Add boto3 to `requirements_airflow.txt`
- [ ] Set S3 lifecycle policy to clean incomplete multipart uploads

## 9. Future Enhancements

### 9.1 Not in Scope for v1 (Document for Later)
- **Resume interrupted uploads**: Track part numbers, resume from last successful part
- **Parallel uploads**: Upload multiple files concurrently
- **Compression**: Compress before upload (may not be beneficial for .d files)
- **Storage class optimization**: Immediate Archive → Glacier after processing
- **Cross-region replication**: Disaster recovery
- **S3 → local restore**: Download from S3 if local backup lost
- **Encryption**: Server-side encryption (SSE-S3 or SSE-KMS)
- **Presigned URLs**: Generate URLs for direct download
- **Cost monitoring**: Track S3 storage and transfer costs per project
- **Intelligent-Tiering**: Auto-move to cheaper storage classes

### 9.2 Known Limitations
- No resume capability for failed uploads (retry from scratch)
- No validation that project bucket exists before upload
- No automatic bucket creation (requires manual setup)
- Single-threaded upload per file (no parallelization)
- No progress bar/percentage in Airflow UI

### 9.3 Performance Considerations
- 500 MB chunks = ~2 minutes per chunk on 100 Mbps connection
- 50 GB file = ~100 parts = ~3.3 hours upload time
- **Bottleneck**: Network bandwidth, not CPU/disk
- **Future**: Use S3 Transfer Manager for auto-optimization

## 10. Security Considerations

### 10.1 Access Control
- IAM user has minimal permissions (no DeleteObject initially)
- Buckets are private by default
- No public access policies
- Consider bucket policies to restrict access by project

### 10.2 Credentials Management
- Store AWS credentials in Airflow Variables (encrypted at rest)
- Rotate credentials periodically (every 90 days)
- Audit: Monitor CloudTrail for S3 API calls

### 10.3 Data Integrity
- ETag verification ensures upload integrity
- Consider enabling S3 Object Lock for compliance (immutable storage)

## 11. Documentation Updates Needed

### 11.1 User-Facing
- Update README with S3 backup configuration
- Document bucket naming convention
- Provide setup guide for new projects (bucket creation)

### 11.2 Developer-Facing
- Document `s3_utils.py` module
- Update architecture diagram with S3 backup flow
- Add troubleshooting guide for common S3 errors

### 11.3 Operations
- Runbook for S3 backup failures
- Monitoring dashboard setup
- Cost estimation per project

---

## Summary

This design provides a minimal, robust S3 backup implementation that:
- ✅ Branches after local backup (non-disruptive)
- ✅ Non-blocking failures (processing continues)
- ✅ ETag verification (data integrity)
- ✅ Configurable via YAML (easy rollout/rollback)
- ✅ Handles multi-file `.d` folders
- ✅ Follows existing patterns (status tracking, error handling)
- ✅ Extensible for future enhancements

**Key files to create/modify**:
1. `shared/db/models.py` - Add S3 fields
2. `shared/s3_utils.py` - New module for S3 operations
3. `dags/impl/handler_impl.py` - Add `backup_raw_file_to_s3()`, modify `copy_raw_file()`
4. `dags/acquisition_handler.py` - Add `upload_to_s3_` task to chain
5. `shared/yamlsettings.py` - Add config keys for S3
6. `envs/alphakraken.*.yaml` - Add `backup` section
7. `airflow_src/requirements_airflow.txt` - Add boto3
8. Tests for all new functionality

**Estimated effort**: 3-5 days for core implementation + 2-3 days for testing and documentation.
