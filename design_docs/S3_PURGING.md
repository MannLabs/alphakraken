# S3 Verification Mode for file_remover

## Context

The file_remover deletes raw files from instrument backup folders after verifying them against a reference backup. Currently, verification is only against the local pool backup. Since S3 backup is already implemented (s3_uploader), we need the file_remover to also support verifying files against S3 before deletion. This is needed for setups where local pool backup may not be available or S3 is the primary backup.

## Approach

Implement `S3FileIdentifier` as a subclass of `FileIdentifier` (following the commented-out skeleton in `file_checks.py`). Use a factory function to select the right identifier based on config and file state. Thread the S3 client through the remover call chain.

## Changes

### 1. `airflow_src/plugins/s3/s3_utils.py` — Add `parse_s3_upload_path()`

Parse `raw_file.s3_upload_path` (e.g. `s3://bucket-name/prefix/`) into `(bucket_name, key_prefix)`. Add `S3_PROTOCOL_PREFIX = "s3://"` constant.

### 2. `airflow_src/plugins/s3/client.py` — Add `head_object()` wrapper

Returns `{"ContentLength": int, "ETag": str}` or `None` (if 404). This avoids the double HEAD request problem — one call gets both size and ETag for caching.

### 3. `airflow_src/plugins/file_checks.py` — Implement `S3FileIdentifier` + factory

**`S3FileIdentifier(FileIdentifier)`:**
- `__init__(raw_file, s3_client)` — calls `super().__init__()`, parses `s3_upload_path` for bucket/prefix
- `_check_reference_exists(rel_file_path)` — HEAD request to S3, caches response (size + ETag)
- `_check_against_reference(rel_file_path, size_in_db, hash_in_db, hash_check)` — compares cached S3 size with `size_in_db`, and if `hash_check=True`, compares S3 ETag with stored ETag from `file_info[2]`
- `_check_against_db()` stays inherited — still compares local instrument file against DB (unchanged)
- `_get_hashes()` stays inherited — still reads `(size, md5)` from DB

**`create_file_identifier(raw_file, s3_client=None)` factory:**
- If `s3_client` is `None` (local mode): returns `FileIdentifier(raw_file)`
- If `s3_client` provided (S3 mode) AND `backup_status == UPLOAD_DONE` AND `s3_upload_path` is set: returns `S3FileIdentifier`
- If `s3_client` provided but file not on S3: returns `None` (file should be skipped, not removed)

### 4. `airflow_src/dags/impl/remover_impl.py` — Use factory, thread S3 client

- Add `_create_s3_client_if_needed()` — returns S3 client if `is_s3_upload_enabled()`, else `None`
- `get_raw_files_to_remove()` — create S3 client once, pass to `_decide_on_raw_files_to_remove()`
- `_decide_on_raw_files_to_remove()` — accept `s3_client` param, pass to `_get_total_size()`
- `_get_total_size()` — accept `s3_client` param, use `create_file_identifier(raw_file, s3_client)`. If factory returns `None`, raise `FileRemovalError` (file gets skipped by caller)
- `remove_raw_files()` — create S3 client once, pass to `_safe_remove_files()`
- `_safe_remove_files()` — accept `s3_client` param, use `create_file_identifier(raw_file, s3_client)`. If factory returns `None`, skip file (log warning, don't remove, don't mark as PURGED)

### 5. Tests

| File | Tests |
|------|-------|
| `tests/test_file_checks.py` | S3FileIdentifier: success, not found on S3, size mismatch, ETag mismatch, no ETag in file_info, hash_check=False. Factory: returns S3 when conditions met, falls back in 3 cases. |
| `tests/s3/test_client.py` | `head_object`: success, 404, non-404 error |
| `tests/s3/test_s3_utils.py` | `parse_s3_upload_path`: bucket-only, with prefix |
| `tests/dags/impl/test_remover_impl.py` | Update existing mocks from `FileIdentifier` to `create_file_identifier`. Add S3 client threading tests. |

## Key design decisions

- **ETag comparison**: S3 ETag is compared against stored ETag in `file_info[2]` (not the MD5 hash at index [1]). Split on `ETAG_SEPARATOR` (`"__"`) to strip chunk size suffix.
- **Older file_info entries** without ETag (only 2 elements): file is skipped (not removed). S3FileIdentifier's `_check_against_reference` returns `False` with a warning log if any file in the raw file lacks a stored ETag.
- **No fallback**: In S3 mode, files not yet uploaded to S3 (`backup_status != UPLOAD_DONE`) are skipped (not removed). The factory returns `None` and callers skip the file.
- **Single HEAD request**: `_check_reference_exists` caches the HEAD response for reuse in `_check_against_reference`.

## Verification

1. Run existing tests: `pytest airflow_src/tests/test_file_checks.py airflow_src/tests/dags/impl/test_remover_impl.py`
2. Run new S3 tests: `pytest airflow_src/tests/s3/`
3. Run full test suite: `pytest airflow_src/tests/`
