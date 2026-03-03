# S3 Verification Mode for file_remover

## Context

The file_remover deletes raw files from instrument backup folders after verifying them against a reference backup. Currently, verification is only against the local pool backup. Since S3 backup is already implemented (s3_uploader), we need the file_remover to also support verifying files against S3 before deletion. This is needed for setups where local pool backup may not be available or S3 is the primary backup.

## Approach

Implement `S3FileIdentifier` as a subclass of `FileIdentifier` (following the commented-out skeleton in `file_checks.py`). Use a factory function to select the right identifier based on config and file state. Thread the S3 client through the remover call chain.

## Changes

### 1. `airflow_src/plugins/s3/s3_utils.py` — Add `parse_s3_upload_path()`

Parse `raw_file.s3_upload_path` (e.g. `s3://bucket-name/prefix/`) into `(bucket_name, key_prefix)`. Add `S3_PROTOCOL_PREFIX = "s3://"` constant.

### 2. `airflow_src/plugins/s3/client.py` — Extend `get_etag()` to also return `ContentLength`

Change `get_etag()` to return `tuple[str | object, int]`: on success, return `(etag, content_length)` from the existing `head_object` call. On 404, return (`S3_FILE_NOT_FOUND_ETAG`, -1). Update callers (`is_upload_needed` in `s3_utils.py`, `_upload_files` in `s3_uploader_impl.py`) to unpack the tuple.

### 3. `airflow_src/plugins/file_checks.py` — Implement `S3FileIdentifier` + factory

**`S3FileIdentifier(FileIdentifier)`:**
- `__init__(raw_file, s3_client)` — calls `super().__init__()`, parses `s3_upload_path` for bucket/prefix, initializes `_cached_head_responses` dict for HEAD caching
- `_get_hashes(rel_file_path)` — calls `super()._get_hashes()` for `(size, md5)`, then extracts ETag from `file_info[2]` (split on `ETAG_SEPARATOR`). Raises `KeyError` if file_info has < 3 elements. Returns `(size, md5, etag)`.
- `check_file(...)` — overrides parent to unpack 3 values from `_get_hashes()`. Passes `md5` to `_check_against_db()` and `etag` to `_check_against_reference()` (via `hash_in_db` parameter).
- `_check_reference_exists(rel_file_path)` — returns `False` if `s3_upload_path` is not set OR `backup_status != UPLOAD_DONE` (graceful skip). Otherwise, HEAD request to S3 via `get_etag()`, caches response `(etag, content_length)`. On non-404 S3 error (`ValueError`): catches, logs warning, returns `False` (file skipped, retried next run).
- `_check_against_reference(rel_file_path, size_in_db, hash_in_db, hash_check)` — `hash_in_db` receives the ETag (routed from overridden `check_file`). Compares cached S3 size with `int(size_in_db)` and cached S3 ETag with `hash_in_db`. Ignores `hash_check` (ETag check is cheap).
- `_check_against_db()` stays inherited — still compares local instrument file against DB (unchanged)

**`create_file_identifier(raw_file, s3_client=None)` factory:**
- If `s3_client` is `None` (local mode): returns `FileIdentifier(raw_file)`
- If `s3_client` provided (S3 mode): returns `S3FileIdentifier(raw_file, s3_client)`

### 4. `airflow_src/dags/impl/remover_impl.py` — Use factory, thread S3 client

- Add `_create_s3_client_if_needed()` — returns S3 client if `is_s3_upload_enabled()`, else `None`
- `get_raw_files_to_remove()` — create S3 client once, pass to `_decide_on_raw_files_to_remove()`
- `_decide_on_raw_files_to_remove()` — accept `s3_client` param, pass to `_get_total_size()`
- `_get_total_size()` — accept `s3_client` param, use `create_file_identifier(raw_file, s3_client)` instead of `FileIdentifier(raw_file)`
- `remove_raw_files()` — create S3 client once, pass to `_safe_remove_files()`
- `_safe_remove_files()` — accept `s3_client` param, use `create_file_identifier(raw_file, s3_client)` instead of `FileIdentifier(raw_file)`

### 5. Tests

| File | Tests |
|------|-------|
| `tests/test_file_checks.py` | S3FileIdentifier: success, not found on S3, size mismatch, ETag mismatch, no ETag in file_info, backup_status not UPLOAD_DONE, s3_upload_path not set, non-404 S3 error (skipped gracefully). Factory: returns S3FileIdentifier when s3_client provided, FileIdentifier when None. |
| `tests/s3/test_client.py` | Update existing `get_etag` tests for `(etag, content_length)` return type. |
| `tests/s3/test_s3_utils.py` | `parse_s3_upload_path`: bucket-only, with prefix |
| `tests/dags/impl/test_remover_impl.py` | Update existing mocks from `FileIdentifier` to `create_file_identifier`. Add S3 client threading tests. |

## Key design decisions

- **ETag access pattern**: `S3FileIdentifier` overrides `_get_hashes()` to return `(size, md5, etag)` and overrides `check_file()` to route `md5` to `_check_against_db()` and `etag` to `_check_against_reference()`. This keeps the interface clean — each method receives the hash type it needs.
- **ETag comparison**: S3 ETag is compared against stored ETag in `file_info[2]` (not the MD5 hash at index [1]). Split on `ETAG_SEPARATOR` (`"__"`) to strip chunk size suffix.
- **Older file_info entries** without ETag (only 2 elements): `_get_hashes()` raises `KeyError`, file is skipped (not removed).
- **Gating**: `_check_reference_exists()` checks both `backup_status == UPLOAD_DONE` and `s3_upload_path is not None`. This prevents deletion when a previous upload attempt set `s3_upload_path` but a retry failed.
- **No fallback**: In S3 mode, files not yet uploaded to S3 are skipped (not removed). `_check_reference_exists()` returns `False`, so `check_file()` returns `False` and callers skip the file via existing error handling.
- **S3 error handling**: Non-404 S3 errors in `_check_reference_exists` are caught and logged as warning. File is skipped (safe default), retried next run. This prevents transient S3 issues from blocking cleanup of other files.
- **Single HEAD request**: `_check_reference_exists` caches the HEAD response for reuse in `_check_against_reference`.

## Verification

1. Run existing tests: `pytest airflow_src/tests/test_file_checks.py airflow_src/tests/dags/impl/test_remover_impl.py`
2. Run new S3 tests: `pytest airflow_src/tests/s3/`
3. Run full test suite: `pytest airflow_src/tests/`
