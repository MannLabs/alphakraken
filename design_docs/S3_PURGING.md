# S3 Verification Mode for file_remover

## Context

The file_remover deletes raw files from instrument backup folders after verifying them against a reference backup. Currently, verification is only against the local pool backup. Since S3 backup is already implemented (s3_uploader), we need the file_remover to also support verifying files against S3 before deletion. This is needed for setups where local pool backup may not be available or S3 is the primary backup.

## Approach

Implement `S3FileIdentifier` as a subclass of `FileIdentifier` (following the commented-out skeleton in `file_checks.py`). Use a factory function to select the right identifier based on config and file state. Thread the S3 client through the remover call chain.

## Changes

### 1. `airflow_src/plugins/s3/s3_utils.py` — Add `parse_s3_upload_path()`

Parse `raw_file.s3_upload_path` (e.g. `s3://bucket-name/prefix/`) into `(bucket_name, key_prefix)`. Add `S3_PROTOCOL_PREFIX = "s3://"` constant.

### 2. `airflow_src/plugins/s3/client.py` — Extend `get_etag()` to also return `ContentLength`

Change `get_etag()` to return `tuple[str | object, int] `: on success, return `(etag, content_length)` from the existing `head_object` call. On 404, return (`S3_FILE_NOT_FOUND_ETAG`, -1). Update callers (`is_upload_needed` in `s3_utils.py`, `_upload_all_files_to_s3` in `s3_uploader_impl.py`) to unpack the tuple.

### 3. `airflow_src/plugins/file_checks.py` — Implement `S3FileIdentifier` + factory

**`S3FileIdentifier(FileIdentifier)`:**
- `__init__(raw_file, s3_client)` — calls `super().__init__()`, parses `s3_upload_path` for bucket/prefix
- `_check_reference_exists(rel_file_path)` — if `s3_upload_path` is not set, returns `False` (graceful skip). Otherwise, HEAD request to S3 via `get_etag()`, caches response (size + ETag)
- `_check_against_reference(rel_file_path, size_in_db, hash_in_db, hash_check)` — compares cached S3 size with `size_in_db`, and always compares S3 ETag with stored ETag from `file_info[2]` (ignores `hash_check`, as the ETag check is cheap in the S3 case)
- `_check_against_db()` stays inherited — still compares local instrument file against DB (unchanged)
- `_get_hashes()` stays inherited — still reads `(size, md5)` from DB

**`create_file_identifier(raw_file, s3_client=None)` factory:**
- If `s3_client` is `None` (local mode): returns `FileIdentifier(raw_file)`
- If `s3_client` provided (S3 mode) returns `S3FileIdentifier`

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
| `tests/test_file_checks.py` | S3FileIdentifier: success, not found on S3, size mismatch, ETag mismatch, no ETag in file_info, backup_status not UPLOAD_DONE, s3_upload_path not set. Factory: returns S3FileIdentifier when s3_client provided, FileIdentifier when None. |
| `tests/s3/test_client.py` | Update existing `get_etag` tests for `(etag, content_length)` return type. |
| `tests/s3/test_s3_utils.py` | `parse_s3_upload_path`: bucket-only, with prefix |
| `tests/dags/impl/test_remover_impl.py` | Update existing mocks from `FileIdentifier` to `create_file_identifier`. Add S3 client threading tests. |

## Key design decisions

- **ETag comparison**: S3 ETag is compared against stored ETag in `file_info[2]` (not the MD5 hash at index [1]). Split on `ETAG_SEPARATOR` (`"__"`) to strip chunk size suffix.
- **Older file_info entries** without ETag (only 2 elements): file is skipped (not removed). S3FileIdentifier's `_check_against_reference` returns `False` with a warning log if any file in the raw file lacks a stored ETag.
- **No fallback**: In S3 mode, files not yet uploaded to S3 (`s3_upload_path` not set) are skipped (not removed). `_check_reference_exists()` returns `False`, so `check_file()` returns `False` and callers skip the file via existing error handling.
- **Single HEAD request**: `_check_reference_exists` caches the HEAD response for reuse in `_check_against_reference`.

## Verification

1. Run existing tests: `pytest airflow_src/tests/test_file_checks.py airflow_src/tests/dags/impl/test_remover_impl.py`
2. Run new S3 tests: `pytest airflow_src/tests/s3/`
3. Run full test suite: `pytest airflow_src/tests/`
