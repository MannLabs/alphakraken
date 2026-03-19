# WP1: Dropped Functionality

## Removed (intentionally, per design doc)

- **`ACTIVATE_MSQC` flag and all MSQC DAG wiring** — MSQC tasks (`run_msqc`, `monitor_msqc`, `compute_msqc_metrics`, `upload_msqc_metrics`) no longer exist in the DAG. Re-added as regular software type in WP2.
- **MSQC Slurm param hack** (`processor_impl.py:327-333`) — The block that overrode `SLURM_CPUS_PER_TASK=2`, `SLURM_MEM=31G`, `SLURM_TIME=00:10:00`, `NUM_THREADS=2` when `job_script_name == "submit_msqc_job.sh"` was removed. WP4 will make Slurm params configurable per settings.
- **Sciex MSQC exclusion hack** — The `instrument_type != InstrumentTypes.SCIEX` guard that prevented MSQC from running on Sciex instruments is gone (was part of the MSQC wiring). WP6 scope resolution will handle vendor-specific settings.
- **`upload_metrics` setting raw file status to DONE** — Status is now set by `finalize_raw_file_status` after all pipeline branches complete.
- **`output_path_check=not do_msqc` on `run_quanting`** — Was `True` when MSQC was off (which it always was in production). Now uses the default (`True`).

## Verification needed before deploying

- **XCom scoping in mapped task groups**: The `push_quanting_env` bridge task pushes `QUANTING_ENV` and `RAW_FILE_ID` to XCom. Downstream operators pull via `ti.xcom_pull(key=...)` without explicit `task_ids`. This relies on Airflow scoping XCom by `map_index` inside mapped task groups. Needs integration testing.

# WP2: MSQC Software Type

## Changes made

- **`SoftwareTypes.MSQC`** added to `shared/keys.py`
- **`fasta_file_name` / `speclib_file_name`** made optional in `Settings` model — MSQC and future Skyline don't need these
- **`create_settings()` validation** — alphadia/custom require both fasta and speclib; msqc does not
- **`compute_metrics()` mapping** — MSQC software type maps to MSQC metrics type
- **Removed MSQC `AirflowSkipException` hack** in `compute_metrics()` — MSQC branch failures now propagate normally
- **Removed `submit_msqc_job.sh` hack** in `run_quanting()` — SKIP_QUANTING now applies uniformly
- **Removed dead MSQC keys** from `common/keys.py` (`Tasks.RUN_MSQC`, `Tasks.MONITOR_MSQC`, `Tasks.COMPUTE_MSQC_METRICS`, `Tasks.UPLOAD_MSQC_METRICS`, `XComKeys.MSQC_JOB_ID`)
- **Webapp** — MSQC added to software type dropdown; fasta/speclib fields hidden for MSQC

## Known issues (deferred)

- **`finalize_raw_file_status` skipped-state bug**: When a job fails, `check_quanting_result` raises `AirflowSkipException`, marking `upload_metrics` as "skipped". But `finalize_raw_file_status` only checks for "failed"/"upstream_failed" — not "skipped". A failed branch may be treated as successful. Deferred per decision.
