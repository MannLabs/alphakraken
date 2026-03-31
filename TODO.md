# WP1: Dropped Functionality

## Removed (intentionally, per design doc)

- **`ACTIVATE_MSQC` flag and all MSQC DAG wiring** — MSQC tasks (`run_msqc`, `monitor_msqc`, `compute_msqc_metrics`, `upload_msqc_metrics`) no longer exist in the DAG. MSQC is non-functional until WP2 re-adds it as a regular software type.
- **MSQC Slurm param hack** (`processor_impl.py:327-333`) — The block that overrode `SLURM_CPUS_PER_TASK=2`, `SLURM_MEM=31G`, `SLURM_TIME=00:10:00`, `NUM_THREADS=2` when `job_script_name == "submit_msqc_job.sh"` was removed. WP4 will make Slurm params configurable per settings.
- **Sciex MSQC exclusion hack** — The `instrument_type != InstrumentTypes.SCIEX` guard that prevented MSQC from running on Sciex instruments is gone (was part of the MSQC wiring). WP6 scope resolution will handle vendor-specific settings.
- **`upload_metrics` setting raw file status to DONE** — Status is now set by `finalize_raw_file_status` after all pipeline branches complete.
- **`output_path_check=not do_msqc` on `run_quanting`** — Was `True` when MSQC was off (which it always was in production). Now uses the default (`True`).

## Verification needed before deploying

- **XCom scoping in mapped task groups**: The `push_quanting_env` bridge task pushes `QUANTING_ENV` and `RAW_FILE_ID` to XCom. Downstream operators pull via `ti.xcom_pull(key=...)` without explicit `task_ids`. This relies on Airflow scoping XCom by `map_index` inside mapped task groups. Needs integration testing.
