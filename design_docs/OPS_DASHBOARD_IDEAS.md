# Operator dashboard: ideas

Status: ideas, not decided. Written 2026-10-22 at commit `5dbf3d91`.

Audience: operators (not users). Goal: show whether the pipeline is healthy and what needs action.

## Already covered by `webapp/pages_/status.py`
- Per instrument: last file creation, last status update, last health check
- How many files are in each status
- Oldest transition to each status
- Throughput (sample count and data volume per day)

## Proposal: new Streamlit page `ops`

### 1. Must have
1.1. **DAG runs per DAG**: for each DAG (`acquisition_handler.*`, `acquisition_processor.*`, `instrument_watcher.*`, `file_mover`, `file_remover`, `s3_uploader`, `cluster_job_poller`), show how many runs are queued, running and failed in the last N hours, linking to the filtered Airflow view. Source: Airflow REST API (`GET /api/v2/dags/~/dagRuns?state=...`).
  - Airflow 3 dropped the per-DAG state circles from Airflow 2. Upstream brings them back in **3.4.0**, which isn't released yet (milestone due 2026-10-26, in beta as of 2026-10-22):
    - Run-state counts: issue https://github.com/apache/airflow/issues/66946, PR https://github.com/apache/airflow/pull/67242
    - Task-state counts for the latest run: issue https://github.com/apache/airflow/issues/66947, PR https://github.com/apache/airflow/pull/69619
  - No ready-made plugin exists.
  - The Dag Runs page is reportedly capped at 50 runs, so it can't serve as a workaround for totals.

1.2. **Files that need action**: files in `error`, `quanting_failed` or `acquisition_failed`, with the error text, instrument, age and links to the Airflow log and file details. Source: `RawFile`.

1.3. **Stuck files**: files in an in-progress status (`copying`, `checksumming`, `quanting`, `queued_for_quanting`) longer than a per-status threshold. Lists every file over the threshold, not just the oldest one per status. Source: `RawFile`.

1.4. **Monitoring health**: which instance currently sends alerts (`monitoring/active_instance.py`), when it last ran, and the active suppressions (`AlertSuppression`).
  - Which alerts are firing right now appears to be held only in the monitoring process's memory (`monitoring/alert_manager.py`). If so, showing it needs that state written to MongoDB first. Not yet confirmed.

### 2. Should have
2.1. **Cluster jobs**: `SlurmJob` counts by `state` and `cluster_id`, the longest-running jobs, and jobs that `LEFT_QUEUE`.

2.2. **Backup / S3**: counts per `BackupStatus` (`upload_failed`, `copying_failed`, uploads in progress longer than a threshold), plus the backlog not yet uploaded.

2.3. **Storage**: free space per instrument and file system over time, from `KrakenStatus.free_space_gb`. Today this only shows up when an alert fires.
  - Showing it over time needs history, but `KrakenStatus` holds only the latest value.

### 3. Nice to have
3.1. Error-type breakdown over time, to spot recurring failure classes.

3.2. Time from acquisition end to `done`, per instrument, as a trend (catches slowdowns before files pile up).

## Open decisions
- D1. Webapp gets Airflow API access (an API user plus `requests`) for 1.1, or drop 1.1 and wait for Airflow 3.4.0?
- D2. Add alert-state persistence to the monitoring service for 1.4?
- D3. Restrict the page to operators (like `admin`), or make it visible to everyone?

## Notes
- Everything except 1.1 reads MongoDB, which the webapp already accesses.
