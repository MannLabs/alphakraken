# C) Airflow 3 features that could replace current patterns

Post-migration opportunities. **Nothing here is required for the upgrade** — doc B stands alone.
Ordered by value-per-effort. Base commit: `609a06bb`.

Honest framing up front: items 1–4 fix things that are *actually wrong or costly today*. Items 5–8 are genuine improvements but discretionary. Item 9 is listed so it can be explicitly declined rather than rediscovered later.

---

## 1. 🥇 `ResumableJobMixin` — stop resubmitting duplicate Slurm jobs

**New in:** 3.3 (AIP-103, built on the task state store)

**Problem today.** `submit_job_task` (`dags/acquisition_processor.py:95-98`) inherits `retries: 4` from the DAG's `default_args`. `submit_job()` submits to Slurm and returns a job id. If the task dies *after* Slurm accepted the job but *before* the task completes — worker crash, XCom push failure, `execution_timeout` — the retry runs `submit_job()` again and **submits a second Slurm job** for the same raw file. The existing `output_path_check` guard (`processor_impl.py:368`) catches the case where output already exists, but not a job that is still running.

**Fix.** `ResumableJobMixin` persists the external job id to the task state store *before* polling, and on retry reconnects instead of resubmitting.

The required interface is a near-exact match for the existing `JobHandler` ABC (`plugins/jobs/job_handler.py:77-85`), which already defines `submit_job`, `get_job_status`, and `get_job_result`:

```python
class SubmitQuantingJobOperator(ResumableJobMixin, BaseOperator):
    external_id_key = "slurm_job_id"

    def execute(self, context):
        return self.execute_resumable(context)

    def submit_job(self, context):
        return submit_job(quanting_env=...)

    def get_job_status(self, external_id, context):
        return get_job_status(external_id, engine=...)

    def is_job_active(self, status):
        return status in (JobStates.PENDING, JobStates.RUNNING, JobStates.COMPLETING)

    def is_job_succeeded(self, status):
        return status == JobStates.COMPLETED
```

**Bonus:** this collapses `submit_job_task` + `WaitForJobStartSensor` + `WaitForJobFinishSensor` into one operator, removing the XCom plumbing in `sensors/ssh_sensor.py:42-57` that currently reaches back into two upstream tasks by hard-coded task id.

**Caveat, from its own docstring:** it does **not** free the worker slot during polling. For that, see item 2 — the two are complementary, not alternatives.

Refs: [Airflow 3.3 release blog](https://airflow.apache.org/blog/airflow-3.3.0/) · [Task SDK API](https://airflow.apache.org/docs/task-sdk/stable/api.html)

---

## 2. 🥇 Deferrable job sensors — reclaim worker and pool slots

**New in:** deferrable operators exist in 2.x, but the triggerer is **currently disabled here** (`docker-compose.yaml:176-185`, commented out) and Airflow 3 improves the execution model.

**Problem today.** `WaitForJobStartSensor` and `WaitForJobFinishSensor` run in default `mode="poke"`, poking every 60 s (`Timings.JOB_MONITOR_POKE_INTERVAL_S`). For the entire duration of a cluster job — potentially hours — each one holds:

- a **Celery worker slot**, and
- a slot in `Pools.CLUSTER_SLOTS_POOL`.

The code already acknowledges the pain: `acquisition_processor.py:110-115` notes the pool coupling, and the whole `weight_rule: "upstream"` + `priority_weight` scheme exists to work around the resulting contention.

**Fix.** Make them deferrable so the wait moves to the triggerer, which handles thousands of concurrent waits in one async process. Enable the `airflow-triggerer` service (doc B §4.4).

⚠️ **Gotcha the migration skill calls out explicitly:** a trigger that calls hooks synchronously inside the asyncio event loop will block or fail. The SSH polling in `plugins/jobs/job_handler.py:128-131` is synchronous `ssh_execute`. Wrap it in `sync_to_async(...)` rather than calling it directly from the trigger.

⚠️ Also: triggers cannot live in the DAG bundle — they must be importable from elsewhere on `sys.path`. The `plugins/` folder qualifies (verified: still appended to `sys.path` in 3.3.1).

**Cheaper interim step, not an Airflow 3 feature:** setting `mode="reschedule"` on these sensors frees the worker slot today with a one-line change. It does not work for `AcquisitionMonitor` — see item 3.

Ref: [Deferrable operators](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)

---

## 3. 🥇 Task state store — fix `AcquisitionMonitor` losing its state on retry

**New in:** 3.3 (AIP-103). Available as `context["task_state_store"]`, with `.get(key, default)` / `.set(key, value, retention=...)` / `.delete(key)`.

**Problem today.** `AcquisitionMonitor` (`plugins/sensors/acquisition_monitor.py`) keeps its entire decision state in **instance attributes**:

```python
self._first_poke_timestamp
self._latest_file_size_check_timestamp
self._last_file_size
self._initial_dir_content
self._main_file_exists
```

These live only in the worker process. The task has `retries: 4` and `execution_timeout=180 min`. On timeout-and-retry, `pre_execute` runs again and **resets all of it** — so the "file size unchanged for `SIZE_CHECK_INTERVAL_M=60` minutes" clock restarts from zero, and `SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M=120` restarts too. An acquisition that should have been declared done can be delayed by a full extra cycle, silently.

This is also *why* the sensor can't use `mode="reschedule"` today: reschedule mode tears down the operator instance between pokes.

**Fix.** Persist the poke state:

```python
def poke(self, context):
    store = context["task_state_store"]
    last_size = store.get("last_file_size", -1)
    ...
    store.set("last_file_size", size)
```

State survives retries **and** reschedules — which then unlocks `mode="reschedule"` on the longest-running sensor in the system.

⚠️ JSON-serialisable values only. `_initial_dir_content` is a `set` — store as a sorted list. `datetime` must be stored via `.isoformat()`.

---

## 4. 🥈 `ExceptionRetryPolicy` — declarative retry rules

**New in:** 3.3 (AIP-105)

**Problem today.** Retry behaviour is expressed by *which exception type gets raised*, decided deep inside impl code:

- `retries: 4` blanket in every DAG's `default_args`
- `AirflowFailException` raised in ~8 places to mean "do not retry"
- a bespoke hierarchy — `QuantingFailedException`, `QuantingFailedKnownErrorException`, `QuantingFailedNewErrorException`, `QuantingFailedUnknownErrorException` — whose only job is to signal retry-worthiness and steer `on_failure_callback` (`plugins/callbacks.py:44-68`)
- `retries=0` on `start_acquisition_handler` (`instrument_watcher.py:79`) with a five-line comment explaining why

**Fix.** Move the policy to the DAG definition, where it is visible:

```python
from datetime import timedelta
from airflow.sdk import ExceptionRetryPolicy, RetryRule, RetryAction

retry_policy = ExceptionRetryPolicy(
    rules=[
        RetryRule(exception="paramiko.ssh_exception.SSHException",
                  action=RetryAction.RETRY, retry_delay=timedelta(minutes=5),
                  reason="transient cluster SSH failure"),
        RetryRule(exception="impl.processor_impl.QuantingFailedKnownErrorException",
                  action=RetryAction.FAIL,
                  reason="known business error - retrying will not help"),
    ],
)
```

**Honest assessment:** the current scheme *works*. The gain is legibility — retry policy stops being an emergent property of exception classes scattered across `impl/`. Worth doing opportunistically, not as a dedicated project. `AirflowFailException` keeps working either way.

---

## 5. 🥈 DAG versioning — mid-run deploys stop changing task behaviour

**New in:** 3.0. Zero code change; you get it by upgrading.

A DAG run now completes against the DAG version it **started** with, even if new code is deployed mid-run. The UI ties runs, task structure, code, and logs to that version.

Directly relevant here: DAG runs in this system are long — `AcquisitionMonitor` up to 3 h, cluster jobs longer — and deploys happen while they're in flight. Today a mid-run deploy silently changes the behaviour of not-yet-started tasks in a running DAG.

Practical follow-up: once versioning is live, `AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL: 300` (doc B §4.1) can be revisited — the reason for the long interval was partly to limit mid-run churn.

Ref: [Airflow 3 GA announcement](https://airflow.apache.org/blog/airflow-three-point-oh-is-here/)

---

## 6. 🥈 Deadline Alerts — "this acquisition is taking abnormally long"

**New in:** 3.1, extended since. `DeadlineAlert` is exported from `airflow.sdk`; `DAG(deadline=...)` is accepted (both verified on 3.3.1).

References available: `AVERAGE_RUNTIME`, `DAGRUN_LOGICAL_DATE`, `DAGRUN_QUEUED_AT`, `FIXED_DATETIME`, plus `register_custom_reference`.

**Why it fits.** The system currently detects "too slow" only via hard `execution_timeout` values (`common/settings.py:Timings`), which are absolute and hand-tuned per task. `DeadlineReference.AVERAGE_RUNTIME` is relative to observed history — it catches "this run is 3× slower than usual" without anyone picking a threshold.

There is already an alerting channel to route this to: `ops_alerts_webhook_url` in `alphakraken.<env>.yaml`.

This is the closest thing to the SLA feature that Airflow 3 removed — and since this repo never used SLAs, it's a net-new capability rather than a replacement.

---

## 7. 🥉 `TriggerDagRunOperator` + dynamic mapping — for 3 of the 4 trigger sites

Covered in doc B §3.2. Once `trigger_dag_run` moves to the REST API, three of the four call sites — `start_file_mover` (`handler_impl.py:372`, the delayed one), `start_s3_uploader` (`:383`), `start_acquisition_processor` (`:485`) — trigger exactly one run and could instead use `TriggerDagRunOperator`, which now supports `run_after`, `conf`, and `trigger_run_id` natively — **no API token needed**.

`watcher_impl.start_acquisition_handler` cannot: it triggers a variable number of runs inside a per-file DB transaction with rollback-on-failure (`watcher_impl.py:327-345`). Splitting that into a mapped `TriggerDagRunOperator.expand()` would break the atomicity the comments there deliberately protect. **Leave that one on the API call.**

Net effect: shrinks the blast radius of the API-token dependency introduced in doc B to a single task.

---

## 8. 🥉 `ObjectStoragePath` for the S3 uploader

`airflow.sdk.ObjectStoragePath` gives a `pathlib`-style interface over S3. `plugins/s3/client.py` currently builds a boto3 client via `AwsBaseHook`, and `s3_uploader_impl.py` drives it manually.

Modest simplification, and it would align the S3 path handling with the `Path`-based code everywhere else in `plugins/`. Low urgency — the existing code works and was recently written.

Ref: [Object storage](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html)

---

## 9. ⛔ Assets / event-driven scheduling — evaluated, recommend **not now**

The tempting version: replace the `@continuous` `instrument_watcher` + `FileCreationSensor` polling loop with an `AssetWatcher`, and replace the whole `trigger_dag_run` chain with asset-driven scheduling.

**Why it doesn't pay off here:**

1. **No suitable trigger exists.** Event-driven scheduling requires a trigger inheriting `BaseEventTrigger`. The shipped ones target message queues (`common.messaging` provider). The instruments here are **SMB/network mounts** (`InternalPaths.MOUNTS_PATH`), and there is no push notification — a custom `BaseEventTrigger` would still poll the filesystem. You'd move the polling from a worker to the triggerer without removing it.
2. **Assets don't carry the payload.** The current chain passes `conf={"raw_file_id": ...}` between DAGs. Asset-triggered runs identify the asset, not a per-file parameter; you'd end up re-deriving `raw_file_id` at the far end.
3. **The real cost is elsewhere.** Item 2 (deferrable sensors) captures most of the worker-slot saving for far less risk.

Item 2's `mode="reschedule"` / deferrable route gets the resource win. Revisit assets only if instrument-side event notification ever becomes available.

Ref: [Event-driven scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html)

---

## Also considered, no action

| Feature | Verdict |
|---|---|
| **Human-in-the-loop tasks** (3.1/3.3, `awaiting_input` state) | The manual-override flows (`AirflowVars.CHECKSUM_OVERWRITE_FILE_ID`, `BACKUP_OVERWRITE_FILE_ID`) currently require setting a Variable and re-running a task. HITL could make that a first-class approval step. Genuinely relevant but speculative — raise with the operators before building |
| **New backfill (UI/API-driven)** | Everything runs `catchup=False` and is externally triggered. No use case |
| **New React UI** | Free with the upgrade. Note `dag.doc_md = __doc__` (set in every DAG) still renders |
| **New mappers** (`FanOutMapper`, `ChainMapper`, …, 3.3) | `processing.expand(settings_id=...)` is a simple 1-D fan-out. Current API is adequate |
| **`airflow.sdk.conf`** | Config isn't read from DAG code here |
| **Language Task SDKs** (Java/Go, 3.3) | No use case — everything is Python |
| **Dag Results API** (3.3) | The pipeline is fire-and-forget into MongoDB; nothing blocks on a DAG's return value |

---

## Suggested sequencing

| Phase | Items | Rationale |
|---|---|---|
| Right after migration | 5 (versioning — free), 3 (state store) | 5 costs nothing; 3 fixes a live correctness bug |
| Next | 1 (`ResumableJobMixin`), 2 (deferrable + triggerer) | Together they fix duplicate job submission *and* the worker/pool contention. Do 1 first — it's lower risk and simplifies the operators that 2 then makes deferrable |
| Opportunistic | 4 (retry policy), 7 (`TriggerDagRunOperator`), 6 (deadline alerts) | Touch as the surrounding code is edited |
| Declined | 9 (assets), 8 (`ObjectStoragePath`) unless S3 code is reworked anyway | |
