# A) Preparatory code changes — land now, still on Airflow 2.11

**Target:** `apache-airflow==3.3.1` (latest at time of writing).
**Current:** `apache-airflow==2.11.0` (`airflow_src/requirements_airflow.txt:2`), Python 3.11.
**Scope of this doc:** changes that can be merged and run on 2.11 **today**, shrinking the migration PR to a mechanical, reversible flip.

Base commit for all line references: `609a06bb`.

---

## 0. What was actually measured

Not asserted from docs — run against a real `apache-airflow==3.3.1` install with this repo's `dags/` + `plugins/` mounted as `AIRFLOW_HOME`:

| Check | Result |
|---|---|
| `airflow dags reserialize` on all 7 DAGs | **0 import errors** (after installing `pyarrow`/`docker`, which the prod image already has) |
| `ruff check --preview --select AIR30,AIR301,AIR302` (removed-in-3) | **0 findings** |
| `ruff check --preview --select AIR31,AIR311,AIR312` (deprecated) | **42 findings**, all import-path renames (14 provider moves + 28 Task-SDK moves) |
| `airflow config lint` on the compose env block | 5 config keys moved (see doc B) |

**Conclusion: nothing in this repo breaks at DAG *parse* time.** Every blocker is at *runtime*, in code paths Ruff does not inspect. The work below is therefore mostly about (1) clearing the deprecations Ruff *does* see, and (2) putting seams around the four places that genuinely break.

Refs:
- [Ruff Airflow (AIR) rules](https://docs.astral.sh/ruff/rules/#airflow-air)
- [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)

---

## 1. Move operator imports to the standard provider — **fully 2.11-compatible**

`apache-airflow-providers-standard` declares `apache-airflow>=2.11.0`, so this lands now with no behaviour change.

Add to `airflow_src/requirements_airflow.txt`:

```
apache-airflow-providers-standard==1.18.0
```

Then rewrite the import in each of the 5 DAG files (14 usages in total):

```python
# before
from airflow.operators.python import PythonOperator, ShortCircuitOperator
# after
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
```

Affected: `dags/acquisition_handler.py:9`, `dags/file_mover.py:9`, `dags/file_remover.py:9`, `dags/instrument_watcher.py:9`, `dags/s3_uploader.py:9`.

This clears 14 of the 42 Ruff findings (all the `suggested-to-move-to-provider` ones).

Ref: [Standard provider](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/index.html)

---

## 2. What can **not** move yet

`airflow.sdk` (the Task SDK) is distributed as `apache-airflow-task-sdk`, which pins `apache-airflow-core>=3.3.0,<3.4.0`. It does not exist on 2.11.

So the remaining 28 Ruff findings **must wait for doc B** and should be left alone now:

| Current (works on 2.11 and 3.3) | Airflow 3 target |
|---|---|
| `airflow.models.dag.DAG` | `airflow.sdk.DAG` |
| `airflow.models.Param` | `airflow.sdk.Param` |
| `airflow.decorators.task` / `task_group` | `airflow.sdk.task` / `task_group` |
| `airflow.sensors.base.BaseSensorOperator` | `airflow.sdk.BaseSensorOperator` |
| `airflow.models.Variable` | `airflow.sdk.Variable` |
| `airflow.exceptions.AirflowFailException` | `airflow.sdk.exceptions.AirflowFailException` |
| `airflow.utils.trigger_rule.TriggerRule` | `airflow.task.trigger_rule.TriggerRule` |
| `airflow.utils.xcom.XCOM_RETURN_KEY` | `airflow.models.xcom.XCOM_RETURN_KEY` |

All of these still import fine on 3.3.1 (verified) — they only emit `DeprecatedImportWarning`. **They are not upgrade blockers**, which is why they are safe to defer.

Ref: [Task SDK](https://airflow.apache.org/docs/task-sdk/stable/index.html)

---

## 3. The four real blockers — build the seams now

Ruff catches none of these. Three of the four already sit behind a single function, which is good news: the fix in doc B is a body swap, not a sweep.

### 3.1 `trigger_dag_run()` — ORM write from task code 🔴

`plugins/common/utils.py:105-128`. Calls `airflow.api.common.trigger_dag.trigger_dag`, which is `@provide_session`-decorated and writes `DagModel`/`DagRun` directly. In Airflow 3 worker code this raises:

```
RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0
```

Its signature also changed (verified on 3.3.1): `execution_date` → `logical_date`, new required kwarg `triggered_by`, new `run_after`. Likewise `DagRun.generate_run_id` is now keyword-only with a required `run_after`.

This is the **spine of the pipeline** — 4 call sites chain every DAG to the next: `handler_impl.py:372` (file mover, delayed), `:383` (s3 uploader), `:485` (acquisition processor), and `watcher_impl.py:331` (acquisition handler, N runs in a transaction).

**Action now:** none beyond a marker comment — the seam holds. Verified: no direct `trigger_dag()` or `DagRun.generate_run_id()` call exists outside `common/utils.py`. **Keep it that way** — a direct call added elsewhere becomes a separate migration site.

### 3.2 `finalize_raw_file_status()` — ORM read from task code 🔴

`dags/impl/processor_impl.py:579-598`:

```python
dag_run = ti.get_dagrun()          # ← does not exist on RuntimeTaskInstance in AF3
all_tis = dag_run.get_task_instances()
```

Verified: `RuntimeTaskInstance` in 3.3.1 has **no** `get_dagrun`. This is the one blocker with no existing seam.

**Action (done):** the state collection is extracted into `_get_branch_states`
(`processor_impl.py:621`) so doc B replaces one function body instead of restructuring the routine:

```python
def _get_branch_states(ti: TaskInstance) -> dict[int, dict[str, str | None]]:
    """Return the state of every processing-branch task, keyed by map index and task id.

    Non-mapped tasks (map_index=-1) are excluded.
    """
    branch_states: dict[int, dict[str, str | None]] = defaultdict(dict)
    for ti_ in ti.get_dagrun().get_task_instances():
        if ti_.task_id.startswith(_TASK_GROUP_PREFIX) and ti_.map_index >= 0:
            branch_states[ti_.map_index][ti_.task_id] = ti_.state
    return branch_states
```

`_extract_errors` now takes `dict[int, dict[str, str | None]]` instead of
`dict[int, list[TaskInstance]]`, and the `t.state == TaskInstanceState.FAILED` check became a
dict-value check. `state` is typed `str | None` because `TaskInstance.state` is nullable.

Behaviour is identical, including the order of `failed_task_names`: it was list-append order from
`get_task_instances()` and is now dict insertion order over the same iteration. Verified — 459 passed
on 2.11, and no new failures on 3.3.1.

### 3.3 `_get_cluster_ssh_connections()` — ORM query on `Connection` 🔴

`plugins/common/utils.py:159-180`. `@provide_session` + `session.query(Connection).filter(Connection.conn_id.startswith(...))`.

The Task Execution API can fetch a connection **by id** (`GetConnection`) but has **no list/scan operation** — verified against `airflow/sdk/execution_time/comms.py`. So this cannot be expressed with the SDK at all and must go to the REST API in doc B.

**Action now:** none — single seam, one call site (`get_cluster_ssh_hook`, `utils.py:200`).

### 3.4 `get_airflow_variable()` — silent kwarg rename 🟡

`plugins/common/utils.py:73-87` uses `airflow.models.Variable.get(key, default_var=...)`. The Airflow 3 replacement `airflow.sdk.Variable.get` renames `default_var` → `default` (verified). `airflow.models.Variable` is the ORM model and will hit the ORM guard from task code.

**Action now:** none — single seam, 9 call sites all route through it. Flag it in doc B so the kwarg rename is not missed; it would otherwise fail only at runtime.

### 3.5 `get_xcom()` without `task_ids` — semantics reverse 🔴

The single worst finding in this repo, and Ruff does not see it.

`xcom_pull(task_ids=None)` means the **opposite** thing in the two versions:

| Version | Docstring | Effect |
|---|---|---|
| 2.11 | "Only XComs from tasks with matching ids will be pulled. **Pass `None` to remove the filter.**" | pull from **any** task in the run |
| 3.3.1 | "**If `None` (default), the task_id of the calling task is used.**" | pull from **the calling task only** |

Every cross-task pull that omits `task_ids` therefore stops finding its value. **11 of the 14
`get_xcom()` call sites omit it, and all 11 are genuine cross-task pulls.** Only the two inside
`_extract_errors` pass `task_ids`.

| Pull site | Key | Pushed by | Airflow 3 outcome |
|---|---|---|---|
| `handler_impl.py:78` `compute_checksum` | `ACQUISITION_MONITOR_ERRORS` | `AcquisitionMonitor.post_execute` | 🔥 **silent** — defaults to `[]` |
| `handler_impl.py:427` `decide_processing` | `ACQUISITION_MONITOR_ERRORS` | `AcquisitionMonitor.post_execute` | 🔥 **silent** — defaults to `[]` |
| `handler_impl.py:227` `copy_raw_file` | `FILES_DST_PATHS` | `compute_checksum` | `KeyError` |
| `handler_impl.py:230` `copy_raw_file` | `FILES_SIZE_AND_HASHSUM` | `compute_checksum` | `KeyError` |
| `handler_impl.py:387` `start_s3_uploader` | `TARGET_FOLDER_PATH` | `compute_checksum` | `KeyError` |
| `mover_impl.py:55` `move_files` | `FILES_TO_MOVE` | `get_files_to_move` | `KeyError` |
| `mover_impl.py:171` `_check_main_file_to_move` | `MAIN_FILE_TO_MOVE` | `get_files_to_move` | `KeyError` |
| `remover_impl.py:417` `remove_raw_files` | `FILES_TO_REMOVE` | `get_raw_files_to_remove` | `KeyError` |
| `remover_impl.py:451` `remove_raw_files` | `INSTRUMENTS_WITH_ERRORS` | `get_raw_files_to_remove` | `KeyError` |
| `watcher_impl.py:198` `decide_raw_file_handling` | `RAW_FILE_NAMES_TO_PROCESS` | `get_unknown_raw_files` | `KeyError` |
| `watcher_impl.py:290` `start_acquisition_handler` | `RAW_FILE_NAMES_WITH_DECISIONS` | `decide_raw_file_handling` | `KeyError` |

Nine fail loudly with `KeyError` (`get_xcom` raises when the value is `None` and no default was
given) — unpleasant but obvious. **The two `ACQUISITION_MONITOR_ERRORS` pulls are the dangerous
ones**: they pass a `[]` default, so they degrade silently into "no acquisition errors", and they
gate exactly the corruption-detection logic —

- `compute_checksum` skips the copy when `FILE_GOT_RENAMED` → would **copy a corrupted file**;
- `decide_processing` sets `ACQUISITION_FAILED` when `MAIN_FILE_MISSING` → would **process a failed
  acquisition as if healthy**.

**Action now (2.11-compatible):** pass `task_ids` explicitly at all 11 sites. Each key has exactly
one pusher, so narrowing the filter is behaviour-preserving on 2.11 and correct on 3.x. The task-id
constants already exist in `common.keys.Tasks`.

Consider making `task_ids` a **required** argument of `get_xcom()` so the ambiguous form cannot be
reintroduced — the wrapper at `utils.py:37` is the only entry point.

---

## 4. Small 2.11-safe cleanups

### 4.1 `DagBag(include_examples=...)` removed in Airflow 3

`tests/dags/test_dags.py:33`. Verified: `include_examples` is **not** in the Airflow 3.3.1 `DagBag.__init__` signature.

Replace the kwarg with the env var, which behaves the same on both versions:

```python
with patch.dict("os.environ",
                AIRFLOW_CONN_CLUSTER_SSH_CONNECTION=...,
                ENV_NAME="_test_",
                AIRFLOW__CORE__LOAD_EXAMPLES="False"):
    return DagBag(dag_folder=DAG_FOLDER)
```

### 4.2 CI uses a removed CLI command

`.github/workflows/branch-checks.yaml:48` runs `airflow db init`. Airflow 3 exposes only `migrate | reset | check | check-migrations | clean` (verified — `db init` is gone).

`airflow db migrate` exists on 2.7+, so switch now:

```yaml
airflow db migrate
```

### 4.3 Pin the `ti` type behind an alias

24 signatures across `dags/impl/*.py` annotate `ti: TaskInstance` (`airflow.models.TaskInstance`). At runtime under Airflow 3 the object is a `RuntimeTaskInstance`, not the ORM model — the annotations are wrong but harmless (the modules use `from __future__ import annotations`, so they are never evaluated).

Introduce one alias in `plugins/common/utils.py` and use it everywhere, so doc B repoints 24 annotations by editing one line:

```python
from airflow.models import TaskInstance
TaskInstanceLike = TaskInstance  # AF3: -> airflow.sdk.execution_time.task_runner.RuntimeTaskInstance
```

Low priority — cosmetic until someone trusts the annotation.

---

## 5. Things that are fine — explicitly checked, no action

Worth recording so nobody re-litigates them during the migration:

| Pattern | Where | Verdict |
|---|---|---|
| `schedule="@continuous"` | `instrument_watcher.py:31` | **Still supported.** `ContinuousTimetable` is present in 3.3.1 (`airflow/timetables/simple.py`) |
| Bare imports (`from common.utils import ...`, `from callbacks import ...`) | all DAGs | **Still work.** `settings.prepare_syspath_for_config_and_plugins()` still appends `PLUGINS_FOLDER` to `sys.path` in 3.3.1; confirmed by the clean parse. The Astro "use `dags.common`" advice does **not** apply to this non-Astro deployment |
| `pre_execute` / `post_execute` on custom sensors | `sensors/*.py` | Still on `airflow.sdk.BaseSensorOperator` |
| `max_active_tis_per_dag`, `weight_rule="upstream"`, `priority_weight`, `pool`, `retry_exponential_backoff`, `execution_timeout`, `queue` | all DAGs | All still valid `BaseOperator` params |
| `TriggerRule.ALL_DONE` | `acquisition_processor.py:157` | Still valid (`dummy` / `none_failed_or_skipped` were the removed ones) |
| Context keys `params`, `ti`, `task_instance`, `exception` | `callbacks.py`, sensors | All present in the AF3 `Context` |
| `xcom_pull(key=..., task_ids=..., map_indexes=..., default=...)` | `utils.py:60` | Signature preserved, **but the `task_ids=None` semantics reverse** — see §3.5. An earlier revision of this doc wrongly claimed the repo was not exposed to this; it is, at 11 call sites |
| `execution_date`, `days_ago`, SubDAGs, SLAs, Datasets, `EmailOperator`, FAB plugins | — | **Not used anywhere.** No work |
| webapp / rest_api / mcp_server | — | **Zero Airflow imports.** Entirely unaffected |

---

## 6. Suggested PR split

| PR | Content | Risk | Status |
|---|---|---|---|
| A1 | §1 standard-provider imports + requirements pin | trivial | **done** (`2c534888`) |
| A2 | §4.1 + §4.2 test/CI fixes | trivial | **done** |
| A3 | §3.2 extract `_get_branch_states` | low — pure refactor, covered by `tests/dags/impl/test_processor_impl.py` | **done** |
| A4 | §4.3 type alias | cosmetic | open |
| — | §3.1 marker comment on `trigger_dag_run` | none | **done** |

Verification baseline after A1–A3: **459 passed on 2.11**; on 3.3.1 **457 passed, 2 failed**, the two
failures being `tests/common/test_utils.py::test_trigger_dag_run{,_with_delay}` — the §3.1 /
doc B §3.2 blocker, which the existing unit tests already catch:

```
airflow_src/plugins/common/utils.py:112: TypeError
DagRun.generate_run_id() got an unexpected keyword argument 'execution_date'
```

So doc B's code diff is now: one import sweep + three function bodies (`trigger_dag_run`,
`_get_branch_states`, `_get_cluster_ssh_connections`) + the `Variable.get` kwarg.

Caveat: unit tests mock `ti`, so they cannot catch the ORM guard in `_get_branch_states` or
`_get_cluster_ssh_connections` — those two still need a real worker (doc B §6).
