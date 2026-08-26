# B) The migration: 2.11.0 → 3.3.1

Prerequisite: doc A (`AIRFLOW3_A_PREP_CHANGES.md`) merged.
Base commit for line references: `609a06bb`.

---

## 1. Version decision

**Go directly 2.11.0 → 3.3.1.** No intermediate hop.

- Airflow 3 requires ≥ 2.7 as the source version ([upgrade guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)); 2.11 is the designated bridge release and this repo is already on it.
- ⚠️ The migration skill recommends "2.11 → 3.0.11 → 3.1". **That advice is stale**: `3.0.11` was never released (3.0.x stops at 3.0.6), and 3.1 is now four minor versions behind. Staging through 3.0.x buys nothing and costs two extra DB migrations.
- Python **3.11 stays** — 3.3.1 supports 3.10–3.14 and `constraints-3.11.txt` exists for it. No interpreter bump needed.

Rollback is a **metadata-DB restore**, not a package downgrade — the schema migration is one-way. Plan accordingly (§7).

---

## 2. Dependency changes

`airflow_src/requirements_airflow.txt` — repoint every constraint URL to `constraints-3.3.1`:

```
apache-airflow==3.3.1 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.11.txt"
apache-airflow-providers-standard==1.18.0 --constraint "..."
apache-airflow-providers-ssh==6.0.1 --constraint "..."
apache-airflow-providers-celery==3.23.1 --constraint "..."
apache-airflow-providers-fab==3.8.0 --constraint "..."     # NEW - see §4.3
apache-airflow-providers-amazon==9.35.0 --constraint "..."
```

`airflow_src/Dockerfile:2` — `ARG AIRFLOW_VERSION=3.3.1`.

### 2.1 The two dependency bumps you flagged

Both confirmed against the real `constraints-3.3.1` file:

| Package | 2.11 constraint | 3.3.1 constraint | Assessment |
|---|---|---|---|
| `pandas` | 2.1.4 | **3.0.5** | major bump |
| `paramiko` (via ssh provider) | 3.x | **5.0.0** | major bump |
| `numpy` | 1.x | 2.5.1 | transitive with pandas 3 |
| `pyarrow` | ≥19 | 25.0.0 | fine |
| `celery` / `SQLAlchemy` / `pendulum` | — | 5.6.3 / 2.0.51 / 3.2.0 | fine |

**pandas** — you're right that the operations are trivial. All pandas use in the Airflow image is confined to `plugins/metrics/metrics/*.py`: `pd.read_csv`, `pd.read_parquet`, `pd.isna`, and column arithmetic inside `_calc()`. Nothing exotic.

One caveat worth a targeted check rather than a blanket "it's fine": **pandas 3.0 makes the dedicated string dtype the default** (previously `object`) and makes Copy-on-Write the only mode. The metrics `_calc()` methods read columns out of search-engine output and compare/aggregate them — if any of those columns are string-typed and get compared or coerced, behaviour can shift silently. `tests/metrics/` covers `alphadia`, `base`, `diann` but **not** `msqc` or `skyline`. Recommend: run the metrics tests under pandas 3 before the migration lands, and eyeball one real `msqc` and one `skyline` output.

Ref: [pandas 3.0 whatsnew](https://pandas.pydata.org/docs/whatsnew/v3.0.0.html)

**Useful scoping detail:** the webapp container pins its own `pandas==2.2.2` (`webapp/requirements_webapp.txt:3`) and has **zero Airflow imports**. The pandas 3 bump therefore does **not** reach the webapp in production. But CI installs webapp and Airflow deps into *one* env (`.github/workflows/branch-checks.yaml`, which already notes this shortcut) — after the bump, CI would test webapp code against pandas 3 while prod runs 2.2.2. Either split the CI envs or accept the divergence knowingly.

**paramiko 5.0** — used only through `SSHHook` (`plugins/common/utils.py:210`, `sensors/ssh_utils.py`). The provider absorbs the API change; the risk is behavioural (auth/algorithm negotiation against your cluster's SSH daemon), not compile-time. As you said, easy to catch — but catch it *deliberately*: run the `submit_job` → `WaitForJobStartSensor` → `WaitForJobFinishSensor` chain against the real cluster in staging before switching production.

Also update `misc/requirements_development.txt:8-10` — the comment pinning `pandas==2.1.4` "because the apache/airflow:2.11.0 image comes with that version" is now wrong.

---

## 3. Code changes

### 3.1 Import sweep (mechanical, 28 sites)

```bash
ruff check --preview --select AIR --fix --unsafe-fixes airflow_src
```

Then verify by hand — the unsafe fixes touch import blocks. Expected mapping:

| From | To |
|---|---|
| `airflow.models.dag.DAG` | `airflow.sdk.DAG` |
| `airflow.models.Param` | `airflow.sdk.Param` |
| `airflow.decorators.task`, `task_group` | `airflow.sdk.task`, `airflow.sdk.task_group` |
| `airflow.sensors.base.BaseSensorOperator` | `airflow.sdk.BaseSensorOperator` |
| `airflow.exceptions.AirflowFailException` / `AirflowSkipException` | `airflow.sdk.exceptions.*` |
| `airflow.utils.trigger_rule.TriggerRule` | `airflow.task.trigger_rule.TriggerRule` |
| `airflow.utils.xcom.XCOM_RETURN_KEY` | `airflow.models.xcom.XCOM_RETURN_KEY` |
| `airflow.models.Variable` | `airflow.sdk.Variable` |
| `airflow.models.TaskInstance` (annotations) | `airflow.sdk.execution_time.task_runner.RuntimeTaskInstance` |

Ref: [Task SDK API](https://airflow.apache.org/docs/task-sdk/stable/api.html)

### 3.2 `trigger_dag_run()` → REST API v2 🔴

`plugins/common/utils.py:105-128`. The current implementation writes the metadata DB through the ORM and will raise `RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0` on every worker.

Keep the signature; swap the body:

```python
import os, requests

_API_BASE = os.environ["AIRFLOW__API__BASE_URL"]
_API_TOKEN = os.environ["ALPHAKRAKEN_AIRFLOW_API_TOKEN"]

def trigger_dag_run(dag_id: str, conf: dict[str, str],
                    time_delay_minutes: int | None = None) -> None:
    """Trigger a DAG run with the given configuration."""
    payload: dict[str, Any] = {"conf": conf, "logical_date": None}
    if time_delay_minutes is not None:
        run_after = datetime.now(tz=pytz.utc) + timedelta(minutes=time_delay_minutes)
        payload["run_after"] = run_after.isoformat()

    response = requests.post(
        f"{_API_BASE}/api/v2/dags/{dag_id}/dagRuns",
        headers={"Authorization": f"Bearer {_API_TOKEN}"},
        json=payload, timeout=30,
    )
    response.raise_for_status()
```

Three things that changed and matter here:

1. **`execution_date` → `logical_date`**, and you **cannot** set a future `logical_date` any more. The current code abuses `execution_date=now + delay` to defer the file-mover run — that must become **`run_after`**, which is the Airflow 3 field for "don't run before".
2. Passing `logical_date: None` is now the normal way to trigger a manual run; identity comes from `run_id`.
3. The hand-built `run_id` via `DagRun.generate_run_id(...)` can be dropped — let the API generate it. (`generate_run_id` is also now keyword-only with a required `run_after`.)

**Alternative worth considering** for the 3 of 4 call sites that trigger exactly one DAG run: `TriggerDagRunOperator` from the standard provider now supports `run_after`, `conf`, and `logical_date` directly, and needs no API token. It does **not** fit `watcher_impl.start_acquisition_handler`, which triggers a variable number of runs inside a DB transaction with rollback-on-failure — that one needs the API call. See doc C §2.

⚠️ **New operational dependency:** workers now need `AIRFLOW__API__BASE_URL` and an API token. Create a token and inject it like the Mongo credentials in `docker-compose.yaml`. This also means the worker network must be able to reach the API server — check this against your nginx/firewall layout before cutover.

Refs: [Stable REST API v2](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html), [TriggerDagRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/trigger_dag_run.html)

### 3.3 `finalize_raw_file_status()` → `ti.get_task_states()` 🔴

`dags/impl/processor_impl.py:579`. `RuntimeTaskInstance` has **no** `get_dagrun()` in Airflow 3 (verified). If doc A §3.2 was done, only `_get_branch_states` changes:

```python
def _get_branch_states(ti) -> dict[int, dict[str, str]]:
    """Return {map_index: {task_id: state}} for all tasks in the processing task group."""
    states = ti.get_task_states(
        dag_id=ti.dag_id,
        task_group_id=TaskGroups.PROCESSING,
        run_ids=[ti.run_id],
    ).get(ti.run_id, {})

    branch_states: dict[int, dict[str, str]] = defaultdict(dict)
    for key, state in states.items():
        task_id, _, map_index = key.rpartition("_")
        if map_index.isdigit():
            branch_states[int(map_index)][task_id] = state
    return branch_states
```

The return shape is **not** documented; it was read off the API-server implementation (`airflow/api_fastapi/execution_api/routes/task_instances.py:1263-1273`):

```python
{"<run_id>": {"<task_id>": state,                  # non-mapped, map_index < 0
              "<task_id>_<map_index>": state}}     # mapped
```

⚠️ **Footgun:** mapped keys are `f"{task_id}_{map_index}"` with no escaping. Any task id ending in `_<digits>` becomes ambiguous. Current task ids are safe — keep it that way, and assert it in a test.

⚠️ Also confirm the `TaskInstanceState.FAILED` comparison still works: `get_task_states` returns the state as a **string**, not the enum. `TaskInstanceState` is a `str` enum so `state == TaskInstanceState.FAILED` still compares equal — but this is worth an explicit test rather than an assumption.

### 3.4 `_get_cluster_ssh_connections()` → REST API v2 🔴

`plugins/common/utils.py:159-180`. The Task Execution API can fetch a connection by id but has **no list operation** (verified in `airflow/sdk/execution_time/comms.py`) — so there is no SDK-only fix.

Good news: the v2 endpoint has a purpose-built query parameter (`connection_id_prefix_pattern`, verified in `airflow/api_fastapi/core_api/routes/public/connections.py:205`), so this maps cleanly:

```python
def _get_cluster_ssh_connections() -> list[str]:
    response = requests.get(
        f"{_API_BASE}/api/v2/connections",
        headers={"Authorization": f"Bearer {_API_TOKEN}"},
        params={"connection_id_prefix_pattern": CLUSTER_SSH_CONNECTION_ID_PREFIX},
        timeout=30,
    )
    response.raise_for_status()
    return sorted(c["connection_id"] for c in response.json()["connections"])
```

Drop `@provide_session` and the `Connection` import. `get_cluster_ssh_hook()` above it is unchanged — `SSHHook(ssh_conn_id=...)` resolves the connection through the Task Execution API automatically.

**Simpler alternative worth weighing:** put the connection ids in an Airflow Variable or in `alphakraken.<env>.yaml` and drop the API call entirely. It removes a network round-trip from a retry path, at the cost of maintaining the list in two places.

### 3.5 `get_airflow_variable()` — kwarg rename 🟡

`plugins/common/utils.py:73-87`. `airflow.sdk.Variable.get` renames `default_var` → **`default`**:

```python
value = Variable.get(key) if default == "__DEFAULT_NOT_SET" else Variable.get(key, default=default)
```

Silent runtime failure if missed — 9 call sites depend on it, including `AirflowVars.CONSIDER_OLD_FILES_ACQUIRED` in `acquisition_monitor.py:108`, where a wrong default silently changes acquisition semantics.

---

## 4. Infrastructure — `docker-compose.yaml`

### 4.1 Config keys that moved

Output of `airflow config lint` run against this repo's actual env block:

| Current (`docker-compose.yaml`) | Airflow 3 |
|---|---|
| `AIRFLOW__WEBSERVER__SECRET_KEY` (l.45) | `AIRFLOW__API__SECRET_KEY` |
| `AIRFLOW__API__AUTH_BACKENDS` (l.49) | **delete** — see §4.3 |
| `AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX` (l.53) | `AIRFLOW__FAB__ENABLE_PROXY_FIX` |
| `AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL` (l.54) | `AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL` |

Unchanged and still valid: `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CELERY__*`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`, `AIRFLOW__CORE__LOAD_EXAMPLES`, `AIRFLOW__CORE__TEST_CONNECTION`, `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK`.

### 4.2 New required config

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-webserver:8080/execution/'
AIRFLOW__API__BASE_URL: 'http://airflow-webserver:8080'
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET:?error}
AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

`EXECUTION_API_SERVER_URL` is what lets workers reach the Task Execution API instead of the metadata DB — **without it, every task fails**. Class path for the auth manager verified by import against `apache-airflow-providers-fab==3.8.0`.

### 4.3 Authentication changed completely

`airflow/api/auth/backend/` **does not exist** in Airflow 3 (verified — the directory is gone). The current value `"airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"` is dead config and must be deleted, not renamed.

Airflow 3's default auth manager is `SimpleAuthManager`. To keep the existing username/password login you must install `apache-airflow-providers-fab` and set `AIRFLOW__CORE__AUTH_MANAGER` as in §4.2. The API is JWT-based now; `_AIRFLOW_WWW_USER_CREATE` in `airflow-init` (l.246-249) still works but only with the FAB provider present.

Ref: [FAB auth manager](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/index.html)

### 4.4 Services

| Change | Detail |
|---|---|
| `airflow-webserver` → **`airflow-apiserver`** | `command: webserver` → `command: api-server`. `airflow webserver` errors out: *"Command `airflow webserver` has been removed. Please use `airflow api-server`"* |
| **NEW: `airflow-dag-processor`** | `command: dag-processor`. **Mandatory** — in Airflow 3 the scheduler no longer parses DAG files. Omit it and DAGs simply never appear |
| **NEW: `airflow-triggerer`** | Currently commented out (l.176-185). Still optional for the current DAGs, but required for the event-driven features in doc C |
| Health checks | `/health` still served on the api-server. Scheduler health-check port 8974 unchanged |
| `nginx` (l.357) | Still proxies 8080 → api-server. Verify `AIRFLOW__FAB__ENABLE_PROXY_FIX` restores the https-redirect behaviour that l.50-52 documents |

Ref: [official Airflow 3 docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml) — worth diffing against ours.

### 4.5 Dockerfile

`airflow_src/Dockerfile:16` sets `PYTHONPATH=$AIRFLOW_HOME` so `shared` is importable. **This still works** — verified. Airflow 3 also still appends `plugins/` to `sys.path`, so the bare imports (`from common.utils import ...`) keep working. No Dockerfile restructuring needed beyond the version ARG.

---

## 5. Metadata DB migration

```bash
# 1. stop everything
./compose.sh down

# 2. BACK UP POSTGRES - this is the only rollback path
pg_dump -Fc -U "$POSTGRES_USER" -h "$POSTGRES_HOST" "$POSTGRES_DB" > airflow_pre_af3_$(date +%F).dump

# 3. on the OLD (2.11) image, shrink the migration surface
airflow db clean --clean-before-timestamp <e.g. 90 days ago>

# 4. confirm no parse errors before upgrading
airflow dags reserialize

# 5. build the 3.3.1 image, then migrate
airflow db migrate

# 6. bring up: api-server, scheduler, dag-processor, workers
```

⚠️ `airflow db init` is **removed** — only `migrate | reset | check | check-migrations | clean` remain. Fix `.github/workflows/branch-checks.yaml:48` (doc A §4.2).

⚠️ Step 3 is not optional at your data volume — this instance has been accumulating runs since 2024 and the 3.0 schema migration rewrites task-instance tables.

---

## 6. Verification order

1. **Local** (`--profile local`): `airflow dags reserialize` → expect 0 import errors. This already passes against 3.3.1 with the current code.
2. **Per-DAG smoke test**, in dependency order — each exercises a different blocker:
   - `file_remover` — simplest; validates cron scheduling + worker→API path
   - `instrument_watcher` — validates `@continuous` and `FileCreationSensor`
   - `acquisition_handler` — validates **§3.2 `trigger_dag_run`** (the file-mover `run_after` delay especially)
   - `acquisition_processor` — validates **§3.3 `get_task_states`**, **§3.4 SSH connections**, paramiko 5, dynamic task mapping, and the `cluster_slots_pool` behaviour
   - `s3_uploader` — validates the amazon provider bump
3. **Explicitly force a branch failure** in `acquisition_processor` and confirm `finalize_raw_file_status` still produces the right `RawFileStatus` (DONE / QUANTING_FAILED / ERROR). This is the subtlest change in the whole migration and has no parse-time signal.
4. **Confirm `on_failure_callback` still fires** and still finds `raw_file_id` — callbacks run in a separate supervisor process in Airflow 3.

---

## 7. Rollback

The schema migration is **one-way**. Rolling back means: stop everything → restore the `pg_dump` from §5 step 2 → redeploy the 2.11 image.

Consequence: **any DAG run that happened after cutover is lost from the Airflow DB.** The MongoDB raw-file state is unaffected, so files would be re-processed rather than lost — but confirm that assumption against `watcher_impl.get_unknown_raw_files` before you need it.

Practical mitigation: cut over during an acquisition gap, and keep the 2.11 image tag pullable.

---

## 8. Risk summary

| Risk | Severity | Signal if wrong |
|---|---|---|
| `trigger_dag_run` API token / network path not reachable from workers | 🔴 | Whole pipeline chain stops; every handler task fails |
| `run_after` semantics ≠ old `execution_date` delay | 🔴 | File mover runs immediately or never |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` missing | 🔴 | Every task fails immediately |
| `dag-processor` service not added | 🔴 | No DAGs appear at all — loud, easy to spot |
| `get_task_states` key parsing wrong | 🟡 | Wrong final `RawFileStatus`; **silent** |
| `Variable.get(default_var=)` not renamed | 🟡 | Wrong defaults; **silent** |
| pandas 3 string-dtype change in metrics | 🟡 | Wrong metric values; **silent**; `msqc`/`skyline` untested |
| paramiko 5 vs cluster SSH daemon | 🟡 | Job submission fails; loud |
| FAB auth not configured | 🟡 | Nobody can log in; loud |
