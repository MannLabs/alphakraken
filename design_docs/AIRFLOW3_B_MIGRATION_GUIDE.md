# B) The migration: 2.11.0 → 3.3.1

Prerequisite: the `airflow_3_prep` stack (doc A §6) merged. Line references are against the tip of
`airflow_3_prep_V`, stacked on main `250579ab`.

⚠️ Everything marked verified below was checked against a running 3.3.1 deployment on the previous
base (`609a06bb`, branch `airflow_3_test_migration`); the plan was then re-based on `250579ab`
without re-running the stack.

---

## 1. Version decision

**Go directly 2.11.0 → 3.3.1.** No intermediate hop.

- Airflow 3 requires ≥ 2.7 as the source version ([upgrade guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)); 2.11 is the designated bridge release and this repo is already on it.
- ⚠️ The migration skill recommends "2.11 → 3.0.11 → 3.1". **That advice is stale**: `3.0.11` was never released (3.0.x stops at 3.0.6), and 3.1 is now four minor versions behind. Staging through 3.0.x buys nothing and costs two extra DB migrations.

Rollback is a **metadata-DB restore**, not a package downgrade — the schema migration is one-way. Plan accordingly (§7).

### 1.1 Python: pin both ends 🔴

A `constraints-3.11.txt` existing is not the same as the image using it. `airflow_src/Dockerfile` uses
the **untagged** `apache/airflow:${AIRFLOW_VERSION}`, which follows Airflow's default python. From
Docker Hub digest comparison:

| tag | default python |
|---|---|
| `apache/airflow:2.11.0` | **3.12** |
| `apache/airflow:3.3.1` | **3.13** |

So the constraint file has *never* matched the interpreter, and the migration widens the gap.
Confirmed from a running container: `/home/airflow/.local/lib/python3.13/site-packages/...`.

**Do:** pin both ends — `FROM apache/airflow:3.3.1-python3.13` in the Dockerfile, `constraints-3.13.txt`
in every requirements URL, CI on 3.13.

This is a determinism problem, not a smoking gun: only **12 of 699** pins differ between
`constraints-3.11.txt` and `constraints-3.13.txt`, and none are in the Flask/FastAPI stack. Pin it
anyway — §2 is unresolvable otherwise, because the resolver and the image must agree on a python.

⚠️ Local dev environments on python 3.11 may no longer be able to install `requirements_airflow.txt`.
Check this before it blocks someone.

---

## 2. Dependency changes

`airflow_src/requirements_airflow.txt` — repoint every constraint URL to
`constraints-3.3.1/constraints-3.13.txt` (§1.1):

```
apache-airflow==3.3.1 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.13.txt"
apache-airflow-providers-standard==1.17.0 --constraint "..."
apache-airflow-providers-ssh==6.0.1 --constraint "..."
apache-airflow-providers-fab==3.8.0 --constraint "..."     # NEW - see §4.3
apache-airflow-providers-amazon==9.34.0 --constraint "..."
sagemaker-studio==1.0.27 --constraint "..."
boto3==1.43.56 --constraint "..."
pandas==3.0.5 --constraint "..."                           # NEW - see §2.2
```

`airflow_src/Dockerfile:2` — `ARG AIRFLOW_VERSION=3.3.1`, and the `FROM` line pinned to
`apache/airflow:3.3.1-python3.13`.

🔴 **Every version here must equal the constraint file's**, because each line carries
`--constraint <that file>`. A disagreement is a hard resolution failure, not a preference. The values
above were read off `constraints-3.13.txt`; re-read them if the Airflow version moves:

| Package | naive guess | actual constraint | note |
|---|---|---|---|
| `apache-airflow-providers-amazon` | 9.35.0 | **9.34.0** | see below |
| `apache-airflow-providers-standard` | 1.18.0 | **1.17.0** | doc A §1 pinned 1.18.0 for 2.11 (`airflow_3_prep`); must be *dropped* here |
| `sagemaker-studio` | — | **1.0.27** | repo pinned 1.0.23 |
| `boto3` | — | **1.43.56** | repo pinned 1.41.4 |
| `apache-airflow-providers-celery` | 3.23.1 | — | **leave unpinned**; the repo never pinned it and the image ships it |

⚠️ **The amazon version blames the wrong package.** amazon 9.35.0 requires `sagemaker-studio>=1.0.25`,
so pip reports a conflict on the *sagemaker* pin, not on amazon. Do not chase the sagemaker line.

**Gate before anything else** (§6 step 0a):

```bash
uv pip compile --python-version 3.13 -c constraints-3.13.txt airflow_src/requirements_airflow.txt
```

Expect 188 packages, no conflicts, with `paramiko==5.0.0` and `flask-appbuilder==5.2.2` falling out.

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

**paramiko 5.0** — used only through `SSHHook` (`plugins/common/utils.py:218`, `sensors/ssh_utils.py`). The provider absorbs the API change; the risk is behavioural (auth/algorithm negotiation against your cluster's SSH daemon), not compile-time. As you said, easy to catch — but catch it *deliberately*: run the `submit_job` → `WaitForJobStartSensor` → `WaitForJobFinishSensor` chain against the real cluster in staging before switching production.

Also update `misc/requirements_development.txt:8-10` — the comment pinning `pandas==2.1.4` "because the apache/airflow:2.11.0 image comes with that version" is now wrong.

### 2.2 pandas was never a dependency — pin it 🔴

From the installed metadata of both releases:

```
apache-airflow 2.11.0 -> ["pandas>=1.2.5,<2.2; extra == 'pandas'", ...]
apache-airflow 3.3.1  -> []      # apache-airflow-core: [] as well
```

pandas is an **extra** in 2.x and absent from core in 3.x. Nothing in `requirements_airflow.txt` or
`shared/requirements_shared.txt` asks for it, yet all five `plugins/metrics/metrics/*.py` import it.
It has only ever been present because the `apache/airflow:2.11.0` base image shipped it.

If the 3.3.1 image does not, every metrics task fails with `ModuleNotFoundError` **at runtime only** —
no parse-time and no unit-test signal, because the test environments install pandas separately.

**Do:** pin `pandas==3.0.5` explicitly (§2), so the behaviour stops depending on the base image.

### 2.3 CI must split into two environments 🔴

The webapp container pins its own `pandas==2.2.2` (`webapp/requirements_webapp.txt:3`) and has **zero
Airflow imports**, so the pandas 3 bump does not reach the webapp in production. But CI installs webapp
and Airflow deps into *one* env (`.github/workflows/branch-checks.yaml`, which already notes this
shortcut).

This is no longer a judgement call: `streamlit==1.55.0` requires `pandas<3` and the airflow constraints
pin `pandas==3.0.5`. The two requirement sets are **mutually exclusive**. Installing streamlit into a
pandas-3 env silently downgrades pandas to 2.3.3 — so a single-env CI would not even fail loudly, it
would just stop testing what production runs.

**Do:** `branch-checks.yaml` builds one venv for the webapp suite and one for the airflow suite.
Production containers were never affected — the webapp image installs only its own requirements.

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

**What ruff does not do — four manual gaps:**

1. `airflow.utils.trigger_rule.TriggerRule` → `airflow.task.trigger_rule.TriggerRule`. Not flagged.
2. The `airflow.exceptions.*` → `airflow.sdk.exceptions.*` moves. Not flagged. They are literally the
   same class objects (verified by `is`), so this only silences deprecation warnings —
   except that `DagNotFound` has **no** SDK equivalent and stays on `airflow.exceptions`.
3. Imports are appended **unsorted**, and `Param` is routed to `airflow.sdk.definitions.param` rather
   than `airflow.sdk`. Consolidate by hand.
4. `ti` annotations. Import it as an **alias** so the signatures do not change (doc A §4.3):
   ```python
   from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance as TaskInstance
   ```
   Six `dags/impl/*.py` files plus `plugins/common/utils.py` and `acquisition_processor.py`. In
   `acquisition_processor.py` put it behind `TYPE_CHECKING` — that file has
   `from __future__ import annotations`, and the guard avoids a ~0.6 s import at DAG-parse time. The
   `impl` files do **not** have it, so their imports must be real.

**Bycatch ruff adds that is not in the table:** `multiple_outputs=True` on three `@task` decorators
(rule `airflow-task-implicit-multiple-outputs`). Behaviour-preserving — the inference code is
byte-for-byte identical in 2.11 and 3.3.1 — but it is a real diff, so do not be surprised by it.

The §3.5 `Variable.get(default_var=)` → `default=` rename **is** handled by ruff; no manual work.

Ref: [Task SDK API](https://airflow.apache.org/docs/task-sdk/stable/api.html)

#### Fallout: the `ty` pre-commit hook breaks in two places

Consequence of the sweep, and not obvious until the hook runs against an env that actually has 3.3.1:

| | Airflow 2 | Airflow 3 SDK |
|---|---|---|
| `DAG(tags=...)` | `list[str]` | `MutableSet[str]` |
| `DAG(params=...)` | `dict` | `ParamsDict` |

Both have attrs converters, so lists and dicts still work at runtime — but `ty` does not model
converters. Change to set literals and `ParamsDict(...)` wrappers.

⚠️ The hook cannot pass on a local env still pinned to 2.11. That is an environment problem, not a code
problem — do not "fix" the code to satisfy it.

### 3.2 `trigger_dag_run()` → the Task Execution API 🔴

`plugins/common/utils.py:106-130`. The current implementation writes the metadata DB through the ORM and will raise `RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0` on every worker.

**Use the Task Execution API, not the public REST API v2.** `TriggerDagRun` in
`airflow/sdk/execution_time/comms.py` is the same channel `TriggerDagRunOperator` uses on AF3. Keep the
signature; send the message via `task_runner.SUPERVISOR_COMMS`, and re-raise `DagNotFound` on a 404.

🔴 **Do not use a static API token.** Airflow 3 has none: `POST /auth/token` mints a JWT bounded by
`AIRFLOW__API_AUTH__JWT_EXPIRATION_TIME` (default 86400 s). A token baked into a long-running worker's
environment stops working after a day and takes the DAG-chaining spine of the pipeline down with it.

🔴 **The 404 must surface as `DagNotFound`.** `watcher_impl.py` catches `DagNotFound` to delete the
just-inserted raw file from MongoDB; its own comment says that without it "the file would need to be
removed from the DB manually". A plain REST call surfaces a missing DAG as HTTP 404 and leaves that
`except` dead — a silently broken rollback path.

Three things that changed and matter here:

1. **`execution_date` → `logical_date`**, and you **cannot** set a future `logical_date` any more. The current code abuses `execution_date=now + delay` to defer the file-mover run — that must become **`run_after`**, which is the Airflow 3 field for "don't run before".
2. Passing `logical_date: None` is now the normal way to trigger a manual run; identity comes from `run_id`.
3. ⚠️ **Keep** the hand-built `run_id` via `DagRun.generate_run_id(...)`. Dropping it is correct for
   the *REST* endpoint but wrong here — `run_id` is a **path parameter** of the execution API. It is a
   pure static method with no session, and `TriggerDagRunOperator` itself calls it on AF3.
   (`generate_run_id` is now keyword-only with a required `run_after`.)

**What this buys:** no API token, no `AIRFLOW__API__BASE_URL` on workers, no worker→api-server REST
path, no new firewall hole. It removes the single largest operational risk in this migration.

**What it costs:** `SUPERVISOR_COMMS` is internal API and may move between minor Airflow versions.
Accepted knowingly — the token approach fails on a one-day timer, which is worse.

The `tests/common/test_utils.py::test_trigger_dag_run{,_with_delay}` tests already fail on 3.3.1
against the old body (doc A §6), so this change has a `pytest` gate, not just a staging smoke test.

Refs: [TriggerDagRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/trigger_dag_run.html)

### 3.3 `finalize_raw_file_status()` → `ti.get_task_states()` 🔴

`RuntimeTaskInstance` has **no** `get_dagrun()` in Airflow 3 (verified).

Doc A §3.2 is **done** (`airflow_3_prep_II`): the ORM read is isolated in `_get_branch_states`
(`dags/impl/processor_impl.py:604`), the only remaining caller of `ti.get_dagrun()`. Swap that one
function body; `finalize_raw_file_status` (`:562`) and `_extract_errors` (`:616`) stay as they are.

```python
def _get_branch_states(ti: TaskInstance) -> dict[int, dict[str, str | None]]:
    """Return the state of every processing-branch task, keyed by map index and task id."""
    states = ti.get_task_states(
        dag_id=ti.dag_id,
        task_group_id=TaskGroups.PROCESSING,
        run_ids=[ti.run_id],
    ).get(ti.run_id, {})

    branch_states: dict[int, dict[str, str | None]] = defaultdict(dict)
    for key, state in states.items():
        task_id, _, map_index = key.rpartition("_")
        if map_index.isdigit():
            branch_states[int(map_index)][task_id] = state
    return branch_states
```

The keys must stay **full** task ids (`processing.prepare_job`, not `prepare_job`) — `_extract_errors`
strips the prefix itself via `removeprefix(_TASK_GROUP_PREFIX)`. `rpartition("_")` on
`processing.prepare_job_0` yields exactly that, so the contract holds.

The return shape is **not** documented; it was read off the API-server implementation (`airflow/api_fastapi/execution_api/routes/task_instances.py:1263-1273`):

```python
{"<run_id>": {"<task_id>": state,                  # non-mapped, map_index < 0
              "<task_id>_<map_index>": state}}     # mapped
```

⚠️ **Footgun:** mapped keys are `f"{task_id}_{map_index}"` with no escaping. Any task id ending in `_<digits>` becomes ambiguous. Current task ids are safe — keep it that way, and assert it in a test.

⚠️ Also confirm the `TaskInstanceState.FAILED` comparison still works: `get_task_states` returns the state as a **string**, not the enum. `TaskInstanceState` is a `str` enum so `state == TaskInstanceState.FAILED` still compares equal — but this is worth an explicit test rather than an assumption.

Two tests to add beyond the swap: `test_task_ids_are_unambiguous_for_map_index`, asserting no task id
ends in `_<digits>`; and one asserting non-mapped tasks in the group are excluded.

### 3.4 `_get_cluster_ssh_connections()` → an Airflow Variable 🔴

`plugins/common/utils.py:160-184`. The Task Execution API can fetch a connection by id but has **no list operation** (verified in `airflow/sdk/execution_time/comms.py`) — so there is no SDK-only fix.

The public REST v2 endpoint does have a purpose-built `connection_id_prefix_pattern` query parameter
(`airflow/api_fastapi/core_api/routes/public/connections.py:205`), so a REST port is possible — but it
reintroduces exactly the token-and-network machinery §3.2 removed, for one function on a retry path.

**Read the ids from a new Airflow Variable `cluster_ssh_connection_ids` instead.** The Connection
objects, with the real secrets, stay in the UI. Drop `@provide_session` and the `Connection` import.
`get_cluster_ssh_hook()` above it is unchanged — `SSHHook(ssh_conn_id=...)` resolves the connection
through the Task Execution API automatically.

⚠️ **This trades a silent failure in.** A connection added in the UI but not listed in the Variable is
never used, with no error. Mitigate in two places — `docs/maintenance.md` and the `get_cluster_ssh_hook`
error message — and accept it as a genuine regression in operability versus the prefix scan.

### 3.5 `get_airflow_variable()` — kwarg rename 🟡

`plugins/common/utils.py:74-89`. `airflow.sdk.Variable.get` renames `default_var` → **`default`**:

```python
value = Variable.get(key) if default == "__DEFAULT_NOT_SET" else Variable.get(key, default=default)
```

Silent runtime failure if missed — 8 call sites depend on it, including `AirflowVars.CONSIDER_OLD_FILES_ACQUIRED` in `acquisition_monitor.py:109`, where a wrong default silently changes acquisition semantics.

In practice `ruff --select AIR --fix` performs this rename, so it needs no manual work — but verify it
happened rather than assuming.

---

## 4. Infrastructure — `docker-compose.yaml`

### 4.1 Config keys that moved

Output of `airflow config lint` run against this repo's actual env block:

| Current (`docker-compose.yaml`) | Airflow 3 |
|---|---|
| `AIRFLOW__WEBSERVER__SECRET_KEY` (l.48) | `AIRFLOW__API__SECRET_KEY` |
| `AIRFLOW__API__AUTH_BACKENDS` (l.52) | **delete** — see §4.3 |
| `AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX` (l.56) | `AIRFLOW__FAB__ENABLE_PROXY_FIX` |
| `AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL` (l.57) | `AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL` |

Unchanged and still valid: `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CELERY__*`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`, `AIRFLOW__CORE__LOAD_EXAMPLES`, `AIRFLOW__CORE__TEST_CONNECTION`, `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK`.

### 4.2 New required config 🔴

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://${AIRFLOW_APISERVER_HOST}:8080/execution/'
AIRFLOW__API__BASE_URL: 'https://${AIRFLOW_EXTERNAL_HOST}'
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET:?error}
AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

**These two URLs are not the same thing, and neither may be a compose service name.** Getting this
wrong is the single easiest way to break this migration; both failure modes were hit in testing.

**`[api] base_url` is the externally visible URL.** It feeds the login redirect
(`auth/managers/simple/routes/login.py:89`), cookie path scoping (`api_fastapi/app.py:56`) and
`log_url` / `mark_success_url` (`models/taskinstance.py:818`). A compose service name here hands the
**browser** an unresolvable hostname — *Server Not Found* immediately after login. Airflow 2's
`[webserver] base_url` was never set in this repo, so a wrong value here is a regression created purely
by the migration.

**`[core] execution_api_server_url` is internal — but a service name is still wrong.** A compose service
name resolves only within one compose project on one host. This deployment splits `infrastructure` and
`workers` across machines, which is why `POSTGRES_HOST` / `MONGO_HOST` are env vars. On 2.11 workers
reached the metadata DB directly and never needed the webserver; on 3.x **every task** needs the
Execution API. A hardcoded service name means every task on every remote worker fails immediately — and
it passes a `--profile local` smoke test cleanly, because there it happens to resolve.

**Do:** two env vars, following the existing `*_HOST` convention. Verify the worker→api-server path from
a *worker host*, not from the compose network.

Class path for the auth manager verified by import against `apache-airflow-providers-fab==3.8.0`.

### 4.3 Authentication changed completely

`airflow/api/auth/backend/` **does not exist** in Airflow 3 (verified — the directory is gone). The current value `"airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"` is dead config and must be deleted, not renamed.

Airflow 3's default auth manager is `SimpleAuthManager`. To keep the existing username/password login you must install `apache-airflow-providers-fab` and set `AIRFLOW__CORE__AUTH_MANAGER` as in §4.2. The API is JWT-based now; `_AIRFLOW_WWW_USER_CREATE` in `airflow-init` (l.249) still works but only with the FAB provider present.

**Three things that are *not* needed** — recorded so nobody adds them:

- **`airflow fab-db migrate`.** The FAB provider declares `"db-managers": [...FABDBManager]` in its
  provider info and `airflow db migrate` auto-discovers it. Verified: `RunDBManager()` resolves
  `[FABDBManager]`, and one migration run creates all eleven `ab_*` tables.
- **`[database] external_db_managers`.** Same reason.
- **Creating the Flask `session` table.** It appears automatically, despite not being in
  `FABDBManager.metadata`. (Its *contents* are a different problem — §5.1.)

`flask_app.secret_key` reads `[api] secret_key` (`providers/fab/www/app.py:70`), so the §4.1
`AIRFLOW__WEBSERVER__SECRET_KEY` → `AIRFLOW__API__SECRET_KEY` rename is correct and sufficient.

Ref: [FAB auth manager](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/index.html)

### 4.4 Services

| Change | Detail |
|---|---|
| `airflow-webserver` → **`airflow-apiserver`** | `command: webserver` → `command: api-server`. `airflow webserver` errors out: *"Command `airflow webserver` has been removed. Please use `airflow api-server`"* |
| **NEW: `airflow-dag-processor`** | `command: dag-processor`. **Mandatory** — in Airflow 3 the scheduler no longer parses DAG files. Omit it and DAGs simply never appear |
| **NEW: `airflow-triggerer`** | Currently commented out (l.181-190). Still optional for the current DAGs, but required for the event-driven features in doc C |
| Health checks | `/health` still served on the api-server. Scheduler health-check port 8974 unchanged |
| `nginx` (l.355) | Still proxies 8080 → api-server. Verify `AIRFLOW__FAB__ENABLE_PROXY_FIX` restores the https-redirect behaviour that l.50-52 documents |

Ref: [official Airflow 3 docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml) — worth diffing against ours.

### 4.5 Dockerfile

`airflow_src/Dockerfile:16` sets `PYTHONPATH=$AIRFLOW_HOME` so `shared` is importable. **This still works** — verified. Airflow 3 also still appends `plugins/` to `sys.path`, so the bare imports (`from common.utils import ...`) keep working.

🔴 **But the DAGs folder is no longer on `sys.path` outside the DAG processor.** In Airflow 2,
`prepare_syspath()` added both folders in every process; in 3.3.1 the renamed
`prepare_syspath_for_config_and_plugins()` (`settings.py:716`) adds only `config/` and
`PLUGINS_FOLDER`. So any plugins-folder module importing from `dags/` fails to load in the api-server.
Doc A §4.5 handles the one case (`callbacks.py`) ahead of time — confirm it landed, and see doc A for
why the resulting `ImportError` names the wrong file.

Beyond that and the version/python ARGs (§1.1), no Dockerfile restructuring is needed.

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

# 6. MANDATORY: drop the Flask session rows written by 2.11.
#    They are pickle-encoded; the FAB provider in 3.x decodes them as msgpack and every
#    request to /auth/login/ dies with msgspec.DecodeError -> HTTP 500. See below.
./compose.sh exec postgres-service psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'DELETE FROM session;'

# 7. bring up: api-server, scheduler, dag-processor, workers
```

### 5.1 The session table must be purged 🔴

Verified by diffing the two implementations:

| | class | serializer |
|---|---|---|
| 2.11 | `airflow.www.session.AirflowDatabaseSessionInterface` | flask-session default (**pickle**) |
| 3.3.1 | `airflow.providers.fab.www.session.AirflowDatabaseSessionInterface` | `_LazySafeSerializer` (**msgpack**, via `msgspec`) |

Same table, same class name, incompatible payloads. `airflow db migrate` does not touch the rows, so
every session carried over from 2.11 is undecodable:

```
File ".../flask_session/sqlalchemy/sqlalchemy.py", line 152, in _retrieve_session_data
  return self.serializer.decode(serialized_session_data)
msgspec.DecodeError: MessagePack data is malformed: trailing characters (byte 1)
```

It surfaces as a 500 on `GET /auth/login/`, and the traceback is doubly confusing because Flask's own
500 handler then fails with `AssertionError: The session has not yet been opened` — the *real* error is
above that in the log.

Nothing is lost by deleting the rows: users simply log in again. **Also clear the browser cookie** for
the Airflow host, since the stale cookie points at a deleted row.

`session` is a registered `db clean` table (`recency_column_name="expiry"`), so
`airflow db clean --table session` is the supported alternative to raw SQL.

⚠️ `airflow db init` is **removed** — only `migrate | reset | check | check-migrations | clean` remain. `.github/workflows/branch-checks.yaml:48` already uses `db migrate` (doc A §4.2, `airflow_3_prep`).

⚠️ Step 3 is not optional at your data volume — this instance has been accumulating runs since 2024 and the 3.0 schema migration rewrites task-instance tables.

---

## 6. Verification order

🔴 **Steps 0a–0c come first and cost seconds.** Of the nine real problems hit during the test
migration, `dags reserialize` caught **none**, the unit suite caught **none** (548 green with four
runtime blockers present simultaneously), `airflow config lint` caught **none** beyond the §4.1 key
renames, and `ruff --select AIR` caught **none**. Everything that mattered was found by starting the
stack. These three checks move four of those failures earlier:

- **0a.** `uv pip compile --python-version 3.13 -c constraints-3.13.txt` over the requirement files
  (§2). Catches the constraint disagreements and the missing pandas pin before a single image builds.
- **0b.** Load the plugins manager with the DAGs folder **off** `sys.path` and assert
  `plugins_manager.get_import_errors()` is empty (doc A §4.5). Catches the api-server plugin failure,
  which no parse check sees.
- **0c.** Read the **whole** container log on first boot, not the tail. Both the session-table failure
  (§5.1) and the plugin failure hide their real cause *above* the visible traceback; the tail of each
  names something unrelated.

Then:

1. **Local** (`--profile local`): `airflow dags reserialize` → expect 0 import errors. This already passes against 3.3.1 with the current code — which is exactly why it is step 1 and not step 0.
2. **Open the UI in a browser.** Not curl: the `[api] base_url` failure (§4.2) only appears to a real
   browser following the post-login redirect.
3. **Per-DAG smoke test**, in dependency order — each exercises a different blocker:
   - `file_remover` — simplest; validates cron scheduling + worker→API path
   - `instrument_watcher` — validates `@continuous` and `FileCreationSensor`
   - `acquisition_handler` — validates **§3.2 `trigger_dag_run`** (the file-mover `run_after` delay especially)
   - `acquisition_processor` — validates **§3.3 `get_task_states`**, **§3.4 SSH connections**, paramiko 5, dynamic task mapping, and the `cluster_slots_pool` behaviour
   - `s3_uploader` — validates the amazon provider bump
4. **Explicitly force a branch failure** in `acquisition_processor` and confirm `finalize_raw_file_status` still produces the right `RawFileStatus` (DONE / QUANTING_FAILED / ERROR). This is the subtlest change in the whole migration and has no parse-time signal.
5. **Confirm `on_failure_callback` still fires** and still finds `raw_file_id` — callbacks run in a separate supervisor process in Airflow 3.

---

## 7. Rollback

The schema migration is **one-way**. Rolling back means: stop everything → restore the `pg_dump` from §5 step 2 → redeploy the 2.11 image.

Consequence: **any DAG run that happened after cutover is lost from the Airflow DB.** The MongoDB raw-file state is unaffected, so files would be re-processed rather than lost — but confirm that assumption against `watcher_impl.get_unknown_raw_files` before you need it.

Practical mitigation: cut over during an acquisition gap, and keep the 2.11 image tag pullable.

---

## 8. Risk summary

| Risk | Severity | Signal if wrong |
|---|---|---|
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` missing, or set to a compose service name | 🔴 | Every task on every **remote** worker fails immediately. Passes a single-host `--profile local` test |
| `AIRFLOW__API__BASE_URL` set to an internal hostname | 🔴 | *Server Not Found* in the browser right after login. Curl and health checks stay green |
| `session` table not purged before first boot | 🔴 | HTTP 500 on `/auth/login/`; traceback names the wrong error (§5.1) |
| `plugins/callbacks.py` still importing from `dags/` | 🔴 | api-server cannot load the plugin; the follow-on `ImportError` names the wrong file (doc A §4.5) |
| `run_after` semantics ≠ old `execution_date` delay | 🔴 | File mover runs immediately or never |
| `dag-processor` service not added | 🔴 | No DAGs appear at all — loud, easy to spot |
| Constraint file ≠ image python, or pins ≠ constraints | 🔴 | Image build fails, blaming a package you did not touch (§2) |
| `get_xcom` `default` not applied by the wrapper | 🔴 | `TypeError: 'NoneType' object is not iterable` in both corruption gates (doc A §3.5) |
| `get_task_states` key parsing wrong | 🟡 | Wrong final `RawFileStatus`; **silent** |
| `Variable.get(default_var=)` not renamed | 🟡 | Wrong defaults; **silent** |
| `cluster_ssh_connection_ids` Variable missing an entry | 🟡 | That cluster is never used; **silent** (§3.4) |
| pandas missing from the 3.3.1 image and not pinned | 🟡 | Every metrics task fails at runtime only; no parse or test signal |
| pandas 3 string-dtype change in metrics | 🟡 | Wrong metric values; **silent**; `msqc`/`skyline` untested |
| paramiko 5 vs cluster SSH daemon | 🟡 | Job submission fails; loud |
| FAB auth not configured | 🟡 | Nobody can log in; loud |
| `SUPERVISOR_COMMS` moves in a future minor version | 🟡 | `trigger_dag_run` breaks on a later upgrade, not this one (§3.2) |

**None of the 🔴 rows above has a parse-time or unit-test signal.** The suite was green with four of
them present at once. Plan the verification (§6) accordingly.

---

## 9. Carried forward — not verified

Everything else in this document was checked against a running 3.3.1 deployment. These were not:

| Item | Why it is still open |
|---|---|
| pandas 3 against real `msqc` / `skyline` output | §2.1; those two metrics have no test coverage |
| paramiko 5 against the cluster SSH daemon | needs the real cluster |
| Can `requirements_airflow.txt` install on a python **3.11** dev env? | the constraints are now 3.13 (§1.1) |
| `pymongo==4.7.2` / `pytz==2025.2` in `requirements_shared.txt` vs the airflow constraint files | `uv pip compile` calls it unsatisfiable, but says the same of 2.11 where CI builds the image today — so the resolver is stricter than pip here. Pre-existing either way; the pytz gap widens from "identical" to a year apart |
| The `get_xcom` default fix (doc A §3.5) | source-verified and unit-tested against a mocked `ti`; **not** exercised by a real task run |
| DB migration against the **real** 2.11 database, and the rollback path | §5 / §7 — only a test database was migrated |
| Worker → api-server reachability in sandbox/production | §4.2; only the single-host local profile was exercised |
| `ty` in CI | the local env is on 2.11, so the hook cannot pass there; CI installs 3.3.1 and should |
