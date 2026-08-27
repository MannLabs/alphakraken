# Notes from the test migration (2.11.0 → 3.3.1)

Companion to `AIRFLOW3_A_PREP_CHANGES.md` (doc A) and `AIRFLOW3_B_MIGRATION_GUIDE.md` (doc B).
Result of executing doc B end to end against a real local deployment. Commit: `49a39866`.

**Bottom line:** doc B's *code* diff was essentially right — the three function bodies and the import
sweep landed with little trouble. Everything that actually cost time was outside it: dependency pins,
the python interpreter, two URLs, and three runtime behaviours that no unit test, `dags reserialize`
or `airflow config lint` can see. Of the nine problems below, **the plan predicted one**.

---

## 1. The difficulties, in the order they were hit

| # | Symptom | Real cause | In the plan? |
|---|---|---|---|
| 1 | `pip` resolution failure on `sagemaker-studio` | doc B §2 named 3 versions that contradict the constraints file | no |
| 2 | Firefox: *Server Not Found: airflow-apiserver:8080* | `[api] base_url` is the **external** URL; doc B §4.2 sets it to the compose service name | no |
| 3 | api-server 500 on `/auth/login/` | `session` table rows written by 2.11 are pickle; 3.x decodes them as msgpack | no |
| 4 | `ModuleNotFoundError: No module named 'impl'` | Airflow 3 no longer puts the DAGs folder on `sys.path` outside the DAG processor | no — doc A §5 says the opposite |
| 5 | `ImportError: cannot import name 'on_failure_callback'` | second-order symptom of #4 against a stale image | no |
| 6 | `TypeError: 'NoneType' object is not iterable` in `compute_checksum` | `xcom_pull` ignores `default` on the branch taken when `map_indexes` is omitted | **partly** — doc A §3.5 found the sibling `task_ids` bug on the same call, but not this one |
| 7 | `ty` pre-commit hook fails | not a code error: it resolves against a local env still on 2.11 | no |
| 8 | (latent, found while fixing #2) every task on every remote worker would fail | `EXECUTION_API_SERVER_URL` set to a compose service name, but workers run on a different machine | flagged as a question in doc B §3.2, not as a config change |
| 9 | (latent, found while checking #1) metrics tasks would fail at import | `apache-airflow` never *requires* pandas — it came from the base image | no |

Only #6 was anticipated at all, and only in half.

---

## 2. Corrections to doc B §1 — the python version

> "Python **3.11 stays** — 3.3.1 supports 3.10–3.14 and `constraints-3.11.txt` exists for it."

A constraints file existing is not the same as the image using it. `airflow_src/Dockerfile` used the
**untagged** `apache/airflow:${AIRFLOW_VERSION}`, which follows Airflow's default python. Resolved by
Docker Hub digest comparison:

| tag | default python |
|---|---|
| `apache/airflow:2.11.0` | **3.12** |
| `apache/airflow:3.3.1` | **3.13** |

So the constraint file has *never* matched the interpreter, and the migration widened the gap.
Confirmed from a running container: `/home/airflow/.local/lib/python3.13/site-packages/...`.

**Changed:** pinned both ends — `FROM apache/airflow:3.3.1-python3.13` and `constraints-3.13.txt`
everywhere, CI on 3.13.

For the record this is a determinism problem, not a smoking gun: only **12 of 699** pins differ between
`constraints-3.11.txt` and `constraints-3.13.txt`, and none are in the Flask/FastAPI stack.

⚠️ **Open:** local dev environments on python 3.11 may no longer be able to install
`requirements_airflow.txt`. Not verified — see §8.

---

## 3. Corrections to doc B §2 — the dependency pins

Doc B's table disagrees with `constraints-3.3.1/constraints-3.13.txt` in three places. Because every
line of `requirements_airflow.txt` carries `--constraint <that file>`, a disagreement is a hard
resolution failure, not a preference.

| Package | doc B §2 | actual constraint | note |
|---|---|---|---|
| `apache-airflow-providers-amazon` | 9.35.0 | **9.34.0** | |
| `apache-airflow-providers-standard` | 1.18.0 | **1.17.0** | already merged as 1.18.0 for 2.11 in PR A1 (`2c534888`); must drop |
| `sagemaker-studio` | not mentioned | **1.0.27** | repo pinned 1.0.23 |
| `boto3` | not mentioned | **1.43.56** | repo pinned 1.41.4 |
| `apache-airflow-providers-ssh` | 6.0.1 | 6.0.1 | ok |
| `apache-airflow-providers-fab` | 3.8.0 | 3.8.0 | ok, new |
| `apache-airflow-providers-celery` | 3.23.1 | — | doc B says to pin it; the repo never did and the image ships it. **Left unpinned** |

The amazon version is what produced the user-visible failure, and it surfaces indirectly: amazon
9.35.0 requires `sagemaker-studio>=1.0.25`, so pip blames the *sagemaker* pin, not the amazon one.

**Verified:** `uv pip compile --python-version 3.13 -c constraints-3.13.txt` over the corrected file —
188 packages, no conflicts. `paramiko==5.0.0` and `flask-appbuilder==5.2.2` as doc B predicted.

### 3.1 pandas was never a dependency

Checked the installed metadata of both releases:

```
apache-airflow 2.11.0 -> ["pandas>=1.2.5,<2.2; extra == 'pandas'", ...]
apache-airflow 3.3.1  -> []      # apache-airflow-core: [] as well
```

pandas is an **extra** in 2.x and absent from core in 3.x. Nothing in `requirements_airflow.txt` or
`shared/requirements_shared.txt` asks for it, yet all five `plugins/metrics/metrics/*.py` import it.
It has only ever been present because the `apache/airflow:2.11.0` base image shipped it.

Whether the 3.3.1 image still does was never verified. If it does not, every metrics task fails with
`ModuleNotFoundError` **at runtime only** — no parse-time or unit-test signal, because the test
environments install pandas separately.

**Changed:** `pandas==3.0.5` is now pinned explicitly, so the behaviour no longer depends on the image.

### 3.2 CI can no longer use one python environment

`streamlit==1.55.0` requires `pandas<3`; the airflow constraints pin `pandas==3.0.5`. Verified:
installing streamlit into a pandas-3 env silently downgrades pandas to 2.3.3.

Doc B §2.1 offered "either split the CI envs or accept the divergence knowingly". There is no longer a
choice — the two requirement sets are mutually exclusive.

**Changed:** `branch-checks.yaml` builds a separate venv for the webapp suite and for the airflow suite.
Production containers were never affected (the webapp image installs only its own requirements).

---

## 4. Corrections to doc B §3 — the code

### 4.1 §3.1 import sweep — mostly automatic, three gaps

`ruff check --preview --select AIR --fix --unsafe-fixes` did the bulk, including the §3.5
`Variable.get(default_var=)` → `default=` rename, which therefore needed no manual work at all.

What it did **not** do:
- `airflow.utils.trigger_rule.TriggerRule` → `airflow.task.trigger_rule.TriggerRule` — done by hand.
- the `airflow.exceptions.*` → `airflow.sdk.exceptions.*` moves — not flagged; swept by hand. They are
  literally the same class objects (verified by `is`), so this only silences deprecation warnings.
  `DagNotFound` has **no** SDK equivalent and stays on `airflow.exceptions`.
- imports were appended unsorted and `Param` was routed to `airflow.sdk.definitions.param`; both
  consolidated onto `airflow.sdk` by hand.

What it did that doc B does not mention: added `multiple_outputs=True` to three `@task` decorators
(rule `airflow-task-implicit-multiple-outputs`). Behaviour-preserving — the inference code is
byte-for-byte identical in 2.11 and 3.3.1 — but it is a real diff beyond the stated mapping table.

### 4.2 §3.2 `trigger_dag_run` — the plan's approach does not work as written

Two independent problems with the REST-API-plus-static-token design:

1. **Airflow 3 has no static API tokens.** `POST /auth/token` mints a JWT bounded by
   `AIRFLOW__API_AUTH__JWT_EXPIRATION_TIME` (default 86400s). A token baked into a long-running
   worker's environment stops working after a day and takes the DAG-chaining spine down with it.
2. **It would have silently killed a rollback path.** `watcher_impl.py` catches `DagNotFound` to delete
   the just-inserted raw file from MongoDB; its own comment says that without it "the file would need to
   be removed from the DB manually". A REST call surfaces a missing DAG as HTTP 404, not `DagNotFound`,
   so doc B's snippet leaves that `except` dead. Neither §3.2 nor the §8 risk table mentions it.

**Changed (decision taken during the migration):** the Task Execution API can trigger DAG runs
(`TriggerDagRun` in `airflow/sdk/execution_time/comms.py`) — the same channel `TriggerDagRunOperator`
uses in AF3. `trigger_dag_run` (`common/utils.py:116`) now sends that message via
`task_runner.SUPERVISOR_COMMS`, and re-raises `DagNotFound` on a 404 so the rollback contract holds.

Consequences: **no API token, no `AIRFLOW__API__BASE_URL` on workers, no worker→api-server REST path.**
Doc B §8's number-one red risk disappears entirely. The cost is that `SUPERVISOR_COMMS` is internal API
and may move between minor versions.

Note doc B §3.2 point 3 ("the hand-built `run_id` can be dropped — let the API generate it") is true for
the REST endpoint but **not** here: `run_id` is a path parameter of the execution API, so
`DagRun.generate_run_id()` is still required. It is a pure static method with no session, and
`TriggerDagRunOperator` itself calls it on AF3.

### 4.3 §3.3 `_get_branch_states` — worked as specified

The only change beyond doc B: added `test_task_ids_are_unambiguous_for_map_index`, which asserts no task
id ends in `_<digits>`, per the footgun doc B flagged. Also added a test that non-mapped tasks in the
group are excluded.

### 4.4 §3.4 `_get_cluster_ssh_connections` — REST replaced by a Variable

The REST approach would have reintroduced exactly the auth machinery §4.2 removed, for one function on a
retry path.

**Changed (decision taken during the migration):** the connection ids are read from a new Airflow
Variable `cluster_ssh_connection_ids` (`common/utils.py:178`); the Connection objects with the real
secrets stay in the UI.

⚠️ **This trades a silent failure in.** A connection added in the UI but not listed in the Variable is
never used, with no error. Documented in `docs/maintenance.md` and in the `get_cluster_ssh_hook` error
message, but it is a genuine regression in operability versus the prefix scan.

### 4.5 New: `ti` annotations and the type checker

Doc A §4.3 declined the alias, doc B said "the plain sweep". Done: `ti: TaskInstance` →
`RuntimeTaskInstance` at 25 sites. In `acquisition_processor.py` it sits behind `TYPE_CHECKING` (that
file has `from __future__ import annotations`), which avoids a ~0.6 s import at DAG-parse time.

Doc A §4.3's stated reason for declining was wrong: `dags/impl/*.py` do **not** have
`from __future__ import annotations`, so those annotations *are* evaluated at import.

The sweep to the SDK types then broke the `ty` pre-commit hook in two places nobody had considered:

| | Airflow 2 | Airflow 3 SDK |
|---|---|---|
| `DAG(tags=...)` | `list[str]` | `MutableSet[str]` |
| `DAG(params=...)` | `dict` | `ParamsDict` |

Both have attrs converters, so lists/dicts still work at runtime — but `ty` does not model converters
and the repo runs it in pre-commit. Changed to set literals and `ParamsDict(...)` wrappers.

---

## 5. Corrections to doc B §4 — infrastructure

### 5.1 §4.2 conflates two different URLs 🔴

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-webserver:8080/execution/'
AIRFLOW__API__BASE_URL: 'http://airflow-webserver:8080'
```

**`[api] base_url` is the externally visible URL.** It feeds the login redirect
(`auth/managers/simple/routes/login.py:89`), cookie path scoping (`api_fastapi/app.py:56`) and
`log_url` / `mark_success_url` (`models/taskinstance.py:818`). A compose service name there hands the
*browser* an unresolvable hostname → "Server Not Found" after login. Airflow 2's `[webserver] base_url`
was never set in this repo, so this is a regression created purely by following the plan.

**`[core] execution_api_server_url` is internal — but a service name is still wrong for production.**
A compose service name resolves only within one compose project on one host. This deployment splits
`infrastructure` and `workers` across machines, which is why `POSTGRES_HOST` / `MONGO_HOST` are env vars.
On 2.11 workers reached the metadata DB directly and never needed the webserver; on 3.x every task needs
the Execution API. A hardcoded service name means **every task on every remote worker fails immediately**
— and it would have passed the `--profile local` smoke test.

**Changed:** two env vars, following the existing `*_HOST` convention.

| Variable | Direction | local | sandbox/production |
|---|---|---|---|
| `AIRFLOW_APISERVER_HOST` | worker → api-server | `airflow-apiserver` | IP of the infrastructure VM |
| `AIRFLOW_BASE_URL` | browser → UI | `http://localhost:8080` | `https://<hostname>:8080` |

Plus `AIRFLOW_JWT_SECRET` (new required secret, all three env files). A
`curl --fail http://${AIRFLOW_APISERVER_HOST}:8080/health` check was added to the worker section of
`docs/deployment.md`.

### 5.2 §4.3 FAB — less work than feared

Two things that turned out **not** to be needed, recorded so nobody adds them:

- **`airflow fab-db migrate` is not required.** The FAB provider declares
  `"db-managers": [...FABDBManager]` in its provider info, and `airflow db migrate` auto-discovers it.
  Verified: `RunDBManager()` resolves `[FABDBManager]`, and a migration run creates all eleven `ab_*`
  tables. `[database] external_db_managers` does not need setting either.
- **The Flask `session` table is created automatically**, despite not being in `FABDBManager.metadata`.

`flask_app.secret_key` reads `[api] secret_key` (`providers/fab/www/app.py:70`), so doc B §4.1's
`AIRFLOW__WEBSERVER__SECRET_KEY` → `AIRFLOW__API__SECRET_KEY` rename is correct and sufficient.

---

## 6. Missing from doc B §5 — the session table must be purged 🔴

Same table, same class name, incompatible payloads:

| | class | serializer |
|---|---|---|
| 2.11 | `airflow.www.session.AirflowDatabaseSessionInterface` | flask-session default (**pickle**) |
| 3.3.1 | `airflow.providers.fab.www.session.AirflowDatabaseSessionInterface` | `_LazySafeSerializer` (**msgpack**) |

`airflow db migrate` does not rewrite the rows, so every session carried over from 2.11 is undecodable
and `GET /auth/login/` returns 500 with
`msgspec.DecodeError: MessagePack data is malformed: trailing characters (byte 1)`.

Doubly hard to diagnose: Flask's own 500 handler then fails with
`AssertionError: The session has not yet been opened`, so the *visible tail* of the traceback says
nothing about the cause — the real error is further up the log.

**Added to doc B §5 as a mandatory step**, plus §5.1 explaining it:

```bash
./compose.sh exec postgres-service psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'DELETE FROM session;'
```

Users must also clear their browser cookie for the Airflow host. Nothing is lost; everyone logs in again.

---

## 7. Corrections to doc A

### 7.1 §5 "bare imports still work" is false for plugins → dags 🔴

> "Still work. `settings.prepare_syspath_for_config_and_plugins()` still appends `PLUGINS_FOLDER` to
> `sys.path` in 3.3.1; confirmed by the clean parse"

The rename of that function is the tell. In Airflow 2, `prepare_syspath()` put **both** the DAGs and
plugins folders on `sys.path` in every process. In 3.3.1 (`settings.py:716`) it adds only `config/` and
`PLUGINS_FOLDER` — the DAGs folder is added by the DAG processor, which the api-server no longer runs.

`plugins/callbacks.py` imported `from impl.processor_impl import ...`, i.e. a plugins-folder module
reaching into the DAGs folder, so the api-server failed to load it. The "clean parse" that doc A cites
only exercised the DAG-processor path — the one that still has both folders.

**Changed:** `mv airflow_src/plugins/callbacks.py airflow_src/dags/callbacks.py` (+ its test).
`callbacks.py` was the only plugins-folder module importing from `dags/`, and the repo defines **no**
`AirflowPlugin` subclass at all — the plugins folder is used purely as a shared-code path — so nothing
needed it to live there. Verified with `plugins_manager.get_import_errors()` and the DAGs folder off
`sys.path`: before `{'callbacks.py': "No module named 'impl'"}`, after `NONE`.

The one-line alternative is `PYTHONPATH` += `${AIRFLOW_HOME}/dags`, which restores Airflow 2 behaviour
but keeps the api-server importing mongoengine/pandas/docker at startup.

**The follow-on symptom points at the wrong file.** `plugins_manager.py:253-255` does
`sys.modules[spec.name] = mod` *before* `exec_module`, and the `except` branch never removes the entry.
`spec.name` is the file stem, `callbacks`. So a failed plugin import parks an empty module under that
name and the DAG parse then reports:

```
ImportError: cannot import name 'on_failure_callback' from 'callbacks' (/opt/airflow/plugins/callbacks.py)
```

— naming the *poisoned* module's `__file__`, not the file being imported.

### 7.2 §3.5 found half the XCom problem 🔴

Doc A correctly found that `xcom_pull(task_ids=None)` reverses meaning between versions, and made
`task_ids` required. It did not notice that **`default` is ignored on the same call**.

`RuntimeTaskInstance.xcom_pull` (`task_runner.py:487`) has two branches:

```python
if not is_arg_set(map_indexes_iterable):        # map_indexes NOT passed
    ...
    xcoms.append(None) if values is None else xcoms.extend(values)
    if single_task_requested and len(xcoms) == 1:
        return xcoms[0]                          # `default` never consulted
    return xcoms

for t_id, m_idx in product(task_ids, map_indexes_iterable):   # map_indexes passed
    ...
    xcoms.append(default if value is None else value)          # `default` honoured
```

So `get_xcom(..., default=[])` **without** `map_indexes` returns `None` on Airflow 3 where Airflow 2
returned `[]`. Exactly two call sites are affected — and they are precisely the two doc A §3.5 singled
out as "the dangerous ones", the corruption-detection gates:

- `handler_impl.py:72` `compute_checksum` — crashed with `TypeError: 'NoneType' object is not iterable`
- `handler_impl.py` `decide_processing` — same shape

The two `_extract_errors` calls pass `map_indexes` and take the safe branch.

**Changed:** `get_xcom` (`common/utils.py:45`) applies the default itself rather than delegating to
`xcom_pull`, which restores Airflow 2 semantics regardless of branch. Two regression tests added.
Doc A's instinct to make the wrapper the sole XCom entry point is what made this a three-line fix.

### 7.3 §4.3 — right conclusion, wrong reason

See §4.5 above: `dags/impl/*.py` have no `from __future__ import annotations`.

---

## 8. Still open

Nothing below was verified during the test migration.

| Item | Why it is still open |
|---|---|
| pandas 3 against real `msqc` / `skyline` output | doc B §2.1; those two metrics have no test coverage |
| paramiko 5 against the cluster SSH daemon | needs the real cluster |
| Does `apache/airflow:3.3.1` ship pandas? | now moot — pinned explicitly — but the answer decides whether §3.1 was a live bug |
| Can `requirements_airflow.txt` install on a python **3.11** dev env? | the constraints are now 3.13; a local `pip install` did not take effect, and this is the likely reason |
| `pymongo==4.7.2` / `pytz==2025.2` in `requirements_shared.txt` vs the airflow constraint files | `uv pip compile` calls it unsatisfiable, but says the same of HEAD on 2.11 where CI builds the image — so the model is stricter than pip here. Pre-existing either way; the pytz gap widens from "identical" to a year apart |
| The `get_xcom` default fix | source-verified and unit-tested against a mocked `ti`; **not** yet exercised by a real task run |
| DB migration on the real 2.11 database, and the rollback path | doc B §5 / §7 |
| Worker → api-server reachability in sandbox/production | see §5.1; only the single-host local profile was exercised |
| `ty` in CI | the local env is still on 2.11, so the hook cannot pass there; CI installs 3.3.1 and should |

---

## 9. What this says about the verification plan

Doc B §6 lists: reserialize → per-DAG smoke tests → force a branch failure → check callbacks.

Everything in §1 of this document that mattered was found by **starting the stack**, not by the checks
that preceded it. For the record, of the nine problems:

- `airflow dags reserialize` caught **none** — it passed cleanly before and after every one of them.
- The unit suite caught **none** — it passed at 548 green with the plugins/`sys.path` bug, the
  `base_url` bug, the session-table bug and the XCom-default bug all present.
- `airflow config lint` caught **none** beyond the four key renames doc B already listed.
- `ruff --select AIR` caught **none** of the runtime issues, as doc A predicted.

The checks that would have caught things early, in rough order of value:

1. `uv pip compile -c <constraints>` over the requirement files — would have caught #1 and #9 in seconds.
2. Loading the plugins manager with the DAGs folder off `sys.path` — would have caught #4/#5.
3. Reading the *whole* container log rather than the tail — #3 and #5 both hide their real cause above
   the visible traceback.
4. Actually opening the UI in a browser — #2.

Suggest adding the first two to doc B §6 as steps 0a and 0b, before reserialize.
