# Settings Refactor: M:N Project-Settings with Scoped Resolution

## Context

The current settings system has three problems:
1. **MSQC is special-cased** — hardcoded `ACTIVATE_MSQC` flag, dirty Slurm param overrides, Sciex hack
2. **Only 1 settings per project** — `Project.settings` is a single `ReferenceField`, so a project can have either alphadia OR custom, not both
3. **Inflexible fallbacks** — no way to scope settings by vendor or instrument

The goal is M:N project-settings relationships with scope-based resolution, dynamic task mapping (multiple parallel pipelines per raw file), and MSQC/Skyline as regular settings entries.

### Design Decisions
- **Scope resolution**: Accumulate all matching scopes. Per `software_type`, most specific scope level replaces less specific (instrument > vendor > `*`). At the same specificity level, **run ALL** — no deduplication. See [Revised Scope Resolution Algorithm](#revised-scope-resolution-algorithm).
- **MSQC**: Becomes a regular `Settings` entry (new software type), eliminates all special-casing
- **Skyline**: Also added as a software type (dedicated WP)
- **Slurm params**: Move into `Settings` model (each settings controls its own resources)
- **Data model**: `ProjectSettings` intermediate model (not ListField). **No unique constraint** — duplicates prevented in webapp UI only.
- **Scope format**: Convention-based single string — `*` = default, known vendor names = vendor, anything else = instrument
- **Branch failures**: All settings branches are equal. Any branch failure (including MSQC) causes `QUANTING_FAILED`. No optional/skippable branches.
- **Output paths**: `<output>/<project_id>/<raw_file_path>/<settings_name>/` — new `settings_name` subfolder prevents collisions when multiple settings process the same raw file. Old data stays at old paths; code handles both patterns during transition.

---

## WP1: DAG Refactor with Mapped Task Groups

**Goal**: Convert the linear DAG to the mapped task group pattern (classic operator API), preparing the structure for multiple settings per raw file. Initially wraps the current single-settings flow.

### Files to modify

**`airflow_src/dags/acquisition_processor.py`** — Rewrite `create_acquisition_processor_dag()`:

```python
@task
def prepare_quanting(params, instrument_id):
    return prepare_quanting_impl(...)  # returns list[dict] (single-element for now)

@task_group
def quanting_pipeline(quanting_env: dict):
    run_ = PythonOperator(task_id="run_quanting", ...)
    wait_ = WaitForJobStartSensor(task_id="wait_for_job_start", ...)
    monitor_ = WaitForJobFinishSensor(task_id="monitor_quanting", ...)
    check_ = ShortCircuitOperator(task_id="check_quanting_result", ...)
    compute_ = PythonOperator(task_id="compute_metrics", ...)
    upload_ = PythonOperator(task_id="store_metrics", ...)
    run_ >> wait_ >> monitor_ >> check_ >> compute_ >> upload_

@task(trigger_rule=TriggerRule.ALL_DONE)
def finalize_status(ti, params):
    finalize_raw_file_status_impl(...)

envs = prepare_quanting()
mapped = quanting_pipeline.expand(quanting_env=envs)
mapped >> finalize_status()
```

- Use classic operators (`PythonOperator`, `ShortCircuitOperator`, sensors) inside `@task_group` — only `prepare_quanting` and `finalize_status` use `@task` (required for `.expand()` return and `TriggerRule`)
- Remove `ACTIVATE_MSQC` flag and all MSQC-specific task definitions/wiring
- Remove MSQC Slurm hack from `run_quanting()` (processor_impl.py:327-333)
- **Assumption**: `ACTIVATE_MSQC = False` in production today, so removing MSQC wiring here is safe. MSQC becomes non-functional between WP1 and WP2.

**`airflow_src/dags/impl/processor_impl.py`**

`prepare_quanting()` (line 62):
- Wrap current single `quanting_env` dict into a `list[dict]` return
- Still uses `get_settings_for_project()` (M:N comes in WP3)

`store_metrics()`:
- Remove `update_raw_file(...DONE...)` — status update moves to `finalize_status`

Add `finalize_raw_file_status()`:
- Inspect all store_metrics task instances in DAG run (pattern from `toy_mapped_taskgroup_classic.py:91-111`)
- All succeeded -> DONE; any failed -> QUANTING_FAILED
- **No optional branches**: all settings branches are equal (including MSQC once added in WP2)

**`airflow_src/plugins/common/keys.py`**
- Add `Tasks.FINALIZE_STATUS`

### Key insight: sensors work unchanged
The sensors read job_id via `get_xcom(context["ti"], self.xcom_key_job_id)` in `pre_execute` (ssh_sensor.py:31-33). Airflow scopes XCom per map_index inside mapped task groups, so each branch reads its own job_id.

**⚠ Verification needed**: `get_xcom()` in `common/utils.py` may use explicit `task_ids` that bypass map_index scoping. Verify that `ti.xcom_pull(task_ids=...)` inside mapped task groups correctly returns the branch-local XCom value.

### Open: XCom bridge inside mapped task groups
Classic operators inside `@task_group` read `QUANTING_ENV` from XCom. But the `quanting_env` dict is passed as a task group parameter, not pushed to XCom. There's a gap between how the param arrives and how downstream operators consume it. Options:
- (a) Add a `@task` inside the group that pushes the param to XCom
- (b) Refactor downstream operators to `@task` decorators
This must be resolved during WP1 implementation.

### Deployment
- Deploy during low-activity window. Wait for **all in-flight `acquisition_processor` DAG runs** to complete before deploying, since the DAG structure change (static → mapped task groups) causes DAG recompilation and in-flight runs would see a different task graph.

### Testing
- Unit test `finalize_raw_file_status` with various succeed/fail combos
- Integration: run toy DAG locally to validate mapped task group + sensor behavior
- `pre-commit run --all-files` and `pytest`

---

## WP2: Add MSQC Software Type ✅

**Goal**: Add MSQC as a software type so it can be used as a regular settings entry, removing all MSQC special-casing.

### Files to modify

**`shared/keys.py`**
- Add `MSQC = "msqc"` to `SoftwareTypes`

**`shared/db/models.py`** (Settings class)
- Make `fasta_file_name` and `speclib_file_name` optional (`required=False`). MSQC and Skyline don't need these files.

**`shared/db/interface.py`** (`create_settings()`)
- Add software-type-aware validation: alphadia requires both `fasta_file_name` and `speclib_file_name`; msqc and skyline require neither. Raise `ValueError` if constraints are violated.

**`airflow_src/dags/impl/processor_impl.py`**
- `compute_metrics()` (line ~459): add MSQC to `software_type -> metrics_type` mapping
- `compute_metrics()`: remove the `AirflowSkipException` hack (processor_impl.py:487-493) that silently skips MSQC failures. With the new design, MSQC branch failure causes `QUANTING_FAILED` like any other branch.
- `run_quanting()`: add `SOFTWARE_TYPE_TO_JOB_SCRIPT` mapping for job script selection

**`airflow_src/plugins/common/keys.py`**
- IGNORE: Add `SOFTWARE_TYPE_TO_JOB_SCRIPT` constant: `{alphadia: "submit_job.sh", custom: "submit_job.sh", msqc: "submit_msqc_job.sh"}`
- INSTEAD: use "submit_job.sh" for all Slurm jobs
- Remove MSQC-specific task/XCom keys: `Tasks.RUN_MSQC`, `Tasks.MONITOR_MSQC`, `Tasks.COMPUTE_MSQC_METRICS`, `Tasks.UPLOAD_MSQC_METRICS`, `XComKeys.MSQC_JOB_ID`

**`webapp/pages_/settings.py`** (line 175)
- Add `SoftwareTypes.MSQC` to `software_type_options`

**`webapp/service/data_handling.py`** (lines 26-91)
- Generalize metrics merge: replace hardcoded alphadia/custom/msqc handling with a loop over all `MetricsTypes` values, each merged with appropriate suffix
- **Important**: With M:N settings, a raw file can have multiple settings of the same type. The merge must use `(raw_file, settings_name)` as the join key, not just `raw_file`. This is more complex than a simple loop — it's a proper N-way join where each settings branch produces its own metrics keyed by `settings_name`.

### Testing
- Update existing tests that reference MSQC-specific keys
- Test generalized metrics merge in data_handling.py
- Test `create_settings()` validation: alphadia without fasta → error, msqc without fasta → ok

---

## WP3: ProjectSettings Model + M:N Assignment

**Goal**: Replace 1:1 `Project.settings` with M:N via `ProjectSettings`. No scope filtering yet — all assignments use scope `*`.

### Files to modify

**`shared/db/models.py`** — Add new model:
```python
class ProjectSettings(Document):
    meta = {"strict": False, "auto_create_index": False}

    project = ReferenceField(Project, required=True)
    settings = ReferenceField(Settings, required=True)
    scope = StringField(max_length=64, default="*")
    created_at_ = DateTimeField(default=datetime.now)
```

**No unique constraint** — duplicates are prevented in the webapp UI only, not at the DB level. This allows assigning the same settings at different scopes, and multiple same-type settings at the same scope (both run).

**`shared/db/interface.py`** — Add new functions:
- `assign_settings_to_project(project_id, settings_id, scope)` — create assignment
- `remove_project_settings(project_settings_id)` — remove assignment by ID
- `get_project_settings(project_id) -> list[ProjectSettings]` — list all assignments
- `resolve_settings_for_raw_file(project_id) -> list[Settings]` — return all assigned settings (no scope filtering yet)

Keep `get_settings_for_project()` and `assign_settings_to_project()` working during transition.

**`airflow_src/dags/impl/processor_impl.py`**
- `prepare_quanting()`: replace `get_settings_for_project()` with `resolve_settings_for_raw_file()`, build list of quanting_envs (one per resolved Settings)
- Each `quanting_env` dict must include `settings_name` for output path disambiguation

**Output path change**: `get_internal_output_path_for_raw_file` (or its callers) must add `settings_name` as a subfolder: `<output>/<project_id>/<raw_file_path>/<settings_name>/`. New runs only — old data stays at old paths. Code in `data_handling.py` (metrics lookup) must handle both patterns during transition.

**`airflow_src/dags/impl/handler_impl.py`** (line 373)
- `_is_settings_configured()`: replace `get_settings_for_project()` with `resolve_settings_for_raw_file()`, return `len(result) > 0`

**`webapp/pages_/projects.py`** — Replace "Assign settings to project" section (lines 102-174):
- Show table of current `ProjectSettings` for selected project
- Add form: settings dropdown (scope defaults to `*`, not editable yet)
- Add remove button per assignment
- Update projects display table to show multiple settings

### Migration
- Script: for each Project with non-null `settings` field, create `ProjectSettings(project=project, settings=project.settings, scope="*")`

### Testing
- Test `resolve_settings_for_raw_file` with: single settings, multiple software_types, no settings
- Update processor_impl tests for list return type
- `pytest`

---

## WP4: Slurm Params in Settings

**Goal**: Make Slurm resource parameters configurable per settings instead of hardcoded.

### Files to modify

**`shared/db/models.py`** (Settings class, line 237)
- Add fields:
  - `slurm_cpus_per_task = IntField(min_value=1, default=8)`
  - `slurm_mem = StringField(max_length=16, default="62G")`
  - `slurm_time = StringField(max_length=16, default="02:00:00")`
  - `num_threads = IntField(min_value=1, default=8)`

**`shared/db/interface.py`** (create_settings, line 233)
- Add optional slurm_* params to `create_settings()` signature

**`airflow_src/dags/impl/processor_impl.py`**
- `prepare_quanting()`: read Slurm params from Settings fields instead of hardcoded values (delete `cpus_per_task = 8` etc. at line 65-68)

**`webapp/pages_/settings.py`**
- Add Slurm param fields to settings form (collapsible "Advanced" section with defaults pre-filled)
- Wire new fields to `create_settings()` call

### Migration
- No breaking changes (`strict: False` handles missing fields)
- Migration script: set explicit Slurm defaults on all existing Settings documents

### Testing
- Update `test_create_settings` for new params
- Test prepare_quanting reads Slurm params from Settings

---

## WP5: Add Skyline Software Type

**Goal**: Add SKYLINE as a software type so Skyline-based settings can be created and assigned to projects.

Note: Settings model changes (optional `fasta_file_name`/`speclib_file_name`) are already handled in WP2. Skyline doesn't need these fields, same as MSQC.

### Files to modify

**`shared/keys.py`**
- Add `SKYLINE = "skyline"` to `SoftwareTypes` (aligning with existing `MetricsTypes.SKYLINE`)

**`airflow_src/dags/impl/processor_impl.py`**
- `compute_metrics()`: add SKYLINE to `software_type -> metrics_type` mapping

**`airflow_src/plugins/common/keys.py`**
- Add `skyline: "submit_job.sh"` to `SOFTWARE_TYPE_TO_JOB_SCRIPT`

**`webapp/pages_/settings.py`** (line 175)
- Add `SoftwareTypes.SKYLINE` to `software_type_options`

### Testing
- Verify Skyline settings can be created and assigned
- `pytest`

---

## WP6: Scope Resolution

**Goal**: Enable scoped settings assignments — vendor-specific and instrument-specific overrides.

### Revised Scope Resolution Algorithm

```
resolve_settings_for_raw_file(project_id, instrument_id, instrument_type):
  1. Load all ProjectSettings for project_id
  2. Classify each by scope specificity:
     - scope == "*" → level 0 (default)
     - scope in KNOWN_VENDOR_NAMES and scope == instrument_type → level 1 (vendor match)
     - scope == instrument_id → level 2 (instrument match)
     - otherwise → no match (skip)
  3. Group matching entries by settings.software_type
  4. Per group: keep only entries at the HIGHEST matched specificity level
  5. Flatten groups → return list[Settings]
```

**Key behaviors**:
- Most specific scope **replaces** less specific for the same `software_type` (instrument > vendor > `*`)
- At the **same specificity level**, ALL settings run (no dedup)
- No unique constraint enforcement at DB level

**Examples**:
- Project has `alphadia@*`, `alphadia@bruker`, `msqc@*`. Bruker instrument gets: `[alphadia@bruker, msqc@*]`
- Project has `alphadia_v1@*`, `alphadia_v2@*`. Any instrument gets: `[alphadia_v1, alphadia_v2]` (both run)

### Files to modify

**`shared/keys.py`**
- Add constants: `DEFAULT_SCOPE = "*"`, `KNOWN_VENDOR_NAMES = ("bruker", "thermo", "sciex")`

**`shared/db/interface.py`**
- Extend `resolve_settings_for_raw_file(project_id, instrument_id, instrument_type)` with scope filtering per the algorithm above

**`airflow_src/dags/impl/processor_impl.py`**
- `prepare_quanting()`: pass `instrument_id` and `instrument_type` to `resolve_settings_for_raw_file()`
- Get `instrument_type` via existing `get_instrument_settings(instrument_id, YamlKeys.TYPE)`

**`airflow_src/dags/impl/handler_impl.py`**
- `_is_settings_configured()`: pass instrument_id and instrument_type to resolution

**`webapp/pages_/projects.py`**
- Add scope selector (text input with help: `*` = all, vendor name = vendor-specific, instrument id = instrument-specific) to the project-settings assignment form
- Validate scope string format: must be `*`, a known vendor name, or a valid instrument_id

### Testing
- Test `resolve_settings_for_raw_file` with scope scenarios:
  - Same software_type at default + vendor scope → vendor wins
  - Same software_type at default + vendor + instrument scope → instrument wins
  - Two same-type settings at same scope → both run
  - Mixed software_types with mixed scopes → correct per-type resolution
  - Non-matching scope (e.g. bruker settings, thermo instrument) → skipped

---

## WP7: Cleanup + Legacy Removal

**Goal**: Remove backward-compat code once everything works.

### Files to modify

**`shared/db/models.py`**
- Remove `Project.settings` field (the old 1:1 ReferenceField, line 226)

**`shared/db/interface.py`**
- Remove `get_settings_for_project()`
- Remove old `assign_settings_to_project()`
- Update all callers

**All test files** — Remove references to removed functions, verify no regressions.

### Migration
- Script: `Project.objects.update(unset__settings=True)` to remove old field
- Only run after confirming new code paths work

### Testing
- Full test suite: `pytest`
- `pre-commit run --all-files`

---

## Dependency Graph

```
WP1 (DAG mapped task groups)
 |
 v
WP2 (MSQC software type + generalized metrics merge)
 |
 v
WP3 (ProjectSettings M:N + webapp assignment)
 |
 ├──> WP4 (Slurm params in Settings + webapp form)
 |
 ├──> WP5 (Skyline software type)
 |
 └──> WP6 (Scope resolution + webapp scope selector)
       |
       v
      WP7 (cleanup, after all)
```

## Verification (end-to-end)

1. `pre-commit run --all-files` — formatting, linting, type-checking
2. `pytest` — all unit tests pass
3. Create MSQC and Skyline settings via webapp (verify fasta/speclib not required)
4. Assign multiple settings to a project with different scopes
5. Trigger `acquisition_processor` DAG — verify mapped task groups create N parallel branches
6. Verify output paths use `<output>/<project_id>/<raw_file_path>/<settings_name>/`
7. Verify metrics are correctly keyed by `(raw_file, settings_name)`
8. Verify finalize_status correctly aggregates branch results (MSQC failure → QUANTING_FAILED)
9. Verify scope resolution: instrument-specific overrides vendor-specific overrides default
