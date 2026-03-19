# Settings Refactor: M:N Project-Settings with Scoped Resolution

## Context

The current settings system has three problems:
1. **MSQC is special-cased** — hardcoded `ACTIVATE_MSQC` flag, dirty Slurm param overrides, Sciex hack
2. **Only 1 settings per project** — `Project.settings` is a single `ReferenceField`, so a project can have either alphadia OR custom, not both
3. **Inflexible fallbacks** — no way to scope settings by vendor or instrument

The goal is M:N project-settings relationships with scope-based resolution, dynamic task mapping (multiple parallel pipelines per raw file), and MSQC/Skyline as regular settings entries.

### Design Decisions (from user input)
- **Scope resolution**: Accumulate all matching scopes, deduplicate by `software_type` (most specific wins per type). Specificity: instrument > vendor > `*`
- **MSQC**: Becomes a regular `Settings` entry (new software type), eliminates all special-casing
- **Skyline**: Also added as a software type
- **Slurm params**: Move into `Settings` model (each settings controls its own resources)
- **Data model**: `ProjectSettings` intermediate model (not ListField)
- **Scope format**: Convention-based single string — `*` = default, known vendor names = vendor, anything else = instrument
- **Webapp**: Included in this plan

---

## WP1: Extend Types + Add Slurm Params to Settings Model

**Goal**: Unify type system, make Slurm params configurable per settings.
EDIT: move Slurm params into a dedicated WP, towards the end

EDIT: move also the scope WP towards the end

### Files to modify

**`shared/keys.py`**
- Add `MSQC = "msqc"` and `SKYLINE = "skyline"` to `SoftwareTypes`
- Add constants: `DEFAULT_SCOPE = "*"`, `KNOWN_VENDOR_NAMES = ("bruker", "thermo", "sciex")`

**`shared/db/models.py`** (Settings class, line 237)
- Add fields to `Settings`:
  - `slurm_cpus_per_task = IntField(min_value=1, default=8)`
  - `slurm_mem = StringField(max_length=16, default="62G")`
  - `slurm_time = StringField(max_length=16, default="02:00:00")`
  - `slurm_num_threads = IntField(min_value=1, default=8)`

**`shared/db/interface.py`** (create_settings, line 233)
- Add optional slurm_* params to `create_settings()` signature (defaults match model defaults)

**`webapp/pages_/settings.py`**
- Add `MSQC` and `SKYLINE` to `software_type_options` (line 175)
- Add Slurm param fields to form (collapsible "Advanced" section with defaults pre-filled)
- Wire new fields to `create_settings()` call

### Migration
- No breaking changes (`strict: False` handles missing fields on old documents)
- Migration script: set explicit Slurm defaults on all existing Settings documents

### Testing
- Existing tests in `shared/tests/db/` — update `test_create_settings` for new params
- Verify old Settings documents load without errors

---

## WP2: ProjectSettings Model + Resolution Logic

**Goal**: M:N relationship with scope-based resolution.

### Files to modify

**`shared/db/models.py`** — Add new model:
```python
class ProjectSettings(Document):
    meta = {"strict": False, "indexes": [
        {"fields": ["project", "settings"], "unique": True}
    ], "auto_create_index": False}

    project = ReferenceField(Project, required=True)
    settings = ReferenceField(Settings, required=True)
    scope = StringField(max_length=64, default=DEFAULT_SCOPE)  # "*", vendor name, or instrument id
    created_at_ = DateTimeField(default=datetime.now)
```

**`shared/db/interface.py`** — Add new functions:
- `create_project_settings(project_id, settings_id, scope)` — create assignment
- `remove_project_settings(project_id, settings_id)` — remove assignment
- `get_project_settings(project_id) -> list[ProjectSettings]` — list all assignments for a project
- `resolve_settings_for_raw_file(project_id, instrument_id, instrument_type) -> list[Settings]` — **core resolution**:
  1. Load all `ProjectSettings` for `project_id`
  2. Filter to matching scopes: `*` always matches, scope == `instrument_type` matches vendor, scope == `instrument_id` matches instrument
  3. Group by `settings.software_type`
  4. Per group, keep highest specificity (instrument > vendor > default)
  5. Return list of winning `Settings` objects

Keep `get_settings_for_project()` and `assign_settings_to_project()` working during transition (removed in WP6).

### Migration
- Script: for each Project with non-null `settings` field, create `ProjectSettings(project=project, settings=project.settings, scope="*")`

### Testing
- Test `resolve_settings_for_raw_file` with:
  - Single default-scope settings
  - Two different software_types at default scope -> returns both
  - Same software_type at default + vendor scope -> vendor wins
  - Same software_type at default + vendor + instrument scope -> instrument wins
  - No matching settings -> empty list

---

## WP3: Refactor processor_impl for Multi-Settings

**Goal**: `prepare_quanting` returns a list of envs. Remove MSQC hardcoding and Slurm hardcoding.

### Files to modify

**`airflow_src/dags/impl/processor_impl.py`**

`prepare_quanting()` (line 62):
- Replace `get_settings_for_project()` with `resolve_settings_for_raw_file(project_id, instrument_id, instrument_type)`
- Get `instrument_type` via existing `get_instrument_settings(instrument_id, YamlKeys.TYPE)`
- Build a `list[dict]` of quanting_envs (one per resolved Settings)
- Each env gets Slurm params from Settings fields (delete hardcoded `cpus_per_task = 8` etc.)
- Push list to XCom (for mapped task group consumption)
- Raise `AirflowFailException` if resolution returns empty list

`run_quanting()` (line ~260):
- Remove MSQC Slurm hack (lines 327-333) — params now come from Settings
- Job script name: add constant `SOFTWARE_TYPE_TO_JOB_SCRIPT` mapping in `plugins/common/keys.py`

`compute_metrics()` (line ~459):
- Add MSQC and SKYLINE to the `software_type -> metrics_type` mapping (identity mapping since types are now aligned)

`upload_metrics()`:
- Remove `update_raw_file(...DONE...)` — status update moves to new `finalize_status` task

Add `finalize_raw_file_status()`:
- Inspect all upload_metrics task instances in DAG run (follows pattern from `toy_mapped_taskgroup_classic.py:91-111`)
- All succeeded -> DONE; all failed -> QUANTING_FAILED; mixed -> DONE (partial success)

**`airflow_src/plugins/common/keys.py`**
- Add `SOFTWARE_TYPE_TO_JOB_SCRIPT` mapping
- Add `Tasks.FINALIZE_STATUS`
- Remove MSQC-specific task/XCom keys after WP4 is done

**`airflow_src/dags/impl/handler_impl.py`** (line 373)
- `_is_settings_configured()`: replace `get_settings_for_project()` with `resolve_settings_for_raw_file()`, return `len(result) > 0`
- Needs instrument_type — get from yaml settings like processor_impl does

### Testing
- Update existing processor_impl tests for list return type
- Test prepare_quanting with multiple resolved settings
- Test finalize_raw_file_status with various succeed/fail combos

---

## WP4: DAG Refactor with Mapped Task Groups

**Goal**: Replace linear DAG with dynamic mapped task groups (following `toy_mapped_taskgroup_classic.py`).
EDIT: use the "classic operator API" wherever possible
EDIT: move this up to WP1, preparing for the extension to multiple software types

### Files to modify

**`airflow_src/dags/acquisition_processor.py`** — Rewrite `create_acquisition_processor_dag()`:

```
@task
def prepare_quanting(params, instrument_id, instrument_type):
    return prepare_quanting_impl(...)  # returns list[dict]

@task_group
def quanting_pipeline(quanting_env: dict):
    run_ = PythonOperator(task_id="run_quanting", ...)
    wait_ = WaitForJobStartSensor(task_id="wait_for_job_start", ...)
    monitor_ = WaitForJobFinishSensor(task_id="monitor_quanting", ...)
    check_ = ShortCircuitOperator(task_id="check_quanting_result", ...)
    compute_ = PythonOperator(task_id="compute_metrics", ...)
    upload_ = PythonOperator(task_id="upload_metrics", ...)
    run_ >> wait_ >> monitor_ >> check_ >> compute_ >> upload_

@task(trigger_rule=TriggerRule.ALL_DONE)
def finalize_status(ti, params):
    finalize_raw_file_status_impl(...)

envs = prepare_quanting()
mapped = quanting_pipeline.expand(quanting_env=envs)
mapped >> finalize_status()
```

- Remove `ACTIVATE_MSQC` flag entirely
- Remove all MSQC-specific task definitions and wiring
- Sensors: XCom works per map_index inside mapped task groups — no sensor changes needed (`get_xcom(context["ti"], ...)` reads from correct map_index)

### Key insight from sensors (ssh_sensor.py:31-33)
The sensors read job_id via `get_xcom(context["ti"], self.xcom_key_job_id)` in `pre_execute`. Airflow's mapped task instances scope XCom per map_index automatically, so each branch reads its own job_id.

### Testing
- Unit test `finalize_raw_file_status`
- Integration: run toy DAG locally first to validate mapped task group + sensor behavior
- Run `pre-commit run --all-files` and `pytest`

---

## WP5: Webapp Updates (can run in parallel with WP3/WP4)

**Goal**: Multi-settings assignment UI with scope, Slurm params in settings form.
EDIT: merge the respective webapp parts into the respective other WPs

### Files to modify

**`webapp/pages_/projects.py`** — Replace "Assign settings to project" section (lines 102-174):
- Show table of current `ProjectSettings` for selected project (settings name, version, software_type, scope)
- Add form: settings dropdown + scope text input (help text: `*` = all, vendor name = vendor-specific, instrument id = instrument-specific)
- Add remove button per assignment
- Update projects display table: show comma-separated `settings_name (scope)` list instead of single settings column

**`webapp/service/db.py`**
- Add `get_project_settings_data(project_id=None)` function

**`webapp/service/data_handling.py`** (lines 26-91) — Generalize metrics merge:
- Replace hardcoded alphadia/custom/msqc handling with a loop over all `MetricsTypes` values
- Each metrics type that has data gets merged with appropriate suffix
- This handles any future metrics types automatically

**`webapp/pages_/projects.py`** — Update imports:
- Import `create_project_settings`, `remove_project_settings`, `get_project_settings` from `shared.db.interface`

### Testing
- Manual webapp testing
- Unit test generalized metrics merge in data_handling.py

---

## WP6: Cleanup + Legacy Removal

**Goal**: Remove backward-compat code once everything works.

### Files to modify

**`shared/db/models.py`**
- Remove `Project.settings` field (the old 1:1 ReferenceField, line 226)

**`shared/db/interface.py`**
- Remove `get_settings_for_project()`
- Remove old `assign_settings_to_project()`
- Update all callers

**`airflow_src/plugins/common/keys.py`**
- Remove `Tasks.RUN_MSQC`, `Tasks.MONITOR_MSQC`, `Tasks.COMPUTE_MSQC_METRICS`, `Tasks.UPLOAD_MSQC_METRICS`
- Remove `XComKeys.MSQC_JOB_ID`

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
WP1 (types + slurm params)
 |
 v
WP2 (ProjectSettings + resolution)
 |
 ├──> WP3 (processor_impl) ──> WP4 (DAG refactor)
 |                                      |
 └──> WP5 (webapp, parallel to WP3/4)   |
                                         v
                              WP6 (cleanup, after all)
```

## Verification (end-to-end)

1. `pre-commit run --all-files` — formatting, linting, type-checking
2. `pytest` — all unit tests pass
3. Create MSQC and Skyline settings via webapp
4. Assign multiple settings to a project with different scopes
5. Trigger `acquisition_processor` DAG — verify mapped task groups create N parallel branches
6. Verify metrics are correctly keyed by (raw_file, settings_name, settings_version)
7. Verify finalize_status correctly aggregates branch results
