# Implementation plan: database interface

Implements `DB_INTERFACE_DESIGN.md`. Written 2026-11-06 against commit 04dd3f2b on
`refactor_db_I`, assuming a clean working tree. The exploratory raw-files slice built on
2026-11-03 is reverted; what it taught is folded into slices 1 and 2.

## Overview

Replace the direct mongoengine use of five clients by `shared.db` (provider-free query
functions returning `shared.domain` dataclasses), with the Mongo code fenced into
`shared/db/_mongo/`. The validation refactoring (`VALIDATION_DESIGN.md`) follows as its own
plan.

## How the slices are cut

The design document's steps 5.1 to 5.8 are layered: first all constants, then all
datetimes, then all dataclasses. This plan cuts the other way: **one slice = one client, or
one page group, taken fully off mongoengine, with its old code deleted in the same PR.**
Every slice ends with a running system, a shorter fence exemption list, and a smoke test on
the local compose stack.

The fence is the progress meter. Slice 1 enables the ruff ban on `mongoengine`, `pymongo`
and `shared.db._mongo` with a per-file exemption list naming every legacy module. Each later
slice deletes its entries. The refactoring is done when the list is empty.

A slice builds only the API its own callers use. No function is added for a caller in a
later slice.

Each slice is one PR. No compatibility shims between slices: a function moves, its callers
move with it, the old one is deleted.

Slice sizes are given in working days for one person who knows the code.

## Vertical slices

### Slice 1: domain package, constants, fence (0.5 day)

**Business value**: no new mongoengine import can slip into a client from here on, and the
exemption list shows how much is left. Nothing changes at runtime.

**Technical scope**: `shared/domain/`, `shared/db/models.py`, `pyproject.toml`, every
module that imports a status constant.

**Implementation details**:
- `shared/domain/__init__.py` and `shared/domain/errors.py` with `NotFoundError`,
  `AlreadyExistsError`, `ValidationError(errors: list[str])`, `DatabaseError`,
  `DatabaseUnavailableError(DatabaseError)`. The names carry the `Error` suffix the ruff
  config enforces; update both design documents.
- Move the status constants (`RawFileStatus`, `BackupStatus`, `InstrumentFileStatus`,
  `ProjectStatus`, `SettingsStatus`, `UserStatus`, `KrakenStatusValues`,
  `KrakenStatusEntities`, `SlurmJobQueueStates`, `TERMINAL_STATUSES`, `ERROR_STATUSES`,
  the expiry constants) from `shared/db/models.py` to `shared/domain/<aggregate>.py`.
  `models.py` imports them from there; all other imports are rewritten. Mechanical.
- `pyproject.toml`: `flake8-tidy-imports.banned-api` for `mongoengine`, `pymongo`,
  `shared.db._mongo`, and, until slice 9 removes them, `shared.db.{models,interface,engine}`
  (otherwise a client importing only `shared.db.models` is not on the list and the list
  measures nothing); `per-file-ignores` with `TID251` for `shared/db/_mongo/**`,
  `shared/tests/db/**`, the migration scripts, `airflow_src/plugins/celery_config.py`
  (worker preload), and the legacy list: `shared/db/{interface,models,engine}.py`,
  `shared/{path_layout,display_paths,settings_scope_resolver}.py`,
  `webapp/service/{db,data_handling}.py`, `webapp/pages_/**`, `rest_api/service.py`,
  `mcp_server/server.py`, `monitoring/**`, `airflow_src/**`, plus the test modules and
  `rest_api/main.py`, `webapp/service/owner.py` that import a banned module. `TID251` flags
  `from mongoengine import X` (verified with ruff 0.9.1), no `TID253` needed.

**Testing**: pre-commit green with the exemption list; the full test suite unchanged.

**Exit**: exemption list committed. Everything outside it is fenced.

### Slice 2: raw files read API, three alerts, status page (1.5 days)

**Business value**: the walking skeleton. Three alerts and one page read dataclasses through
the fenced backend; the mapper, error translation and connection handling exist and are
verified on a real database.

**Technical scope**: `shared/domain/raw_files.py`, `shared/db/raw_files.py`,
`shared/db/_mongo/{connection,errors,mappers,raw_files}.py`, three alerts,
`webapp/service/db.py` (one function), their tests.

**Implementation details**:
- `RawFile` frozen dataclass: all document fields, UTC-aware datetimes, `file_info:
  dict | None` with `None` meaning not loaded, `has_project`.
- `shared/db/raw_files.py`: `RawFileFilter` and `find(where, *, order, limit, offset,
  include_file_info=False)`. Only `find`; `count`, `get`, `latest_per_instrument` and
  `count_by_status` arrive with their callers in slices 3, 4 and 9. `order` is validated
  against the dataclass field names; `DEFAULT_ORDER` is the single definition of the
  default, used by the backend too.
- `_mongo/raw_files.py`: table-driven translation of the filter into query conditions
  (ruff's complexity limit rejects an if-chain), `exclude("file_info")` unless opted in.
- `_mongo/mappers.py`: `to_raw_file(doc, *, file_info_loaded)`; `as_utc` in two variants
  so that the non-optional audit fields need no `type: ignore`; `file_info` lists back to
  tuples.
- `_mongo/errors.py`: `translate_errors` decorator; `DoesNotExist` to `NotFoundError`,
  `NotUniqueError`/`DuplicateKeyError` to `AlreadyExistsError`, mongoengine
  `ValidationError` to the domain one with one message per field,
  `ConnectionFailure`/`ServerSelectionTimeoutError` to `DatabaseUnavailableError`, other
  `PyMongoError` to `DatabaseError`.
- `_mongo/connection.py`: `ensure_connected()` with a module flag, calling
  `connect_db(raise_on_error=True)` so the flag is set only after a successful
  registration. Reconnects after a drop are the pymongo client's job; a docstring says so.
- Port `raw_file_error_alert.py`, `s3_upload_failure_alert.py`,
  `instrument_stall_alert.py` (explicit `order`, no `ensure_utc`), and
  `get_raw_files_for_status_df` in `webapp/service/db.py`. Clients import the module:
  `from shared.db import raw_files`.
- Tests: `shared/tests/factories.py` with a `raw_file(**overrides)` factory returning a
  `RawFile` dataclass; the alert tests use it and patch `shared.db.raw_files.find`; the
  empty-DB page fixture patches the same; `shared/tests/db/test_raw_files.py` covers the
  mapper (real documents in, dataclasses out), the filter translation, the error
  translation and the connection flag with `MagicMock` querysets.

**Testing**: pre-commit; `pytest shared monitoring/tests/alerts webapp`; on the local
compose stack: provoke one ERROR file and one UPLOAD_FAILED file, open the status page;
the monitoring log shows both issues, the page shows the rows. Record the status page
render time for the comparison in slice 6.

**Exit**: the three alerts read raw files through `shared.db` only. They stay on the exemption
list until slice 5, as they still type-hint `KrakenStatus` from `shared.db.models`; the
`monitoring/**` entry therefore stays a tree until then.

### Slice 3: UTC-aware timestamps on the write side (0.5 day)

**Business value**: every timestamp the system stores and reads is explicitly UTC; the
`ensure_utc` and `tzinfo is None` branches can go as their callers are ported.

**Technical scope**: `shared/db/models.py` defaults, `interface.py` `datetime.now` calls,
test fixtures.

**Implementation details**:
- `default=datetime.now` becomes `default=lambda: datetime.now(UTC)` on all models;
  `interface.py` already uses `pytz.utc` on explicit writes, unify to `datetime.UTC`.
- The legacy read path (`interface.py`) returns naive datetimes until its functions are
  ported; the mappers return aware ones. Both are UTC. The one known comparison of naive
  `datetime.now()` against DB values is the elapsed-time code in
  `airflow_src/plugins/file_handling.py`; check it and the `ensure_utc` callers.

**Testing**: `pytest shared airflow_src`; on the stack, a new raw file shows the right
`created_at_` in the webapp.

Own slice so that a tz regression is bisectable.

### Slice 4: metrics read, MCP server off mongoengine (2 days)

**Business value**: one implementation of "raw file with its latest metrics per type"
replaces three; the MCP server and the REST raw files endpoint no longer touch mongoengine.
Harmonization of legacy metric names happens once.

**Technical scope**: `shared/domain/metrics.py`, `shared/db/metrics.py`,
`shared/db/_mongo/metrics.py`, `shared/db/raw_files.py` (`count`), `shared/db/status.py`
(`instrument_ids()` only), `mcp_server/server.py`, `rest_api/service.py` (raw files part),
`monitoring/alerts/pump_pressure_alert.py`.

**Implementation details**:
- `Metrics` dataclass with `values: dict[str, Any]`; `metrics.latest_by_raw_file(ids |
  created_after, types, fields)`; the legacy key map in `_mongo/metrics.py`, applied to
  the documents and to the `fields` projection; new key wins on collision.
- `raw_files.count(where)` for REST pagination.
- `status.instrument_ids()` and the `KrakenStatus` dataclass, read side only, for the MCP
  instruments tool (the rest of the status aggregate is slice 5).
- MCP: the three tools on `raw_files.find`, `metrics.latest_by_raw_file`,
  `status.instrument_ids`; delete its `augment_raw_files_with_metrics` copy; fix the
  gradient filter `KeyError` and the docstring default while there (BOYSCOUT 2026-11-03).
- REST `/raw_files`: `raw_files.find/count` plus `metrics.latest_by_raw_file`; flatten
  `{**metadata, **values}` back into the `metrics` list so the payload stays identical.
- Pump pressure alert on `metrics.latest_by_raw_file(fields=[...])`.
- Delete `shared.db.interface.augment_raw_files_with_metrics` (no callers left).

**Behaviour change, state it in the PR**: "latest" becomes newest per type. Today the shared
function returns the oldest per type, so REST and the pump alert change their result for
re-quanted files.

**Testing**: `pytest shared rest_api monitoring/tests/alerts`; on the stack: re-quant one
file with the same settings, query `/raw_files/?name_contains=<id>` and the MCP tool, both
must show the newer metrics; pump alert log unchanged otherwise.

**Exit**: `mcp_server/server.py` off the exemption list. The pump pressure alert stays on it
until slice 5 for the `KrakenStatus` type hint, like the slice 2 alerts; the `KrakenStatus`
dataclass is not needed before slice 5 either, `instrument_ids()` returns strings.

### Slice 5: status and the remaining alerts, monitoring off mongoengine (2 days)

**Business value**: the whole monitoring process is fenced, including the lease and its
fail-open rule, and catches domain errors only.

**Technical scope**: `shared/db/{status,alerts,users,alerting_leases}.py` and backends,
`shared/db/raw_files.py` (`latest_per_instrument`, `count_by_status`), `monitoring/**`.

**Implementation details**:
- `status.find()`, `status.update(...)` (today `update_kraken_status`, two Airflow callers
  move too, signature unchanged).
- `raw_files.latest_per_instrument(n, created_after)` for the queue stop alert (replaces
  its `distinct` plus per-instrument queries), `raw_files.find(RawFileFilter(ids=...))`
  for its sizes; `raw_files.count_by_status(instrument_id, statuses_not)` for the status
  pile-up alert, as an aggregation pipeline, not `item_frequencies` (mapReduce, deprecated
  since MongoDB 5.0).
- `AlertIssue`, `AlertSuppression`, `User` dataclasses; `alerts.find_issues/add_issue/
  set_issue_notified(issue_id, at)`, `alerts.find_suppressions(active_only)`,
  `users.find(include_inactive)`, `users.project_owner(raw_file_id)`.
- `alerting_leases.try_acquire(name, holder, ttl, now) -> bool`, raising `DatabaseError`;
  `ActiveInstanceElection` keeps the fail-open rule by catching it. Delete the pymongo code
  in `active_instance.py`.
- `monitoring/main.py` catches `DatabaseUnavailableError` from `shared.db.ping()` instead of
  `pymongo.errors.ConnectionFailure`.
- `base_alert.py`, `alert_manager.py`, `recipients.py`, `suppressions.py` type-hint the
  dataclasses; `ensure_utc` calls removed where the value now comes from a mapper.

**Testing**: `pytest monitoring shared`; on the stack: stop the DB container for one loop,
the monitoring must send one "db_connection_error" message (the ping fails before the
election, so no "acting as active") and keep the last suppressions; restart it, the next
loop must run the DB-backed checks; add a suppression in the webapp and see the next loop
mute the issue. Revoke the alerting role's write access to the lease collection for one
loop to see "acting as active".

**Exit**: `monitoring/**` off the exemption list. `monitoring` is added to the unit-tests
job as a run of its own, with its requirements. `mcp_server` has no tests.

Slices 6, 8 and 9 are split (2026-11-10): slice 5 touched 58 files, and these three were
planned as large or larger. Each part is one PR.

### Slice 6a: webapp status page and throughput (0.5 day)

**Business value**: the status page reads dataclasses only.

**Technical scope**: `webapp/service/{db,status}.py`, `webapp/pages_/status.py`.

**Implementation details**:
- `get_status_data` on `status.find()`, `get_raw_files_for_throughput_per_day` on
  `raw_files.find` (needs the `statuses` filter, exists); `df_from_db_data` takes
  dataclasses for these callers.
- `_id` becomes `id` in `webapp/service/status.py`.

**Testing**: `pytest webapp`; on the stack: the status page with its throughput charts.
Compare the status page render time with the value recorded in slice 2 (design 4.2).

**Exit**: `webapp/pages_/status.py` off the exemption list (`webapp/pages_/**` becomes an
explicit file list).

### Slice 6b: webapp overview page (1.5 days)

**Business value**: the overview reads dataclasses; the per-type
`filter_dict`/`drop_duplicates` merge and the `_id` column disappear from the webapp.

**Technical scope**: `webapp/service/{db,data_handling,components}.py`,
`webapp/pages_/overview.py`, `webapp/pages_/impl/overview_*`.

**Implementation details**:
- `get_raw_file_and_metrics_data` becomes `raw_files.find(RawFileFilter(ids | created_after,
  instrument_ids))` plus `metrics.latest_by_raw_file`; the DataFrame is built from
  `vars(row)`; `_merge_metrics_by_type` builds from the dict of dicts.
  `latest_by_raw_file` gains `created_after` here, for its first caller; `types` has none.
- `get_full_raw_file_data` (`include_file_info=True`) on the new API; `get_output_folders`
  on `metrics.find(ids, fields=[])`: it needs every metrics entry of a file, not the latest
  per type (design: `output_locations`). `latest_by_raw_file` is built on `find`.
- `_id` becomes `id` in `components.py`, `overview_plotting.py`, `data_handling.py` (merge
  keys, index).
- The webapp's `msqc_` prefix stripping and `quanting_time_elapsed` rename stay in the
  webapp: moving them into the legacy key map would change REST and MCP keys for old files.
- Delete `connect_db` and the model imports from `webapp/service/db.py`; what remains there
  are DataFrame helpers.

**Behaviour change**: none for the metrics, the overview already kept the newest per type
(`df_from_db_data` sorts by `created_at_`). The raw file id column is `id`, in the table,
the CSV download and `columns_config.yaml`.

**Testing**: `pytest webapp`; on the stack: overview with and without `max_age`, with a
`raw_file_ids` query param, the baseline selection, the download of full raw file data;
compare row counts with the current version.

**Exit**: `webapp/service/data_handling.py` off the exemption list. `webapp/service/db.py`
stays until slice 8b, which ports its project and settings queries.

### Slice 7: projects and settings read side, REST off mongoengine (1.5 days)

**Business value**: projects, settings and assignments exist as dataclasses with
`unique_name` as the settings id; the REST API is fully fenced.

**Technical scope**: `shared/domain/{projects,settings}.py`, `shared/db/{projects,settings,
project_settings}.py` and backends, `rest_api/{service,main}.py`.

**Implementation details**:
- `Project` (`owner_email`), `Settings` (`id = unique_name`), `ProjectSettings`
  (`project_id`, embedded `settings`) dataclasses; `projects.find(ids, include_deleted,
  limit, offset)`, `projects.count`, `settings.find(name, include_deleted, limit, offset)`,
  `settings.count`, `settings.get(unique_name)`, `project_settings.find(project_ids)` (one
  assignments query plus one `in` query for the settings). Only what REST uses:
  `settings.latest_active` and `include_archived` arrive with the webapp in slice 8b.
- `split_unique_settings_name` and its separator move to `shared/domain/settings.py`.
- REST projects and settings endpoints on these; the dicts are `vars()` without unset
  fields. `rest_api/main.py` drops its `connect_db()` call and the lifespan: the backend
  connects on first use. No `ping()` at startup, the API would refuse to start while the
  database is briefly down, which it tolerates today.
- The legacy `get_project_settings`, `get_settings_by_id`, `get_all_settings`,
  `get_latest_active_settings_by_name`, `get_active_project_ids` stay in `interface.py`
  for their webapp and Airflow callers until slice 8; they return documents, as today.
- ❗ Moved to slice 8c: `settings_scope_resolver`, `path_layout`, `display_paths` type-hint
  the dataclasses there, where their first dataclass caller appears. Airflow passes
  documents to them until slice 9b, so the signatures take the document or the dataclass
  in between, as the raw file ones do since slice 4, or `ty` fails on the Airflow callers.

**Testing**: `pytest rest_api shared`; on the stack: `/projects/`, `/projects/{id}`,
`/settings/`, `/settings/{name}_v{n}`, diff the JSON against the current version
(timestamps now carry the UTC offset).

**Exit**: `rest_api/**` off the exemption list.

### Slice 8a: users and alert suppressions, admin page (1 day)

**Business value**: the admin page is fenced; no ObjectId in its page state.

**Technical scope**: `shared/db/{users,alerts}.py` (write functions),
`webapp/pages_/admin.py`, `webapp/pages_/impl/admin_utils.py`, `webapp/service/owner.py`.

**Implementation details**:
- Move `add_user`, `update_user`, `set_user_status`, `add_alert_suppression`,
  `delete_alert_suppression` into `users.py` and `alerts.py`; the last readers of
  `get_all_users` and `get_alert_suppressions` move to `users.find` and
  `alerts.find_suppressions`, and both are deleted from `interface.py`.
- Keyword-only parameters named like the domain fields (design 4.8). Their `ValueError`s
  become `ValidationError([message])`.
- ❗ Invariant checks do not go into `_mongo`: a provider only stores. They live in the
  provider-free API module (`shared/db/users.py`), built on its read functions, marked
  `# moves to shared/services, cf. VALIDATION_DESIGN.md`. Until that move, the field
  constraints of the Mongo models (regex, lengths, ranges) remain the only validation that
  just the provider knows; the validation plan gives them a provider-free home.
- `add_owner_initials`, `show_owner_selectbox` on `users.find`.

**Testing**: `pytest webapp shared`; on the stack: add and edit a user, deactivate one, add
and delete a suppression and see the monitoring honour it.

**Exit**: `webapp/pages_/admin.py`, `webapp/service/owner.py` and their tests off the
exemption list.

### Slice 8b: project and settings writes (1 day)

**Business value**: the write functions live with their aggregates, ready for the
validation refactoring. No change of identifiers yet.

**Technical scope**: `shared/db/{projects,settings,project_settings}.py` (write
functions), `webapp/pages_/{projects,settings}.py`, `webapp/pages_/impl/*`.

**Implementation details**:
- Move `add_project`, `archive_project`, `delete_project`, `create_settings`,
  `archive_settings`, `delete_settings`, `assign_settings_to_project`,
  `check_not_assigned_with_same_filters`, `unassign_settings_from_project` with their
  callers; the settings functions still take the ids the pages pass today. Same rules for
  parameters and `ValidationError` as in 8a.
- Their eight invariant checks (project id taken, archived project, duplicate filters,
  settings still assigned, ...) go into the API modules `shared/db/{projects,settings,
  project_settings}.py`, not into `_mongo`, as in 8a. Where a check needs data, the API
  module reads it through its own find functions; add a read function if one is missing.
- `get_project_data`, `get_settings_data` on `projects.find`, `settings.find`.
- Delete the moved functions from `interface.py`.

**Testing**: `pytest webapp shared`; on the stack: create a project, create settings,
assign with scopes and filters, update the assignment to a new version, archive and delete
both.

**Exit**: no entry leaves the list yet: the two pages still hold ObjectIds and
`ProjectSettings.objects`.

### Slice 8c: settings id = unique_name (1 day)

**Business value**: ObjectIds no longer appear in page state or XCom; the webapp is fully
fenced.

**Technical scope**: `shared/db/{settings,project_settings}.py`,
`webapp/pages_/{projects,settings}.py`, `airflow_src/dags/impl/processor_impl.py`.

**Implementation details**:
- Settings functions take `unique_name`. Pages: `str(row["_id"])`, `str(ps.id)`,
  `str(latest.id)`, `str(s.id)` become the unique name; `ProjectSettings.objects.all()` in
  `settings.py:580` becomes `project_settings.find()`; `ps.project.id` becomes
  `ps.project_id`; `projects.py:92` loop becomes one `project_settings.find()` grouped by
  project.
- `settings_scope_resolver`, `path_layout`, `display_paths` type-hint the dataclasses next to
  the documents (moved here from slice 7); `_unique_by_id` keys on `unique_name`.
- Airflow `processor_impl.py`: `resolve_settings` returns unique names; `prepare_job` calls
  `settings.get(unique_name)`. **Deploy note**: in-flight processor runs carry ObjectIds in
  XCom and fail at `prepare_job` with `NotFoundError`; deploy with the quanting queue
  drained or re-trigger them. No dual-format lookup. This is the only part of slice 8 with
  a deploy constraint, hence its own PR.

**Testing**: `pytest webapp airflow_src shared`; on the stack: the 8b walk-through again,
then trigger a quanting run and check it resolves the settings by name.

**Exit**: `webapp/**` off the exemption list.

### Slice 9a: slurm jobs and metrics writes (0.5 day)

**Business value**: the two small Airflow aggregates are off `interface.py`.

**Technical scope**: `shared/db/{slurm_jobs,metrics}.py`, `airflow_src/dags/impl/{poller,
processor}_impl.py`, `airflow_src/plugins/jobs/slurm_ssh_job_handler.py`.

**Implementation details**:
- `slurm_jobs.upsert_states`, `slurm_jobs.get` with a `SlurmJob` dataclass (the
  `replace(tzinfo=...)` in `slurm_ssh_job_handler.py` goes); `metrics.add(raw_file_id,
  Metrics)`.

**Testing**: `pytest airflow_src shared`; on the stack: one quanting run on the cluster or
the local job engine, its job state and metrics appear.

### Slice 9b: Airflow raw files (1 day)

**Business value**: the last client reads and writes dataclasses.

**Technical scope**: `shared/db/raw_files.py` write functions, `airflow_src/dags/impl/*`,
`airflow_src/plugins/**`, `shared/{path_layout,display_paths,settings_scope_resolver}.py`.

**Implementation details**:
- `raw_files.add/update/delete` (typed `update` with the `NO_UPDATE` sentinel),
  `raw_files.get(raw_file_id, include_file_info)` replacing `get_raw_file_by_id` (13 call
  sites, `NotFoundError` instead of `None`), `raw_files.find(RawFileFilter(original_names=..))`,
  the filter for `get_purge_candidates_by_age`, `include_file_info=True` at the four
  readers (design 4.2).
- `watcher_impl.py:335` catches `AlreadyExistsError`, `processor_impl.py:84`
  `NotFoundError`.
- The transitional document-or-dataclass signatures of the path helpers collapse to the
  dataclass; `get_created_at_year_month` and `parse_file_info_item` move to `shared/domain`.

**Testing**: `pytest airflow_src shared`; on the stack: a full acquisition through watcher,
handler, mover, processor, remover with one file; a second file with the same name
(collision path); the S3 uploader with the local MinIO if configured.

**Exit**: `airflow_src/**` and the three `shared/*.py` modules off the exemption list.

### Slice 9c: fence closed (0.5 day)

**Business value**: `interface.py`, `engine.py` and the top-level `models.py` are gone; a
provider switch now means a new `_mongo`-like package. No logic changes in this PR.

**Technical scope**: `shared/db/`, the ban list in `pyproject.toml`, the migration scripts.

**Implementation details**:
- `shared/db/models.py` moves to `shared/db/_mongo/models.py`, `engine.py` into
  `_mongo/connection.py`; `interface.py` is deleted; `shared/tests/db/test_interface.py`
  is gone by now, its surviving call-side assertions having moved slice by slice to
  `shared/tests/db/test_<aggregate>.py`.
- The three legacy modules leave the ban list; the `shared/db/*.py` exemption stays for
  the API modules.
- The migration scripts under `shared/_migrations` and `airflow_src/plugins/_db_migrations`
  import `shared.db._mongo.models`; they stay exempt.

**Testing**: `pytest shared webapp airflow_src rest_api`, `pytest monitoring`; on the
stack: every service starts and the 9b walk-through passes once more.

**Exit**: exemption list contains only `shared/db/*.py`, `shared/db/_mongo/**`, its tests,
the migration scripts and `celery_config.py`. Done.

### After slice 9

- Design doc section 6 (deferred): real-database test suite in CI, Protocol layer. Not
  scheduled.
- `VALIDATION_DESIGN.md` gets its own plan; its prerequisites are met after slice 8c.

## Order and dependencies

```
1 → 2 → 3 → 4 → 5 → 6a → 6b → 7 → 8a → 8b → 8c → 9a → 9b → 9c
            └──────────────────→ 7 (independent of 5 and 6)
```

Slices 1 to 5 are done. 6a, 7, 8a and 9a are independent of each other and can be taken in
any order. 6b needs 4. 8b needs 7. 8c needs 8b. 9b needs 8c (settings id in Airflow). 9c
is last.

Total: about 15 days, of which about 8.5 remain.

## Risks

- **Real-database behaviour is only checked by the per-slice smoke test** on the local
  stack, as CI has no MongoDB (design 6.0). Each slice names its smoke test; do not merge
  without it.
- **Slice 8c touches the Airflow deploy.** It is cut small so that nothing else rides on
  the drained queue.
- **`shared/tests/db/test_interface.py`** is dismantled across slices 4 to 9c. Its
  call-side assertions move, its return-side assertions on `MagicMock` documents are
  rewritten against documents with real field values (design, tests paragraph).
