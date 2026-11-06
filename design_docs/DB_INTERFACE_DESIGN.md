# Database interface design

Status: proposal. Analysis at commit 87937c8c (2026-11-03).

Goal: one interface between the database and its clients (Airflow, webapp, MCP server, REST
API, monitoring) that hides the storage provider. A switch away from MongoDB is not planned;
the design must make one easier, not harder. So: everything that hides the ORM from clients
is in scope (domain types, mappers, errors, the import ban), everything that only pays off
once a second provider exists is deferred and listed in section 6.

A second refactoring, centralizing the validation of writes, builds on this one; see
`VALIDATION_DESIGN.md`. This design only reserves the places that one fills (section 4.8).

## 1. Current state

`shared/db/` holds three modules: `engine.py` (connection), `models.py` (mongoengine
documents plus all status constants), `interface.py` (~45 functions, written for Airflow).

| Client | Goes through `interface.py` | Direct mongoengine/pymongo use |
|---|---|---|
| Airflow | yes, exclusively | catches `DoesNotExist` (processor_impl.py:84), `NotUniqueError` (watcher_impl.py:335) |
| Webapp | partly (projects, settings, users, suppressions) | `webapp/service/db.py`: 8 query functions on `RawFile`, `Metrics`, `KrakenStatus`, `Project`, `Settings`; pages: `ProjectSettings.objects.all()` (settings.py:580) |
| REST API | `augment_raw_files_with_metrics` only | `rest_api/service.py`: 4 query functions, `.to_mongo().to_dict()`, `_id` renaming |
| MCP | no | `mcp_server/server.py`: 3 queries, own copy of `augment_raw_files_with_metrics` |
| Monitoring | alert issues, suppressions, users | 7 alerts query `RawFile.objects` (`.only`, `.distinct`, `.item_frequencies`); `alert_manager.py:73` `KrakenStatus.objects`; `active_instance.py` raw pymongo; `main.py` catches `pymongo.errors.ConnectionFailure` |

Mongo users per service (docker-compose): Airflow readWrite, webapp `webappRole`,
monitoring `alertingRole`, MCP and REST read-only. The roles already define which client
writes which collection.

## 2. Redundant logic

2.1. **Raw files joined with their latest metrics per type**, three implementations:
`shared/db/interface.py:625` (`augment_raw_files_with_metrics`, used by REST and the pump
pressure alert), `mcp_server/server.py:221` (near-copy), `webapp/service/data_handling.py:152`
(`_merge_metrics_by_type` via `df_from_db_data(drop_duplicates=["raw_file"])`).

2.2. **Legacy metric name harmonization** (`raw:gradient_length_m` -> `gradient_length`,
`msqc_` prefix), five sites: interface.py:658, mcp_server/server.py:268, rest_api/service.py:90,
webapp/service/data_handling.py:20-56, pump_pressure_alert.py:69-72 (field list).

2.3. **"Non-deleted projects/settings"** filter: interface.py `get_all_settings`,
webapp/service/db.py `get_project_data`/`get_settings_data`, rest_api/service.py
`get_projects`/`get_settings`.

2.4. **"Raw files of instrument(s) created after cutoff"**, six variants: MCP
`get_raw_files_for_instrument`, REST `_query_raw_files`, webapp
`get_raw_file_and_metrics_data`, pump_pressure_alert.py:57, queue_stop_alert.py:68,
instrument_stall_alert.py:38.

2.5. **Document to dict**: `to_mongo()` plus `_id` pop/rename in REST
(`_project_to_dict`, `_settings_to_dict`, `_to_raw_file_dict`), MCP, webapp
`df_from_db_data`, interface `augment_raw_files_with_metrics`.

2.6. **Instrument ids from `KrakenStatus.entity_type`**: MCP `get_available_instruments`,
pump_pressure_alert `_get_instrument_ids`, base_alert `get_instrument_id`,
status_pile_up_alert inline, webapp components.

2.7. **`connect_db()`** as first line of every function (77 test patches of it in 10 files).

## 3. Storage details that leak

These are what a provider switch would break.

3.1. `_id` key: 15 sites in webapp pages/components, REST, MCP.

3.2. ObjectId as identity of `Settings`, `ProjectSettings`, `AlertSuppression`:
`str(row["_id"])`, `str(ps.id)` with `# type: ignore[unresolved-attribute]`
(projects.py:191, 235, 258; settings.py:551-621; admin.py:226). REST already invented
`unique_name` (`fallback_v7`) as the public settings id, a sign the ObjectId is not a domain id.

3.3. `QuerySet` as a return type (`webapp/service/db.py` returns QuerySets to pages;
`data_handling.py` caches DataFrames because QuerySets do not pickle).

3.4. mongoengine query DSL at call sites: `__in`, `__gte`, `__icontains`, `__nin`,
`created_at___gte` (audit field naming), `Q`, `.only`, `.exclude`, `.distinct`,
`.item_frequencies`, `.skip/.limit/.count`.

3.5. Lazy `ReferenceField` dereferencing in pages: `ps.settings.name`, `ps.project.id`
(one query per access; projects.py:92 has a TODO "could be single query").

3.6. Provider exceptions in clients: `DoesNotExist`, `NotUniqueError` (Airflow),
`ConnectionFailure`, `DuplicateKeyError`, `PyMongoError` (monitoring),
`mongoengine.ValidationError` shown to webapp users as toast text via `str(e)`.

3.7. pymongo collection access: `active_instance.py:35` (`find_one_and_update`),
interface.py:566 (`_get_collection().bulk_write`).

3.8. `shared/db/models.py` is both the schema and the home of domain constants
(`RawFileStatus`, `TERMINAL_STATUSES`, ...). Every client imports the ORM module to get a
string constant. `path_layout`, `display_paths`, `settings_scope_resolver` type-hint on
mongoengine documents.

3.9. `Metrics` is a `DynamicDocument`: free-form keys per metrics type. `file_info` is a
`DictField` whose tuples come back as lists. Both are schemaless and the hardest part to port.

3.10. Naive UTC datetimes come back from mongoengine; monitoring compensates with
`ensure_utc`, webapp with `to_display_timezone`.

3.11. TTL indexes (`SlurmJob` 7 d, `AlertIssue` 90 d via `init-mongo.sh`) do expiry that the
code relies on silently.

## 4. Target design

Principle: **clients import `shared.domain` and `shared.db` only. Nothing outside
`shared/db/_mongo/` and the migration scripts imports `mongoengine`, `pymongo`, or
`shared.db._mongo`.** Enforced by ruff `TID251` (banned-api), not by convention.

```
webapp / mcp_server / rest_api / monitoring / airflow
        |  reads and writes:  shared.db
        |  types:             shared.domain
        v
   [ shared/services/   reserved for VALIDATION_DESIGN.md: validate, then call shared.db ]
        v
shared/db/raw_files.py  metrics.py  projects.py  settings.py  users.py
          status.py  alerts.py  slurm_jobs.py  leases.py
        |
        v
shared/db/_mongo/   models.py (mongoengine), mappers.py, connection.py
```

Dependency direction: `shared.domain` <- `shared.db` <- clients. `shared.db` is persistence
and must not import `shared.runners` or `shared.yamlsettings`.

Import convention: clients import the module, not the function, `from shared.db import
raw_files` and `raw_files.find(...)`. Tests then patch `shared.db.raw_files.find` regardless
of which client is under test. Today's `from shared.db.interface import get_x` forces a patch
target per client module.

### 4.1 Domain types: `shared/domain/`

Frozen dataclasses, one per aggregate, with `id: str` as the domain identifier.
Status constant classes and `TERMINAL_STATUSES`/`ERROR_STATUSES` move here from `models.py`.

| Type | `id` | Notes |
|---|---|---|
| `RawFile` | file name based id (unchanged) | `file_info: dict[str, FileInfoItem] \| None`, None = not loaded (4.2); tz-aware datetimes |
| `Metrics` | none | `raw_file_id`, `type`, `settings_name`, `settings_version`, `relative_output_path`, `output_display_base_path`, `created_at`, `values: dict[str, Any]` |
| `Project` | project id (unchanged) | `owner_email: str \| None` instead of a reference |
| `Settings` | `unique_name`, e.g. `fallback_v7` | `number` stays a field |
| `ProjectSettings` | opaque string | `project_id: str`, `settings: Settings` (eagerly joined), scope and filter lists |
| `User` | email | |
| `KrakenStatus` | entity id | |
| `AlertIssue` | opaque string | |
| `AlertSuppression` | opaque string | |
| `SlurmJob` | `<cluster>/<job>` (unchanged) | |

"Opaque string" means: generated by the backend, only ever passed back to it. Any provider
can supply one.

`Metrics.values` holds the dynamic keys. Its values are `Any`: `metrics_calculator.py`
stores `None` for missing alphadia metrics and NaN, and lists for arrays. Legacy name
harmonization (2.2) happens exactly once, in `_mongo`: a map from stored key to domain key
(`raw:gradient_length_m` -> `gradient_length`, `msqc_evosep_pump_hp_pressure_max` ->
`evosep_pump_hp_pressure_max`) applied both when mapping documents and when translating a
`fields` projection, so that `fields=["gradient_length"]` also projects the legacy key. If a
document holds both keys, the new key wins; today the legacy one overwrites it
(interface.py:658-666). On a relational provider, `values` is a JSON column.

These are output types. Dataclasses, not pydantic models: the webapp reads thousands of raw
files per page, and validating each on the way out of the DB is wasted work. Conversion to
rows uses `vars(x)`, not `asdict()`, which deep-copies every value. The input types of the
validation refactoring are separate classes (4.8).

### 4.2 Query functions: `shared/db/*.py`

Plain functions, no Protocol/DI layer: there is one provider at a time, and a switch means
rewriting function bodies, not call sites. Add a Protocol only if two providers must coexist
(e.g. dual-write during migration).

Named methods with keyword filters, no generic query builder. The complete raw file surface:

```python
@dataclass(frozen=True)
class RawFileFilter:
    ids: list[str] | None = None
    original_names: list[str] | None = None          # case-insensitive
    instrument_ids: list[str] | None = None
    project_id: str | None = None
    name_contains: str | None = None                 # case-insensitive
    created_after: datetime | None = None
    created_before: datetime | None = None
    updated_after: datetime | None = None
    statuses: list[str] | None = None
    statuses_not: list[str] | None = None
    backup_status: str | None = None
    instrument_file_status: str | None = None
    instrument_file_statuses_not: list[str] | None = None

def find(filter: RawFileFilter, *, order: str = "-created_at", limit: int | None = None,
         offset: int = 0, include_file_info: bool = False) -> list[RawFile]
def count(filter: RawFileFilter) -> int
def get(raw_file_id: str, *, include_file_info: bool = False) -> RawFile   # raises NotFoundError
def latest_per_instrument(n: int, *, created_after: datetime) -> dict[str, list[RawFile]]
def count_by_status(instrument_id: str, *, statuses_not: list[str]) -> dict[str, int]
def add(...) -> str                                  # raises AlreadyExistsError
def update(raw_file_id: str, *, status=NO_UPDATE, ...) -> None   # today's typed signature with the sentinel
def delete(raw_file_id: str) -> None
```

`latest_per_instrument` keys its result by the instruments that have files after the cutoff,
as `queue_stop_alert.py:66` does today via `distinct("instrument_id")` plus a per-instrument
query. Instruments without recent files yield no entry, which is what the alert needs.

`include_file_info` is the only projection on raw files: `file_info` is the one field that
grows without bound (tens of KB for a Bruker folder), the rest of a document is about 1 KB,
and REST already exposes exactly this flag. It defaults to False, so a call site pays only
when it opts in; `file_info` is then `None`, distinguishable from an empty mapping. The
readers that opt in: `handler_impl.py:195` (existing checksums), `s3_uploader_impl.py:135`,
`file_checks.py:74` and `raw_file_wrapper_factory.py:566` via the wrapper's raw file, REST
with its flag. The `.only()` calls in monitoring and webapp are dropped. Projection saves
wire bytes and decoding only; MongoDB reads the whole document either way. With the False
default the status page loads what it does today plus a few short fields, so no measurement
step is needed.

`get` raises `NotFoundError` where `get_raw_file_by_id` returns `None` today. No caller checks for
`None` (13 call sites in Airflow); a missing file fails on the next attribute access, so only
the exception type changes.

`find`/`count` replace: `get_raw_files_by_names`, `get_raw_files_by_instrument_file_status`,
`get_purge_candidates_by_age`, MCP (2 queries), REST `_query_raw_files`, webapp
`get_raw_file_and_metrics_data`/`get_full_raw_file_data`/`get_raw_files_for_status_df`/
`get_raw_files_for_throughput_per_day`, raw_file_error, s3_upload_failure, pump_pressure,
queue_stop (`latest_per_instrument`), instrument_stall (`find(limit=1)`), status_pile_up
(`count_by_status`).

Metrics:

```python
def latest_by_raw_file(raw_file_ids: list[str] | None = None, *,
                       created_after: datetime | None = None,
                       types: list[str] | None = None,
                       fields: list[str] | None = None) -> dict[str, dict[str, Metrics]]
    # raw_file_id -> metrics type -> latest Metrics; the one join implementation (2.1)
    # "latest" = newest `created_at_` per type. Today only the MCP copy does this; see 5.4.
    # fields: keys of `values` to load, None for all; the metadata fields are always loaded.
    # Keeps today's projection of augment_raw_files_with_metrics: metrics documents are
    # wide and open-ended, the pump pressure alert needs four keys of a day's worth.
def add(raw_file_id: str, metrics: Metrics) -> None
def output_locations(raw_file_ids) -> list[Metrics]    # webapp get_output_folders
```

The webapp optimization "fetch metrics by `created_at_` superset instead of sending ids"
stays inside the Mongo implementation, selected by passing `created_after` without ids.

Projects/settings/users/status/alerts/slurm jobs: today's `interface.py` functions, with
these changes:
- settings functions take `unique_name`, not ObjectId strings;
- `get_project_settings` returns `ProjectSettings` with `settings` embedded (one `in` query);
- `find_projects(include_deleted=False, ...)`, `find_settings(...)` with `limit/offset` and
  a `count` replace 2.3;
- `instrument_ids()` in `status.py` replaces 2.6;
- the write functions keep their keyword-only signatures and the invariant checks they
  contain today (archived project, duplicate filters, ...); see 4.8 for what changes later;
- functions that take a document in take an id instead: `set_alert_issue_notified(issue_id,
  notified_at)` (today `issue.update(...)` on the document, interface.py:697),
  `check_not_assigned_with_same_filters(project_id, settings_unique_name, ...)` (today a
  `Settings` document, interface.py:338, called from projects.py:228);
- `get_created_at_year_month` moves from `models.py` to `shared/domain`; `path_layout`,
  `display_paths` and `settings_scope_resolver` type-hint the domain types. They only read
  attributes, so their bodies are unchanged.

`leases.py`: `try_acquire(name: str, holder: str, ttl: timedelta, now: datetime) -> bool`
replaces `active_instance.py`'s pymongo call. It returns False when another holder has the
lease and raises `DatabaseError` on any other error, including permission errors. The
fail-open rule of today's `ActiveInstanceElection` (act as active when the claim cannot be
made, e.g. for missing permissions) stays in the monitoring, which catches `DatabaseError`.
On Postgres: `INSERT ... ON CONFLICT DO UPDATE ... WHERE expires_at < now`.

`slurm_jobs.py`: `upsert_states`, `get` unchanged; the `bulk_write` stays in the Mongo body.

### 4.3 Errors: `shared/domain/errors.py`

`ValidationError(errors: list[str])`, `NotFoundError`, `AlreadyExistsError`, and `DatabaseError` with
the subclass `DatabaseUnavailableError` for connection failures. The Mongo layer translates
`DoesNotExist`, `NotUniqueError`/`DuplicateKeyError`, `mongoengine.ValidationError` (its
per-field messages become the list), `ConnectionFailure`/`ServerSelectionTimeoutError` to
`DatabaseUnavailableError`, and every other `PyMongoError` (`OperationFailure` for auth and
permissions among them) to `DatabaseError`. Clients catch only these: the startup checks
in `monitoring/main.py` catch `DatabaseUnavailableError`, the lease caller `DatabaseError`. The
`ValueError`s raised by today's invariant checks become `ValidationError` with one entry.

### 4.4 Connection

Lazy and invisible: every function in `_mongo/` calls `_ensure_connected()`, which calls
`mongoengine.connect()` once and remembers it in a module flag. Today's `connect_db` is
re-entrant only because it swallows the `ConnectionFailure` that a second `connect()` on
alias `default` raises (engine.py:50-56). Clients never call `connect_db`;
`rest_api/main.py` lifespan and `monitoring/main.py` call `shared.db.ping()` for their
startup checks.

### 4.5 Datetimes

The mapper returns tz-aware UTC datetimes. The write side changes first, in its own step
(5.2): the models' `default=datetime.now` (naive local time, correct only because every
writer runs in a UTC container) becomes `datetime.now(UTC)`; pymongo stores aware datetimes
as UTC.
`ensure_utc` call sites in monitoring go away. `to_display_timezone` in the webapp stays
(display concern).

### 4.6 Webapp DataFrames

`df_from_db_data(QuerySet)` becomes `pd.DataFrame([vars(x) for x in rows])`; the column is
`id`, not `_id`. `_merge_metrics_by_type` builds from `metrics.latest_by_raw_file`, which also
removes the per-type `filter_dict`/`drop_duplicates` dance. The `st.cache_data` note about
QuerySets not pickling becomes moot.

### 4.7 Enforcement

`pyproject.toml`:

```toml
[tool.ruff.lint.flake8-tidy-imports.banned-api]
"mongoengine".msg = "use shared.db"
"pymongo".msg = "use shared.db"
"shared.db._mongo".msg = "use shared.db"
[tool.ruff.lint.per-file-ignores]
"shared/db/*.py" = ["TID251"]      # the API modules are the only importers of the backend
"shared/db/_mongo/**" = ["TID251"]
"shared/tests/db/**" = ["TID251"]
"shared/_migrations/**" = ["TID251"]
"airflow_src/plugins/_db_migrations/**" = ["TID251"]
```

Until `interface.py`, `engine.py` and the top-level `models.py` are gone (5.8), the ban
also names them, so that every legacy client appears on the exemption list and the list
measures what is left; cf. `DB_INTERFACE_PLAN.md`.

### 4.8 What this design reserves for the validation refactoring

`VALIDATION_DESIGN.md` adds `shared/services/` and pydantic input types. So that it changes
nothing here when it lands:

- `shared.db` write functions get no new validation. The invariant checks they hold today
  are marked with a comment that they move to `shared/services`. The field constraints of the
  Mongo models (regex, max_length) stay as they are; they become a backstop later.
- `shared.db` write functions use keyword-only, typed parameters named like the fields of the
  domain types; `update` keeps its `NO_UPDATE` sentinel. The later input types use the same
  names, so `create(**new.model_dump())` or a signature change to `create(new: NewSettings)`
  is mechanical.
- `shared/domain/` is a package, one module per aggregate (`raw_files.py`, `settings.py`,
  ...), so the input types land next to the output types. `shared/domain/errors.py` already
  defines `ValidationError(errors: list[str])`.
- `shared.db` imports nothing from `shared.runners`, `shared.yamlsettings`,
  `shared.validation`, `shared.config_params`. The services layer will.
- The webapp-side validators (`_check_project_id`, `validate_suppression`,
  `settings_validation.py`, `user_input.py`) are left in place and untouched.

## 5. Migration steps

Each step is a shippable PR; no compatibility shims are kept between steps.

5.1. Split `interface.py` into the per-aggregate modules of 4.2 and move the constants from
`models.py` to `shared/domain/`; fix imports. Mechanical, bodies unchanged. Every later PR
then touches one module.

5.2. Datetimes (4.5): tz-aware on the write side (`datetime.now(UTC)` defaults) and, for
now, `ensure_utc` applied in `interface.py` on the read side. Its own step so that a tz
regression is bisectable; it touches many test fixtures (7.5).

5.3. Add domain dataclasses, `shared/domain/errors.py` and `_mongo/mappers.py`. Make the
per-aggregate functions return domain objects and raise domain errors. The mapper already
embeds `Settings` into `ProjectSettings` and sets `project_id`, so that `ps.settings.name`
keeps working in the webapp pages and `settings_scope_resolver`; the `ps.project.id` sites
(settings.py:585, rest_api/service.py:220) become `ps.project_id` in this step, and the
two document-taking signatures of 4.2 change with their callers (alert_manager.py,
projects.py:228). Airflow keeps working: it only reads attributes, and `Settings.id` is
still the ObjectId string until 5.6. The two exception imports (3.6) change.

5.4. `metrics.latest_by_raw_file` with harmonization. Port REST, MCP, pump pressure alert,
webapp `data_handling`. Delete the MCP copy and `augment_raw_files_with_metrics`. REST
flattens `{**metadata, **values}` back into its `metrics: list[dict]` entries, so the payload
stays identical apart from one behaviour change: "latest" becomes newest per type
everywhere. Today `augment_raw_files_with_metrics` returns the oldest per type, because it
iterates newest-first and each document overwrites the previous one (interface.py:645-679);
the MCP copy returns the newest (server.py:262); the webapp keeps natural order
(`webapp/service/db.py:147`, then `drop_duplicates`). REST `/raw_files` and the pump pressure
alert change their result for re-quanted files.

5.5. `raw_files.find/count/...`. Port `webapp/service/db.py`, REST, MCP, six alerts. Delete
the raw file queries from `webapp/service/db.py` (keep the DataFrame helpers).

5.6. Settings id = `unique_name`. Port webapp pages (drop `ProjectSettings.objects.all()`,
all `_id`), REST, and Airflow: `processor_impl.py:101` puts `str(settings.id)` into XCom and
`prepare_job` reads it back with `get_settings_by_id`. In-flight processor DAG runs across
this deploy carry ObjectIds in XCom and fail at `prepare_job` with `NotFoundError`. Deploy with
the quanting queue drained, or re-trigger the failed runs; no dual-format lookup.

5.7. `leases.py`, hidden connection. Delete `active_instance.py`'s pymongo code and all
client-side `connect_db` calls.

5.8. Enable the ruff ban (4.7).

Tests: client tests patch `shared.db.<module>.<function>` and construct dataclasses instead
of `MagicMock` documents. `_mongo/` keeps today's test style, `MagicMock` querysets, and
`shared/tests/db/test_interface.py` (1754 lines) is split along 5.1. Its call-side
assertions (which filter, update, save was called with what) survive. Its return-side
assertions (`assert result == [file1, file2]` on MagicMock documents, 21 of them, backed by
127 `return_value`s) break in 5.3, because the result is now mapped: they are rewritten to
build documents with real field values and compare against the expected dataclasses. The
mappers do not accept MagicMocks. New tests are written for the functions whose bodies
change (`find`, `latest_by_raw_file`, `latest_per_instrument`, `count_by_status`, the
mappers). No MongoDB service in CI for now; see 6.0.

## 6. Deferred until a switch is planned

Nothing here is needed for this refactoring. It is what a switch would add on top, listed so
that the design above does not make any of it harder.

6.0. A provider-agnostic test suite against the `shared.db` API on a real database: a
MongoDB service container in the `unit-tests` job of `branch-checks.yaml` (none today), a
conftest creating and dropping a database per session, the `MagicMock` tests replaced. This
is the first step of an actual switch, because it is what validates the second provider.
Writing it now would be the largest cost of the plan for no current benefit.

6.1. `Metrics.values` and `file_info` need a JSON column or an EAV table. Filtering by
metric value (none today) would need JSON operators.

6.2. TTL expiry (3.11) becomes a backend job; document it as a responsibility of
`slurm_jobs`/`alerts` in the new provider.

6.3. `Settings.number = count() + 1` (interface.py:403) is a race on every provider;
a sequence fixes it in SQL. `number` is exposed by the REST schema only and shown nowhere in
the webapp; `(name, version)` is the working unique key.

6.4. The per-collection Mongo roles (`init-mongo.sh`) must be recreated as grants.

6.5. A Protocol per `shared.db` module and a backend selector, only if both providers must
run at once (dual-write during data migration).

## 7. Decisions to confirm

7.0. Decided: the switch is not planned, the design must make one easier, not harder. The
domain types and mappers stay in scope: they are what keeps mongoengine documents, ObjectIds
and lazy references out of the clients (3.1 to 3.6), which a lighter design (query modules
inside `shared/db`, documents as return types) would not. The real-database test suite is
deferred (6.0).

7.1. Settings public id = `unique_name` everywhere (webapp too), ObjectId never leaves
`_mongo/`.

7.2. Functions per aggregate, no Protocol/DI layer (4.2).

7.3. Dataclasses for the output types (4.1).

7.4. Projections: `include_file_info` on raw files, default False, `fields` on metrics,
nothing else (4.2).

7.5. Tz-aware datetimes at the boundary (4.5): touches many test fixtures.
