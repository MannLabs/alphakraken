# Spec: multiple settings per software type

## Context

`resolve_scoped_settings` keeps exactly **one** settings per `software_type` per raw file
(`shared/settings_scope_resolver.py:79-89`). `software_type` is doing three unrelated jobs at once:

1. the **override key** for scoping (`*` < vendor < instrument),
2. the **output namespace** (`out_<file>/<software_type>`, `shared/path_layout.py:46`),
3. the **job identity** (docker container name, `docker_job_handler.py:105`).

For `alphadia` these coincide. For `custom` they do not: two different custom pipelines (e.g.
`diann_adiama` and `peaks_zenotof_dia`) cannot run on the same raw file — the second is silently
dropped at resolution time.

Inspection of two real projects showed that scope is used to **partition** (mutually exclusive
vendor scopes), never to **override**. The override semantics that the dedup exists to provide are
unused, and are replaced here by explicit exclusions — which are strictly more expressive, because
they also work along the file-name axis, which scoping cannot express at all today.

**Outcome:** any number of settings can apply to one raw file; what applies is stated explicitly
through include/exclude scopes and include/exclude file-name filters; and the output directory and
job identity are namespaced by settings instead of by software type.

## Scope

**In:** resolution, the `ProjectSettings` model, output paths, container names, the assignment API,
the projects page, the migration.

**Out:** the webapp's metrics columns. `webapp/service/data_handling.py:_merge_metrics_by_type`
stays exactly as it is. It flattens a one-to-many into one row per raw file via a `__`-joined
column prefix, and the same flattening is implemented two more times with two more collapse rules
(`shared/db/interface.py:434-459`, `mcp_server/server.py:262-271`). Making that carry a settings
identity means encoding a relation in a column name, which is a patch on a design that should be
reworked on its own terms — see *Known limitations* and the follow-up note at the end.

## Design

### Resolution (`shared/settings_scope_resolver.py`)

`ScopeLevel` and the per-`software_type` dedup are deleted. The resolver becomes a filter:

```
for ps in project_settings:
    if not _scopes_match(ps.scopes, instrument_id, instrument_type):       continue
    if _scopes_match(ps.excluded_scopes, instrument_id, instrument_type):  continue
    if not _name_included(ps.raw_file_id_filter, raw_file_id):             continue
    if _name_excluded(ps.raw_file_id_exclude_filter, raw_file_id):         continue
    result.append(ps.settings)
return _unique_by_id(result)
```

- `_scopes_match` is today's `_classify_scope` reduced to a bool and applied over a list: `*`
  matches everything, a vendor name matches when it equals `instrument_type`, anything else matches
  when it equals `instrument_id`. The same helper serves the include and the exclude list.
- Exclude beats include, on both axes.
- File-name filters are lists of substrings: include = matches **any** entry (empty list = matches
  all), exclude = matches **no** entry. Case sensitive, as today.
- `raw_file_id=None` (webapp preview) keeps its current meaning: **both** file-name filters are
  ignored, so the preview shows everything that could apply.
- `_unique_by_id` is a safety net only — the same `Settings` document matched twice would otherwise
  produce two jobs writing the same output dir.
- Keep the per-assignment `logging.info` lines. They are the only debugging aid for "why did this
  settings not run".

### Data model (`shared/db/models.py:291-302`)

| now | after |
|---|---|
| `scope = StringField(default="*")` | `scopes = ListField(StringField(max_length=64), default=lambda: [DEFAULT_SCOPE])` |
| `excluded = ListField(...)` — instrument IDs only | `excluded_scopes = ListField(...)` — instrument IDs **and** vendor names |
| `raw_file_id_filter = StringField(default="")` | `raw_file_id_filter = ListField(StringField(max_length=128), default=list)` |
| — | `raw_file_id_exclude_filter = ListField(StringField(max_length=128), default=list)` |

### Output layout (`shared/path_layout.py:26-48`)

Last segment changes from `<software_type>` to `<settings_name>_v<version>`:

```
out_RAW-1.raw/fallback_v7/
out_RAW-1.raw/diann_adiama_v2/
```

`get_output_folder_rel_path(raw_file, software_type=None)` becomes
`get_output_folder_rel_path(raw_file, settings=None)`.

**`airflow_src/plugins/common/paths.py:41-47` must change in lockstep.**
`processor_impl.py:129-130` uses `get_internal_output_path_for_raw_file` for the `.runN` existence
probe while `:180-182` uses `get_output_folder_rel_path` for the path the job actually writes. If
only one is updated, the probe checks a different directory than the job writes to, and
`submit_job` (`:362-380`) either raises "already exists" or silently overwrites.
`airflow_src/plugins/jobs/_experimental/file_based_job_handler.py:91-93` hard-codes
`software_type=SoftwareTypes.CUSTOM` in the same call and must be updated too.

Historic data is untouched: `Metrics.relative_output_path` (`shared/db/models.py:203`) stores the
path per document and `shared/display_paths.py:get_display_output_path` renders what is stored. Old
files keep pointing at `.../alphadia/`, new ones at `.../fallback_v7/`. **No output migration.**

The `.runN` logic (`processor_impl.py:145-166`) stays — re-running the *same* settings version on
the same file still collides; only the cross-settings collision goes away. Two behaviour shifts to
put in the release note: re-quanting with a *newer settings version* now lands in a fresh folder
instead of tripping `OUTPUT_EXISTS_MODE`, so that variable's meaning narrows from "this file was
already quanted" to "this exact settings already ran on this file"; and `add`-mode folder names
change from `alphadia.run2` to `fallback_v7.run2`.

### Job identity (`airflow_src/plugins/jobs/docker_job_handler.py:105`)

`kraken-<software_type>-<raw_file_id>` → `kraken-<settings_name>-v<version>-<raw_file_id>`, still
through `_to_container_name` (`:242`). Without this, `_remove_container` kills the sibling job.
`JOB_LABEL` (`:144`) is the raw file ID and is now non-unique across concurrent containers — add the
settings identity to the label as well, so the prune-by-label procedure in `docs/deployment.md:393`
stays usable.

**Do not change `QuantingEnv`.** It is `extra="forbid"` (`quanting_env.py:15`), it already carries
`settings_name` and `settings_version` (`:40-41`), and `software_type` (`:29`) is still read by
`check_job_result` (`processor_impl.py:486`). Adding or removing a field breaks in-flight XComs.

### Assignment API (`shared/db/interface.py:237-274`)

- `scope: str` → `scopes: list[str]`; `excluded` → `excluded_scopes`; `raw_file_id_filter` becomes a
  list; new `raw_file_id_exclude_filter`.
- The duplicate-`software_type`-per-scope rejection (`:253-262`) is deleted — that is the feature.
  Note `:253` is a mongoengine query **on the field name**; after the rename it raises
  `InvalidQueryError` rather than returning nothing, so it cannot be left behind by accident.
- Replacement guard: reject assigning the same `Settings` document to the same project twice with
  identical file-name filters. With `scopes` a list, "same settings on thermo and on sciex" is one
  assignment, not two.

### Webapp (`webapp/pages_/projects.py`)

- `:223-229` scope `selectbox` → `multiselect`, default `["*"]`, same `SCOPE_OPTIONS` (`:40`).
- `:231-236` "Exclude instruments from scope" → "Exclude scopes", options `SCOPE_OPTIONS` minus `*`.
- `:238-242` include filter becomes multi-entry; new sibling input for the exclude filter.
- `:145-155` listing renders `scopes` joined and both filters.
- `:176-201` the "upgrade to latest version" button re-assigns with the new field names.
- `:91` and `:344` help texts: drop "A certain software type can only be assigned once for a scope"
  and the precedence sentence; document exclude-beats-include and that everything matching runs.
- `webapp/pages_/impl/projects_utils.py:20-35`: the grouping by `raw_file_id_filter` was a
  workaround for the dedup — replace with a single `resolve_scoped_settings` call.
- `projects_utils.py`: **warn in the resolved-settings preview** when an instrument resolves more
  than one settings of the same `software_type`. This is legal now, but it is also what a forgotten
  exclude filter looks like, it doubles cluster load, and — until the display rework — the second
  one's metrics are invisible in the overview. Configuration time is the only place to catch it.

### Migration and rollout

**Strict cutover, in a maintenance window:** stop the schedulers → run the migration → deploy →
restart. There is no compatibility read, so the order is not optional. Deploying first leaves
`scopes` empty on every un-migrated assignment, which makes `_is_settings_configured`
(`handler_impl.py:429-444`) return False and files terminate as `DONE_NOT_QUANTED` — a status in
`TERMINAL_STATUSES` (`models.py:84-89`), so those files are never retried and nothing alerts.

One script `shared/_migrations/from_0.10.0/_migrate_project_settings_scopes.py`, following
`_migrate_job_engine_to_runner.py`: usage block in the docstring, `argparse` with `--dry-run`, raw
pymongo via `ProjectSettings._get_collection()`, validate the whole batch before the first write,
`[DRY RUN]` log prefixes, final summary. Idempotent — skip documents that already have `scopes`.

```
{"$set": {"scopes": [doc["scope"]],
          "excluded_scopes": doc.get("excluded", []),
          "raw_file_id_filter": [f] if (f := doc.get("raw_file_id_filter")) else [],
          "raw_file_id_exclude_filter": []},
 "$unset": {"scope": "", "excluded": ""}}
```

`--dry-run` must additionally print a **co-firing report**: per project and instrument, which
assignments would newly resolve together after the semantics change. The migration cannot infer
intent, and any project that did rely on override silently doubles its quanting load on the first
file after deploy. This report is how that gets caught beforehand.

`shared/_migrations/from_0.8.0/_migrate_project_settings_to_mn.py:67,72` constructs and queries
`scope=` and will break on the rename. Leave a comment pinning it to the pre-1.0 schema so it is not
re-run blindly.

Folder choice: `from_0.10.0`, because `v0.10.0` is the last tag and `1.0.0` is unreleased. Move to
`from_1.0.0/` if 1.0.0 ships first.

## Known limitations shipped deliberately

1. **Metrics display.** Two settings producing the same `metrics_type` for one raw file still means
   only one is visible in the overview (`data_handling.py:150`, `drop_duplicates(["raw_file"])`).
   The feature works; half of it is invisible until the display rework. The projects-page warning
   above is the mitigation. Unchanged from today's behaviour, so nothing regresses.
2. **Capacity.** The job sensors run in poke mode and hold a `CLUSTER_SLOTS_POOL` slot plus one of
   `MAXNO_JOB_MONITOR_TASKS_PER_DAG = 14` (`settings.py:41`) for the whole job
   (`acquisition_processor.py:102-121`). One raw file now consumes N of those 14 instead of 1 — at
   N=3, roughly 4 files in flight per instrument instead of 14. Release note only; sensor `mode` and
   pool sizing are tuned separately once real N values are known. Also note N× declared memory on
   the Airflow host for the docker runner (`docker_job_handler.py:150-153`), and that
   `STATUS_PILE_UP_THRESHOLDS["quanting"] = 10` (`monitoring/alerts/config.py:38`) counts files, so
   alert sensitivity effectively rises.
3. **Status details truncation.** `_build_status_details` caps at 1024 chars
   (`processor_impl.py:565,671-677`), matching `RawFile.status_details`. With N branches each
   contributing `[settings_name] error`, truncation becomes routine and the last branches' errors
   are the ones lost.
4. **Two pre-existing bugs**, written to a BOYSCOUT report rather than fixed here:
   `augment_raw_files_with_metrics` (`interface.py:434-459`) iterates `-created_at_` and overwrites,
   so the **oldest** metrics doc wins though the docstring says "latest" —
   `shared/tests/db/test_interface.py:964` asserts the buggy behaviour, and it feeds
   `rest_api/service.py:141` and `monitoring/alerts/pump_pressure_alert.py:101`; and
   `mcp_server/server.py:262-271` keeps one doc per type and drops the rest. Both become routinely
   reachable once this ships. (Append to the existing `BOYSCOUT_20260915_131303.md`.)

## Files to change

| file | change |
|---|---|
| `shared/settings_scope_resolver.py` | rewrite: drop `ScopeLevel` + dedup, list scopes, exclude filters |
| `shared/db/models.py` | `ProjectSettings` fields |
| `shared/db/interface.py` | `assign_settings_to_project` signature + guard |
| `shared/path_layout.py` | output segment `<name>_v<version>` |
| `airflow_src/plugins/common/paths.py` | `get_internal_output_path_for_raw_file` param — lockstep |
| `airflow_src/dags/impl/processor_impl.py` | call sites `:130,180-181` |
| `airflow_src/plugins/jobs/docker_job_handler.py` | container name `:105`, job label `:144` |
| `airflow_src/plugins/jobs/_experimental/file_based_job_handler.py` | hard-coded `software_type` `:91-93` |
| `webapp/pages_/projects.py` | widgets, listing, help texts `:91,:344` |
| `webapp/pages_/impl/projects_utils.py` | drop grouping workaround, add same-software-type warning |
| `shared/_migrations/from_0.10.0/_migrate_project_settings_scopes.py` | new, incl. co-firing report |
| `shared/_migrations/from_0.8.0/_migrate_project_settings_to_mn.py` | comment pinning it to the old schema |
| `docs/upgrade_from_0.10.0.md` | migration entry, checklist row, cutover procedure, capacity note |
| `design_docs/PATH_HANDLING_DESIGN.md` | `:22` output layout |

## Verification

1. `python -m pytest shared webapp`, and `python -m pytest airflow_src` with Airflow installed.
   Tests that must change:
   - `shared/tests/test_settings_scope_resolver.py` — near-total rewrite. The `ScopeLevel` import
     (`:6`), all `_classify_scope` tests (`:37-67`) and every "replaces/overridden" test
     (`:103,116,130,187,273,288`) encode the dedup being deleted; `_make_ps` (`:13-22`) sets the old
     fields.
   - `shared/tests/db/test_interface.py:445-446,457-510,537` — kwargs and the rejection test.
   - `shared/tests/test_path_layout.py:48-58`, `shared/tests/test_runners.py:207`.
   - `airflow_src/tests/plugins/jobs/test_docker_job_handler.py:120,146,251` — container name.
   - `airflow_src/tests/dags/impl/test_processor_impl.py` — `:58,127,836` (patches), `:876`
     (`"...alphadia.run2"`), `:207-238,273-293` (resolver mocks), plus the `:1026-1294` block.
   - `airflow_src/tests/dags/impl/test_handler_impl.py:1393,1416`.
   - `airflow_src/tests/conftest.py:17-19` — `_QUANTING_ENV_DEFAULTS` hard-codes
     `"PID1/out_test_file.raw/alphadia"`; every `make_quanting_env` user inherits the old layout.
   - `webapp/tests/pages_/test_projects.py` — `mock_ps1` is a bare `MagicMock`; `scopes` /
     `excluded_scopes` need real list values or the listing f-string misbehaves.
   - New: `shared/tests/_migrations/from_0.10.0/test_migrate_project_settings_scopes.py`, using the
     `importlib.util.spec_from_file_location` idiom of the existing migration tests.
   - `airflow_src/tests/dags/test_dags.py:73-77` asserts `len(dag.tasks) == 9` — should stay green;
     if it fails, the DAG shape changed unintentionally.
2. Migration dry run against a copy of the sandbox DB, and **read the co-firing report** before
   going further:
   `PYTHONPATH=. python shared/_migrations/from_0.10.0/_migrate_project_settings_scopes.py --dry-run`
3. End-to-end locally (`./compose.sh`): assign two `custom` settings with `scopes=["*"]` to one
   project, drop a raw file, and confirm two quanting jobs run, two output dirs appear
   (`out_<file>/<name>_v<version>`), two containers with distinct names exist, and two `Metrics`
   documents are written. Exactly one of them showing in the overview is the expected, documented
   behaviour.
4. Regression against the two real projects that motivated this: one assignment with
   `scopes=["thermo","sciex"]` and `excluded_scopes=["stellar1"]` must reproduce the old per-vendor
   resolution in the projects-page preview, instrument by instrument.
5. Keep the `output_exists_mode` Airflow variable at `raise` across the deploy window, so any
   path-layout surprise fails loudly instead of overwriting.

## Follow-up (separate spec, not this change)

Reshape the metrics display to one row per (raw file × settings). That deletes the `__` column
prefix, `expand_columns` (`overview_utils.py:52-98`) and all three collapse implementations, and
makes `settings_name` a column value instead of part of a column name. It removes code rather than
adding it, and it is the reason the metrics-column question has no good answer today.
