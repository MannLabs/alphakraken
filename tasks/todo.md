# Tasks: Named runners (D3-lite)

Spec refs are to `SPEC.md`. Every task: one commit, `pre-commit run --all-files` clean,
`pytest shared`, `pytest webapp`, `pytest airflow_src --ignore=airflow_src/tests/plugins/jobs/test_docker_job_handler.py` green.

## Phase 1: Foundations (independent slices)

### Task 1: `DOCKER_HOST_VIEW` from the `MOUNTS_PATH` environment variable

**Description:** `EnvVars.MOUNTS_PATH`; `_build_docker_host_view` reads it, unset yields an empty
view. `docker-compose.yaml` passes `MOUNTS_PATH` through `airflow-common-env`. The three test
conftests set `MOUNTS_PATH` next to `ENV_NAME`; the `_test_` stub loses `general.mounts_path`.
Factory error text and `docker_job_handler.py` module docstring name the variable instead of the
yaml key. `envs/*.env` comment: must be absolute when the docker runner is used. The yaml key
`locations.general.mounts_path` stays, unread, until T10. Spec 1.2.7, 7.8.

**Acceptance criteria:**
- [x] `MOUNTS_PATH=/m` -> `DOCKER_HOST_VIEW.resolve(OUTPUT, "P1/x") == PurePosixPath("/m/output/P1/x")`; unset -> `has(OUTPUT)` is `False` (7.8).
- [x] Docker factory branch with empty view raises naming `MOUNTS_PATH`.
- [x] `grep -rn mounts_path shared airflow_src docker-compose.yaml` returns nothing.

**Verification:**
- [x] `pytest shared/tests/test_path_views.py airflow_src/tests/plugins/jobs/test_job_handler.py`
- [x] All three suites (conftests changed); `docker compose config` shows `MOUNTS_PATH` in an airflow service's environment (checked via the yaml anchors, `docker compose` not runnable in the sandbox).

**Dependencies:** None
**Files:** `shared/keys.py`, `shared/path_views.py`, `shared/yamlsettings.py` (stub), `docker-compose.yaml`, `envs/{local,sandbox,production}.env`, `airflow_src/plugins/jobs/job_handler.py`, `airflow_src/plugins/jobs/docker_job_handler.py` (docstring), `{shared,airflow_src,webapp}/tests/conftest.py`, `shared/tests/test_path_views.py`, `airflow_src/tests/plugins/jobs/test_job_handler.py`
**Scope:** M by change size, L by file count (one to three lines each)

### Task 2: `backup.backup_base_path` replaces `CLUSTER_VIEW` in `get_backup_base_path`

**Description:** `YamlKeys.Backup.BACKUP_BASE_PATH`; module constant `BACKUP_BASE_PATH` in
`yamlsettings.py`, read at import through a small function that raises `KeyError` naming the key.
Three yamls and the `_test_` stub gain `backup.backup_base_path` = former
`locations.backup.absolute_path` (stub: `./tmp/test/backup`). `get_backup_base_path` =
`PurePosixPath(BACKUP_BASE_PATH) / get_raw_file_folder_rel_path(raw_file)`; line 68 comment
reworded. Consistency test: key present in every in-repo yaml. Spec 1.2.3, 2.2, 7.3 second half.

**Acceptance criteria:**
- [x] `get_backup_base_path` returns the same string as before for the same yaml values (regression test with patched `BACKUP_BASE_PATH`).
- [x] The reader function raises naming `backup.backup_base_path` on a dict without the key.
- [x] `handler_impl.py` no longer imports `CLUSTER_VIEW`.

**Verification:**
- [x] `pytest shared/tests/test_yamlsettings.py shared/tests/test_deployment_paths.py airflow_src/tests/dags/impl/test_handler_impl.py`
- [x] All three suites

**Dependencies:** None
**Files:** `envs/alphakraken.{local,sandbox,production}.yaml`, `shared/yamlsettings.py`, `shared/tests/test_yamlsettings.py`, `airflow_src/dags/impl/handler_impl.py`, `airflow_src/tests/dags/impl/test_handler_impl.py`, `shared/tests/test_deployment_paths.py`
**Scope:** M

### Task 3: `_check_content` explicit field list

**Description:** Replace the dump-everything loop (spec 2.6). Strict check on the listed relative
fields incl. `slurm_mem` (`runner` is added in T5); `software` with `allow_absolute_paths=True`;
`config_params` via `substitute_dummy_values(settings.config_params)` with spaces. Not checked:
`raw_file_path`, `settings_path`, `output_path`, `custom_command`, `slurm_time`. One
`TODO: revisit validation ...` above the list. `shared/validation.py` untouched.

**Acceptance criteria:**
- [x] Still rejects `..`, `;`, `$` in relative paths, file names, `software`, `slurm_mem`, `config_params` (7.2 second half).
- [x] A `QuantingEnv` with `raw_file_path='\\server\share\x.raw'`, `output_path='Z:\out'` and posix relative parts returns no errors.
- [x] Every new branch covered (7.9).

**Verification:**
- [x] `pytest airflow_src/tests/dags/impl/test_processor_impl.py -k check_content`
- [x] `pytest airflow_src`

**Dependencies:** None
**Files:** `airflow_src/dags/impl/processor_impl.py`, `airflow_src/tests/dags/impl/test_processor_impl.py`
**Scope:** S

### Task 4: `shared/runners.py` and the `runners:` yaml block

**Description:** New module per spec 2.2: `OperatingSystems`, frozen `Runner`, `_build_runners`,
`RUNNERS`, `get_runner`. `YamlKeys.RUNNERS` plus nested keys (`name`, `engine`, `os`,
`ssh_connection_id_prefix`, `view`). The three yamls gain `runners:` with a `slurm` and a `docker`
runner whose `view` copies the `absolute_path` values; the `_test_` stub gains `slurm`, `docker`,
`file_based` with the `./tmp/test/...` paths. `local.yaml` carries the reference comments incl.
the one-line 2.5.1/2.5.2 rationale next to `ssh_connection_id_prefix`. Nothing consumes `RUNNERS`
yet. Consistency test: every in-repo yaml declares `runners`, engine and os known, each runner has
`view`, each `slurm` runner declares all five locations, `backup_base_path == slurm view.backup`.
`locations` stays in the yamls. Spec 2.1, 2.2, 2.9 runner parts, 7.1.

**Acceptance criteria:**
- [ ] `_build_runners` rejects: empty list, missing `name`, duplicate `name`, unknown `engine`, missing or unknown `os`, missing `view`, unknown `view` key; each error names runner and key.
- [ ] Accepts: prefix on a `docker` runner, runner without `slurm`; `macos` == `linux` flavour.
- [ ] Windows runner resolves `\\server\share\backup\test1\1970_01\f.raw` and `Z:\...\out_f.raw\alphadia` from the layout functions; `get_runner("nope")` raises `KeyError` listing known names.
- [ ] Each new consistency assertion fails on a mutated yaml (one mutation each, reverted).
- [ ] No yaml key or engine/os literal at a use site in `runners.py`.

**Verification:**
- [ ] `pytest shared/tests/test_runners.py shared/tests/test_deployment_paths.py`
- [ ] `pytest shared`

**Dependencies:** T2 (equality assertion)
**Files:** `shared/runners.py` (new), `shared/tests/test_runners.py` (new), `shared/yamlsettings.py`, `envs/alphakraken.{local,sandbox,production}.yaml`, `shared/tests/test_deployment_paths.py`
**Scope:** M

### Checkpoint 1: Foundations
- [ ] Four commits, three suites green, pre-commit clean.
- [ ] `envs/alphakraken.local.yaml` loads with `runners`, `backup_base_path` and old `locations` side by side; nothing imports `shared.runners` outside tests.
- [ ] Human review before Phase 2.

## Phase 2: Jobs are dispatched by runner

### Task 5: `QuantingEnv.runner`, factory takes a `Runner`

**Description:** `QuantingEnv.job_engine` -> `runner: str = Field(alias="_RUNNER")`.
`_get_job_handler(runner: Runner)` dispatches on `runner.engine`; `start_job/get_job_status/
get_job_result(..., runner_name)` call `get_runner`. `ssh_sensor.py` reads `.runner`.
`processor_impl` passes `quanting_env.runner` to the job functions and, for this commit only,
sets `runner=settings.job_engine` (transitional line, removed in T6, named in the commit
message). `runner` joins the strict list of `_check_content`. Slurm branch still resolves
`CLUSTER_VIEW` for its base dir (T8). Spec 2.3, 2.5 factory signature, 7.5 partial, 9.5.

**Acceptance criteria:**
- [ ] `QuantingEnv.to_dict()` differs from before only in `_JOB_ENGINE` -> `_RUNNER` (existing full-dict assertion in `test_create_quanting_env`, spec 7.3).
- [ ] Factory per engine with a `Runner`; unknown engine raises `ValueError`.
- [ ] `start_job(env, runner_name="nope")` raises `KeyError` listing declared runners (9.5).
- [ ] `grep -rn job_engine airflow_src --include='*.py'` hits only the transitional line.

**Verification:**
- [ ] `pytest airflow_src/tests/plugins/jobs/test_job_handler.py airflow_src/tests/plugins/sensors airflow_src/tests/dags/impl/test_processor_impl.py`
- [ ] `pytest airflow_src`

**Dependencies:** T3, T4
**Files:** `airflow_src/plugins/common/quanting_env.py`, `airflow_src/plugins/jobs/job_handler.py`, `airflow_src/plugins/sensors/ssh_sensor.py`, `airflow_src/dags/impl/processor_impl.py`, tests: `conftest.py`, `test_job_handler.py`, `test_ssh_sensor.py`, `test_processor_impl.py`
**Scope:** M

### Task 6: `Settings.runner` replaces `job_engine`; webapp selectbox

**Description:** `Settings.runner = StringField(required=True, max_length=64)`, `job_engine`
deleted; `create_settings(runner=...)`. The T5 transitional line becomes `runner=settings.runner`.
Webapp (spec 2.7): options `list(RUNNERS)`, default first declared, `SHOW_RUNNER_SELECT`, prefill
key `runner`, docker-only-custom check via `RUNNERS[runner].engine`, help texts at 314 and 415
say "runner". Test 7.6.

**Acceptance criteria:**
- [ ] `grep -rn job_engine --include='*.py' . | grep -v _migrations` returns nothing (9.1).
- [ ] Webapp selectbox options equal `list(RUNNERS)`; docker + non-custom rejected by the engine of the selected runner (7.6).
- [ ] `create_settings` without `runner` raises.

**Verification:**
- [ ] `pytest shared/tests/db/test_interface.py webapp airflow_src/tests/dags/impl/test_processor_impl.py`
- [ ] All three suites

**Dependencies:** T5
**Files:** `shared/db/models.py`, `shared/db/interface.py`, `webapp/pages_/settings.py`, `airflow_src/dags/impl/processor_impl.py` (one line), tests: `shared/tests/db/test_interface.py`, `webapp/tests/pages_/test_settings.py`, `airflow_src/tests/dags/impl/test_processor_impl.py` (mocks)
**Scope:** M

### Task 7: Migration `job_engine` -> `runner`

**Description:** `shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py`, shape of
`_migrate_backfill_settings_fields.py`: for each Settings document with `job_engine` and without
`runner`, `$set runner` from `_ENGINE_TO_RUNNER` (identity by default), `$unset job_engine`.
`--dry-run`; prints distinct target runner names with counts. Docstring states the yaml
precondition. Spec 2.8, 9.7.

**Acceptance criteria:**
- [ ] Documents already carrying `runner` are skipped; each document reported once (9.7).
- [ ] Summary lists distinct target names with counts.

**Verification:**
- [ ] `--dry-run` against a local mongo with two hand-made Settings docs (one legacy, one migrated): 1 updated, 1 skipped.
- [ ] `pre-commit run --all-files`

**Dependencies:** T6
**Files:** `shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py` (new)
**Scope:** S

### Checkpoint 2: Runner flows
- [ ] Three suites green, pre-commit clean; `grep -n "job_engine" airflow_src/dags/impl/processor_impl.py` empty.
- [ ] Local stack: create a settings entry in the webapp with runner `slurm`, trigger a quanting DAG with `debug_no_cluster_ssh=true`, the `prepare_job` XCom shows `_RUNNER: slurm` and the same paths as before.
- [ ] Migration `--dry-run` runs against a sandbox DB copy; note the distinct names (open question 1).
- [ ] Human review before Phase 3.

## Phase 3: Paths come from the runner

### Task 8: Slurm base dir and SSH connections from the runner

**Description:** Factory: `SlurmSSHJobHandler(runner.view.resolve(Locations.SLURM),
runner.ssh_connection_id_prefix)`, `AirflowFailException` naming the runner if the prefix is
`None`. `ssh_execute(command, ssh_connection_id_prefix)`, `get_cluster_ssh_hook(attempt_no,
prefix, ...)`, `_get_cluster_ssh_connections(prefix)`; error text names the given prefix.
`CLUSTER_SSH_CONNECTION_ID_PREFIX` deleted; `constants.py:10` comment reworded.
`debug_no_cluster_ssh` untouched. `test_job_handler.py` builds `Runner` objects directly and drops
`yaml_locations`. Spec 2.5, 7.4, 7.5.

**Acceptance criteria:**
- [ ] Two prefixes select disjoint connection sets (7.4).
- [ ] `slurm` runner with `ssh_connection_id_prefix=None` raises naming the runner (7.5).
- [ ] `grep -rn CLUSTER_SSH_CONNECTION_ID_PREFIX .` empty; `job_handler.py` no longer imports `CLUSTER_VIEW`.

**Verification:**
- [ ] `pytest airflow_src/tests/plugins/jobs airflow_src/tests/common/test_utils.py airflow_src/tests/plugins/sensors`
- [ ] `pytest airflow_src`

**Dependencies:** T5
**Files:** `airflow_src/plugins/jobs/job_handler.py`, `airflow_src/plugins/jobs/slurm_ssh_job_handler.py`, `airflow_src/plugins/sensors/ssh_utils.py`, `airflow_src/plugins/common/utils.py`, `airflow_src/plugins/common/constants.py`, tests: `test_job_handler.py`, `test_utils.py`, `test_ssh_utils.py`, `test_slurm_ssh_job_handler.py`
**Scope:** L by file count; mechanical plumbing of one argument, not split (a partial chain leaves a dead parameter)

### Task 9: `prepare_job` resolves through `runner.view`; `CLUSTER_VIEW` deleted

**Description:** `runner = get_runner(settings.runner)`; the four `CLUSTER_VIEW.resolve` calls
become `runner.view.resolve`; parameter types `PurePosixPath` -> `PurePath`; relative paths stay
posix. `CLUSTER_VIEW`, `_build_cluster_view` and `test_cluster_view` removed. Test helper
`yaml_locations` -> `runner_view(name, **paths)` patching `RUNNERS[name].view._locations`;
`test_processor_impl.py` switches to it. Test 7.2: windows runner yields windows strings in
`RAW_FILE_PATH`, `SETTINGS_PATH`, `OUTPUT_PATH`, `CUSTOM_COMMAND`, substituted `_CONFIG_PARAMS`,
and `_check_content` passes. Spec 2.4, 7.2, 7.3, 9.4.

**Acceptance criteria:**
- [ ] Windows-runner `prepare_job` output matches 9.4; `_RELATIVE_RAW_FILE_PATH` and `RELATIVE_OUTPUT_PATH` stay `/`-separated.
- [ ] Slurm runner with the former `absolute_path` values: `to_dict()` unchanged vs. T5 (7.3).
- [ ] `grep -rn CLUSTER_VIEW --include='*.py' .` empty.

**Verification:**
- [ ] `pytest airflow_src/tests/dags/impl/test_processor_impl.py shared/tests/test_path_views.py`
- [ ] All three suites

**Dependencies:** T2, T6, T8
**Files:** `airflow_src/dags/impl/processor_impl.py`, `shared/path_views.py`, `airflow_src/tests/helpers.py`, `airflow_src/tests/dags/impl/test_processor_impl.py`, `shared/tests/test_path_views.py`
**Scope:** M

### Checkpoint 3: Paths through the runner
- [ ] Three suites green, pre-commit clean; 9.4 by test.
- [ ] Local stack: `prepare_job` XCom identical to checkpoint 2.
- [ ] Human review before Phase 4.

## Phase 4: Yaml final shape

### Task 10: `locations` -> `mounts`, `mount.sh`, keys removed

**Description:** In the three yamls move `backup`, `output`, `logs` mount entries to top-level
`mounts:` and delete `locations` (`general.mounts_path`, every `absolute_path`; `settings`,
`software`, `slurm` entries not carried over, spec 1.2.6). `_test_` stub loses `locations`.
`YamlKeys.LOCATIONS`, `ABSOLUTE_PATH`, `YamlKeys.Locations` removed; `YamlKeys.MOUNTS` added;
`Locations` docstring in `path_views.py` reworded. `mount.sh`: `ENTITY_TYPE=mounts`,
`MOUNTS_PATH` from `envs/${ENV}.env` sourced in a subshell exporting only that variable.
Consistency test: no top-level `locations`, every `mounts.<x>` has `mount_src` and
`mount_target`, mount-target assertions iterate `mounts`; line 52 docstring reworded.
Spec 1.2.6, 1.2.7, 2.1, 2.9, 2.9a, 7.7.

**Acceptance criteria:**
- [ ] `grep -n '^locations:' envs/*.yaml` empty; `grep -rn 'YamlKeys.LOCATIONS\|ABSOLUTE_PATH\|mounts_path' --include='*.py' . | grep -v _migrations` returns only `shared/validation.py:ABSOLUTE_PATH_ERROR`.
- [ ] `ENV=local ./mount.sh {backup,output,logs,test1} fstab` prints the same lines as before this task when `MOUNTS_PATH` in `envs/local.env` equals the old yaml value (manual diff, noted in the commit message).
- [ ] Each new consistency assertion fails on a mutated yaml (7.7).

**Verification:**
- [ ] `pytest shared`; all three suites
- [ ] Manual `mount.sh ... fstab` diff

**Dependencies:** T1, T9
**Files:** `envs/alphakraken.{local,sandbox,production}.yaml`, `shared/yamlsettings.py`, `shared/path_views.py` (docstring), `shared/tests/test_yamlsettings.py`, `shared/tests/test_deployment_paths.py`, `mount.sh`
**Scope:** M

### Checkpoint 4: Yaml final shape
- [ ] Three suites green, pre-commit clean; 9.6 holds (import fails naming `runners` on a stub without it, one reload-based test at most).
- [ ] Local stack boots on the new yaml; `prepare_job` XCom identical to checkpoint 3.
- [ ] Human review before Phase 5.

## Phase 5: Docs and closure

### Task 11: Docs, comment sweep, success criteria

**Description:** Spec 2.10 and the reword-only list at the end of spec §5: `docs/deployment.md`
(SSH section with 2.5.1/2.5.2 and prefix selection; standalone docker section says "runner",
mentions `runners:`, drops the two-keys-in-sync instruction at 232/346 for "`MOUNTS_PATH` must be
absolute for the docker runner"; mounting section: relative `MOUNTS_PATH` yields a relative fstab
line; upgrade notes: new yaml and code together, then migration before any quanting DAG runs,
webapp container included; line 187 `view.slurm`). `shared/config_params.py:30,33`. Remaining
reword sites: `webapp/pages_/settings.py:311,381`, `file_based_job_handler.py:10`,
`test_docker_job_handler.py:37`. TODOs reworded, never resolved.

**Acceptance criteria:**
- [ ] 9.1 and 9.2 greps return only migration scripts and `design_docs`.
- [ ] Every 2.10 bullet has a corresponding diff hunk.
- [ ] `git grep -c TODO -- '*.py' | awk -F: '{s+=$2} END {print s}'` equals the count on `main` plus one (the T3 TODO).

**Verification:**
- [ ] `grep -rn 'CLUSTER_VIEW\|absolute_path\|mounts_path\|locations\.[a-z*]*\.\|"locations"\|^locations:\|YamlKeys.LOCATIONS' --include='*.py' --include='*.sh' --include='*.md' . envs/*.yaml | grep -v node_modules`
- [ ] All three suites; `pre-commit run --all-files`

**Dependencies:** T10
**Files:** `docs/deployment.md`, `shared/config_params.py`, `webapp/pages_/settings.py`, `airflow_src/plugins/jobs/_experimental/file_based_job_handler.py`, `airflow_src/tests/plugins/jobs/test_docker_job_handler.py`
**Scope:** S (text only)

### Checkpoint 5: Complete
- [ ] Spec §9.1 to 9.7 satisfied; 9.7 run against a sandbox DB copy.
- [ ] Eleven commits, each green on its own (`git rebase -x 'pytest shared' main` as optional spot check).
- [ ] Ready for PR.
