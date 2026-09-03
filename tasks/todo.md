# Tasks: Named runners (D3-lite)

Spec refs are to `SPEC.md`. Every task: one commit, `pre-commit run --all-files` clean,
`pytest shared`, `pytest webapp`, `pytest airflow_src --ignore=airflow_src/tests/plugins/jobs/test_docker_job_handler.py` green.

## Phase 1: Runners exist (no consumer yet)

### Task 1: `shared/runners.py` with unit tests

**Description:** New module per spec 2.2: `OperatingSystems`, frozen `Runner`, `_build_runners`,
`RUNNERS`, `get_runner`. `YamlKeys.RUNNERS` plus nested keys (`name`, `engine`, `os`,
`ssh_connection_id_prefix`, `view`) in `yamlsettings.py`. The `_test_` stub gains `runners`
(`slurm`, `docker`, `file_based`, current `./tmp/test/...` paths). `locations` stays untouched.

**Acceptance criteria:**
- [ ] `_build_runners` rejects: missing list/empty list, missing `name`, duplicate `name`, unknown `engine`, missing/unknown `os`, missing `view`, unknown `view` key. Each error names runner and yaml key.
- [ ] Accepts: prefix on a `docker` runner, runner without `slurm`, `macos` == `linux` flavour.
- [ ] Windows runner resolves `\\server\share\backup\test1\1970_01\f.raw` and `Z:\...\out_f.raw\alphadia` from `get_raw_file_rel_path` / `get_output_folder_rel_path`.
- [ ] `get_runner("nope")` raises `KeyError` listing the known names.

**Verification:**
- [ ] `pytest shared/tests/test_runners.py`
- [ ] `pytest shared` (import of stub with `runners` works)
- [ ] `python -c "import shared.runners"` with `ENV_NAME=_test_`

**Dependencies:** None
**Files:** `shared/runners.py` (new), `shared/yamlsettings.py`, `shared/tests/test_runners.py` (new)
**Scope:** M

### Task 2: In-repo yamls declare runners; consistency test

**Description:** `envs/alphakraken.{local,sandbox,production}.yaml` gain `runners:` with a `slurm`
and a `docker` runner whose `view` values copy the existing `absolute_path` values (spec 2.1).
`local.yaml` carries the reference comments incl. the one-line 2.5.1/2.5.2 rationale next to
`ssh_connection_id_prefix`. Extend `test_deployment_paths.py`: every yaml declares `runners`,
each engine and os known, each runner has `view`, each `slurm` runner declares all five
locations. `locations` stays in the yamls.

**Acceptance criteria:**
- [ ] `_build_runners(yaml["runners"])` succeeds for all three yamls.
- [ ] Each new assertion fails on a mutated yaml (try one mutation per assertion, revert).
- [ ] Existing `locations` assertions untouched and green.

**Verification:**
- [ ] `pytest shared/tests/test_deployment_paths.py`
- [ ] `docker compose config` still parses (`./compose.sh` equivalent) with `ENV=local`

**Dependencies:** T1
**Files:** `envs/alphakraken.local.yaml`, `envs/alphakraken.sandbox.yaml`, `envs/alphakraken.production.yaml`, `shared/tests/test_deployment_paths.py`
**Scope:** M

### Checkpoint A
- [ ] `pytest shared` green; `runners` and `locations` coexist; nothing imports `shared.runners` outside tests.

## Phase 2: Jobs are dispatched by runner

### Task 3: `QuantingEnv.runner` and factory by `Runner`

**Description:** `QuantingEnv.job_engine` -> `runner: str = Field(alias="_RUNNER")`.
`_get_job_handler(runner: Runner)` dispatches on `runner.engine`; `start_job/get_job_status/
get_job_result(..., runner_name)` call `get_runner`. `ssh_sensor.py` reads `.runner`.
`processor_impl` passes `quanting_env.runner` to the job functions and, for this one commit,
sets `runner=settings.job_engine` (bridge, removed in T4). Slurm branch still resolves
`CLUSTER_VIEW` for its base dir (changes in T8). Test 7.5 partial: factory per engine with a
`Runner`, unknown engine raises `ValueError`.

**Acceptance criteria:**
- [ ] `QuantingEnv.to_dict()` differs from before only in `_JOB_ENGINE` -> `_RUNNER` (existing `test_create_quanting_env` expected dict, spec 7.3).
- [ ] `start_job(env, runner_name="nope")` raises `KeyError` listing declared runners (spec 9.5).
- [ ] `grep -rn job_engine airflow_src` hits only the bridge line in `processor_impl.py`.

**Verification:**
- [ ] `pytest airflow_src/tests/plugins/jobs/test_job_handler.py airflow_src/tests/plugins/sensors airflow_src/tests/dags/impl/test_processor_impl.py`
- [ ] Full `pytest airflow_src` (with the docker ignore)

**Dependencies:** T1
**Files:** `airflow_src/plugins/common/quanting_env.py`, `airflow_src/plugins/jobs/job_handler.py`, `airflow_src/plugins/sensors/ssh_sensor.py`, `airflow_src/dags/impl/processor_impl.py`, tests: `conftest.py`, `test_job_handler.py`, `test_ssh_sensor.py`, `test_processor_impl.py`
**Scope:** M (4 source, 4 test files)

### Task 4: `Settings.runner` replaces `job_engine`; webapp selectbox

**Description:** `Settings.runner = StringField(required=True, max_length=64)`, `job_engine`
deleted; `create_settings(runner=...)`. `processor_impl` bridge becomes `runner=settings.runner`.
Webapp (spec 2.7): options `list(RUNNERS)`, default first declared, `SHOW_RUNNER_SELECT`,
prefill key `runner`, docker-only-custom check via `RUNNERS[runner].engine`, help texts at
314/415 say "runner". Test 7.6.

**Acceptance criteria:**
- [ ] `grep -rn job_engine --include='*.py' . | grep -v _migrations` returns nothing (spec 9.1).
- [ ] Webapp selectbox options equal `list(RUNNERS)`; docker+non-custom rejected by engine of the selected runner.
- [ ] `create_settings` without `runner` raises.

**Verification:**
- [ ] `pytest shared/tests/db/test_interface.py webapp`
- [ ] `pytest airflow_src` (mocks in `test_processor_impl.py` use `.runner`)

**Dependencies:** T3
**Files:** `shared/db/models.py`, `shared/db/interface.py`, `webapp/pages_/settings.py`, `airflow_src/dags/impl/processor_impl.py` (1 line), tests: `shared/tests/db/test_interface.py`, `webapp/tests/pages_/test_settings.py`, `airflow_src/tests/dags/impl/test_processor_impl.py`
**Scope:** M

### Task 5: Migration `job_engine` -> `runner`

**Description:** `shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py`, shape of
`_migrate_backfill_settings_fields.py`: for each Settings document with `job_engine` and without
`runner`, `$set runner` from `_ENGINE_TO_RUNNER` (identity by default), `$unset job_engine`.
`--dry-run`; prints distinct target runner names with counts. Docstring states the yaml
precondition and the deploy order (spec 2.8, 2.10 upgrade note goes to `docs/deployment.md` in T13).

**Acceptance criteria:**
- [ ] Documents already carrying `runner` are skipped; each document is reported once (spec 9.7).
- [ ] Summary lists distinct target names with counts.

**Verification:**
- [ ] `--dry-run` against a local mongo with two hand-made Settings docs (one legacy, one migrated) reports 1 updated, 1 skipped.
- [ ] `ruff`/`ty` clean via pre-commit.

**Dependencies:** T4
**Files:** `shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py` (new), `shared/_migrations/README.migrations.md` (list entry if the README enumerates scripts)
**Scope:** S

### Checkpoint B
- [ ] 9.1 grep empty; 9.5 reproducible in a unit test; migration `--dry-run` runs.
- [ ] Human review before Phase 3.

## Phase 3: Paths come from the runner

### Task 6: `_check_content` explicit field list

**Description:** Replace the dump-everything loop (spec 2.6). Strict check on the listed
relative fields incl. `runner` and `slurm_mem`; `software` with `allow_absolute_paths=True`;
`config_params` via `substitute_dummy_values(settings.config_params)` with spaces. Not checked:
`raw_file_path`, `settings_path`, `output_path`, `custom_command`, `slurm_time`. Add the one
`TODO: revisit validation ...` above the list. Character set in `shared/validation.py` unchanged.

**Acceptance criteria:**
- [ ] Still rejects `..`, `;`, `$` in relative paths, file names, `software`, `config_params` (spec 7.2 second half).
- [ ] A `QuantingEnv` with `raw_file_path='\\server\share\x.raw'` and posix relative parts returns no errors.
- [ ] Every new branch covered (spec 7.9).

**Verification:**
- [ ] `pytest airflow_src/tests/dags/impl/test_processor_impl.py -k check_content`

**Dependencies:** T3
**Files:** `airflow_src/dags/impl/processor_impl.py`, `airflow_src/tests/dags/impl/test_processor_impl.py`
**Scope:** S

### Task 7: `prepare_job` resolves through `runner.view`

**Description:** `runner = get_runner(settings.runner)`; the four `CLUSTER_VIEW.resolve` calls
become `runner.view.resolve` (`raw_file_path`, `settings_path`, `output_path`, software path);
parameter types `PurePosixPath` -> `PurePath`; relative paths stay posix (spec 2.4). Test helper
`runner_view(name, **paths)` added to `airflow_src/tests/helpers.py` (patches
`RUNNERS[name].view._locations`); `test_processor_impl.py` switches from `yaml_locations` to it.
Test 7.2: windows runner (patched `RUNNERS`) yields windows strings in `RAW_FILE_PATH`,
`SETTINGS_PATH`, `OUTPUT_PATH`, `CUSTOM_COMMAND`, substituted `_CONFIG_PARAMS`, and
`_check_content` passes.

**Acceptance criteria:**
- [ ] `processor_impl.py` no longer imports `CLUSTER_VIEW`.
- [ ] Windows-runner `prepare_job` output matches spec 9.4; `_RELATIVE_RAW_FILE_PATH` and `RELATIVE_OUTPUT_PATH` stay `/`-separated.
- [ ] Existing posix expectations in `test_processor_impl.py` unchanged (7.3).

**Verification:**
- [ ] `pytest airflow_src/tests/dags/impl/test_processor_impl.py`
- [ ] `pytest airflow_src`

**Dependencies:** T6
**Files:** `airflow_src/dags/impl/processor_impl.py`, `airflow_src/tests/helpers.py`, `airflow_src/tests/dags/impl/test_processor_impl.py`
**Scope:** M

### Task 8: Slurm handler and SSH connections from the runner

**Description:** Factory: `SlurmSSHJobHandler(runner.view.resolve(Locations.SLURM),
runner.ssh_connection_id_prefix)`, `AirflowFailException` naming the runner if the prefix is
`None`. `ssh_execute(command, ssh_connection_id_prefix)`, `get_cluster_ssh_hook(attempt_no,
prefix, ...)`, `_get_cluster_ssh_connections(prefix)`; error text names the given prefix.
`CLUSTER_SSH_CONNECTION_ID_PREFIX` deleted (spec 2.5). `debug_no_cluster_ssh` untouched.
`test_job_handler.py` switches to `runner_view`. Tests 7.4, 7.5 complete.

**Acceptance criteria:**
- [ ] Two prefixes select disjoint connection sets (7.4).
- [ ] `slurm` runner with `ssh_connection_id_prefix=None` raises naming the runner (7.5).
- [ ] `grep -rn CLUSTER_SSH_CONNECTION_ID_PREFIX .` returns nothing; `job_handler.py` no longer imports `CLUSTER_VIEW`.

**Verification:**
- [ ] `pytest airflow_src/tests/plugins/jobs airflow_src/tests/common/test_utils.py airflow_src/tests/plugins/sensors`
- [ ] `pytest airflow_src`

**Dependencies:** T3, T7 (for `runner_view`)
**Files:** `airflow_src/plugins/jobs/job_handler.py`, `airflow_src/plugins/jobs/slurm_ssh_job_handler.py`, `airflow_src/plugins/sensors/ssh_utils.py`, `airflow_src/plugins/common/utils.py`, `airflow_src/plugins/common/constants.py`, tests: `test_job_handler.py`, `test_utils.py`, `test_ssh_utils.py`, `test_slurm_ssh_job_handler.py`
**Scope:** L (5 source files, mechanical plumbing of one argument; do not split, a partial chain leaves a dead parameter)

### Checkpoint C
- [ ] Only `handler_impl.get_backup_base_path`, `tests/helpers.yaml_locations` and `test_path_views` still reference `CLUSTER_VIEW`.
- [ ] 9.4 demonstrated by test; human review.

## Phase 4: `locations` dissolved

### Task 9: `backup.backup_base_path` replaces `CLUSTER_VIEW` in `get_backup_base_path`

**Description:** `YamlKeys.Backup.BACKUP_BASE_PATH`; module constant `BACKUP_BASE_PATH` in
`yamlsettings.py` read at import, missing key raises naming it. Three yamls and the `_test_` stub
gain `backup.backup_base_path` = former `locations.backup.absolute_path`. `get_backup_base_path`
= `PurePosixPath(BACKUP_BASE_PATH) / get_raw_file_folder_rel_path(raw_file)`. Consistency test:
key present and equal to the `slurm` runner's `view.backup`.

**Acceptance criteria:**
- [ ] `get_backup_base_path` returns the same string as before for the same yaml values (7.3).
- [ ] `handler_impl.py` no longer imports `CLUSTER_VIEW`; line 68 comment reworded.
- [ ] Consistency assertion fails when `backup_base_path` is mutated.

**Verification:**
- [ ] `pytest shared/tests/test_deployment_paths.py airflow_src/tests/dags/impl/test_handler_impl.py`

**Dependencies:** T2
**Files:** `shared/yamlsettings.py`, `envs/alphakraken.{local,sandbox,production}.yaml`, `airflow_src/dags/impl/handler_impl.py`, `shared/tests/test_deployment_paths.py`, `airflow_src/tests/dags/impl/test_handler_impl.py`
**Scope:** M

### Task 10: `DOCKER_HOST_VIEW` from `MOUNTS_PATH` environment

**Description:** `EnvVars.MOUNTS_PATH`; `_build_docker_host_view` reads it (unset -> empty view).
`docker-compose.yaml` passes `MOUNTS_PATH` in `airflow-common-env`. The three test conftests set
`MOUNTS_PATH` next to `ENV_NAME`; `_test_` stub loses `general.mounts_path` (the `locations`
block otherwise stays until T11). Factory error text names the variable instead of the yaml key;
`docker_job_handler.py` module docstring likewise. `envs/*.env` comment: must be absolute for the
docker runner. Test 7.8.

**Acceptance criteria:**
- [ ] `DOCKER_HOST_VIEW` built from the variable; unset variable reaches nothing (7.8).
- [ ] `grep -n mounts_path shared airflow_src docker-compose.yaml` returns nothing.
- [ ] `docker compose config` shows `MOUNTS_PATH` in the airflow services' environment.

**Verification:**
- [ ] `pytest shared/tests/test_path_views.py airflow_src/tests/plugins/jobs/test_job_handler.py`
- [ ] All three suites (conftests changed)

**Dependencies:** None (independent of T9)
**Files:** `shared/keys.py`, `shared/path_views.py`, `shared/yamlsettings.py`, `docker-compose.yaml`, `envs/{local,sandbox,production}.env`, `airflow_src/plugins/jobs/job_handler.py`, `airflow_src/plugins/jobs/docker_job_handler.py` (docstring), conftests (3), `shared/tests/test_path_views.py`, `airflow_src/tests/plugins/jobs/test_job_handler.py`
**Scope:** L by file count, each change one to three lines

### Task 11: `mounts:` block, `mount.sh`, `locations` removed from the yamls

**Description:** In the three yamls move `backup`, `output`, `logs` mount entries to top-level
`mounts:`; delete `locations` entirely (`settings`, `software`, `slurm` entries are not carried
over, spec 1.2.6). `_test_` stub loses `locations`. `YamlKeys.MOUNTS`. `mount.sh`: `ENTITY_TYPE`
`mounts`, `MOUNTS_PATH` from `set -a; source envs/${ENV}.env; set +a` (spec 2.9a). Deployment
test: mount-target assertions iterate `mounts`, no top-level `locations`, every `mounts.<x>` has
`mount_src` and `mount_target`. Line 52 docstring reworded.

**Acceptance criteria:**
- [ ] `grep -n '^locations:' envs/*.yaml` empty; `_build_cluster_view()` returns an empty view without error (still unused).
- [ ] `ENV=local ./mount.sh backup fstab` prints the same line as before this task, except the mounts folder comes from `envs/local.env`.
- [ ] Each new consistency assertion fails on a mutated yaml.

**Verification:**
- [ ] `pytest shared/tests/test_deployment_paths.py`; all three suites
- [ ] Manual: diff of `mount.sh ... fstab` output before/after for `backup`, `output`, `logs`, `test1`

**Dependencies:** T7, T8, T9 (no runtime reader of `locations` left)
**Files:** `envs/alphakraken.{local,sandbox,production}.yaml`, `shared/yamlsettings.py`, `mount.sh`, `shared/tests/test_deployment_paths.py`
**Scope:** M

### Task 12: Delete `CLUSTER_VIEW`

**Description:** Remove `CLUSTER_VIEW`, `_build_cluster_view` from `path_views.py`;
`YamlKeys.LOCATIONS`, `ABSOLUTE_PATH`, `YamlKeys.Locations` from `yamlsettings.py`; `Locations`
docstring reworded; `yaml_locations` helper and `test_cluster_view` deleted; `file_based_job_handler.py`
docstring and `test_docker_job_handler.py:37` comment reworded.

**Acceptance criteria:**
- [ ] `grep -rn 'CLUSTER_VIEW\|YamlKeys.LOCATIONS\|absolute_path' --include='*.py' . | grep -v _migrations` empty.
- [ ] Import of `shared.path_views` on a yaml without `runners:` fails naming the key (9.6, via `shared.runners` import chain; verify with a `_test_` stub mutation in a test).

**Verification:**
- [ ] All three suites; `pre-commit run --all-files`

**Dependencies:** T11
**Files:** `shared/path_views.py`, `shared/yamlsettings.py`, `shared/tests/test_path_views.py`, `airflow_src/tests/helpers.py`, `airflow_src/plugins/jobs/_experimental/file_based_job_handler.py`, `airflow_src/tests/plugins/jobs/test_docker_job_handler.py`
**Scope:** S

### Checkpoint D
- [ ] 9.3 (7.1 to 7.8 green), 9.6 by test, `mount.sh` output unchanged.
- [ ] Human review.

## Phase 5: Docs

### Task 13: Docs and comment sweep

**Description:** Spec 2.10 and the comment list in spec §5: `docs/deployment.md` (SSH connection
section with 2.5.1/2.5.2, standalone docker section says "runner" and drops the `mounts_path`
sync instruction, mounting section notes relative `MOUNTS_PATH`, line 187 `view.slurm`, upgrade
note: yaml+code together then migration), `shared/config_params.py:30,33`, remaining TODO
rewordings (`webapp/pages_/settings.py:311,381`, `constants.py:10`).

**Acceptance criteria:**
- [ ] Spec 9.2 grep returns only migration scripts and `design_docs`.
- [ ] Existing TODOs remain TODOs, only reworded.

**Verification:**
- [ ] `grep -rn 'CLUSTER_VIEW\|absolute_path\|mounts_path\|locations\.[a-z*]*\.\|"locations"\|^locations:\|YamlKeys.LOCATIONS' --include='*.py' --include='*.sh' --include='*.md' . envs/*.yaml | grep -v node_modules`
- [ ] `pre-commit run --all-files`

**Dependencies:** T12
**Files:** `docs/deployment.md`, `shared/config_params.py`, `webapp/pages_/settings.py`, `airflow_src/plugins/common/constants.py`
**Scope:** S

### Checkpoint E: Complete
- [ ] Spec §9 all met; three suites and pre-commit green.
- [ ] Ready for review / merge.
