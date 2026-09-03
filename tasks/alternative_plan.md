# Implementation Plan: Named runners (SPEC.md, D3-lite)

On approval this file is saved as `tasks/plan.md`; the work packages below become `tasks/todo.md`.

## Context

`Settings.job_engine` says *how* jobs run, never *where*. All exported paths go through one
`CLUSTER_VIEW`, SSH connections through one global prefix, and `_check_content` rejects every
Windows path. SPEC.md introduces named runners (engine + os + view + ssh prefix) as the seam for a
future SSH handler, moves validation to the user-controlled relative parts, and restructures the
yaml (`runners:`, `mounts:`, `backup.backup_base_path`, no `locations`).

## Constraints that shape the slicing

- Each work package (WP) is one commit, green on its own (`pytest shared`, `pytest webapp`,
  `pytest airflow_src`, `pre-commit run --all-files`), and the in-repo yamls stay loadable at every
  commit. Yaml and code change together within a WP.
- No backwards compatibility in the final state (SPEC §8). Two WPs carry a *named transitional*
  line that the next WP removes; they are marked below.
- Existing TODOs are reworded where 9.2 demands, never resolved.

## Dependency graph

```
WP1 MOUNTS_PATH -> DOCKER_HOST_VIEW          (independent)
WP2 backup.backup_base_path                  (independent)
WP3 _check_content explicit list             (independent)
WP4 shared/runners.py + runners: yaml        (independent)
        |
WP5 SSH prefix plumbing                      (independent)
        |
WP6 Settings.runner + QuantingEnv + webapp + migration   <- WP4
        |
WP7 factory takes Runner                     <- WP4, WP5, WP6
        |
WP8 prepare_job via runner.view, CLUSTER_VIEW deleted    <- WP2, WP6, WP7
        |
WP9 yaml: locations -> mounts, mount.sh      <- WP1, WP8
        |
WP10 docs, comment sweep, success criteria   <- all
```

Highest-risk WPs (WP8 byte-identical regression, WP9 mount.sh/deployment) come as early as the
graph allows; the four independent WPs go first because they are small and unblock everything.

---

## Phase 1: Foundations (independent slices)

### WP1: `DOCKER_HOST_VIEW` from the `MOUNTS_PATH` environment variable

**Description:** `_build_docker_host_view` reads `EnvVars.MOUNTS_PATH` (new constant) instead of
`locations.general.mounts_path`. `docker-compose.yaml` passes `MOUNTS_PATH` through
`airflow-common-env`. Test conftests set `MOUNTS_PATH` next to `ENV_NAME`. The yaml key stays in
place, unread, until WP9. Spec 1.2.7, 2.1, 7.8.

**Acceptance criteria:**
- [ ] Unset variable yields a view that reaches nothing; the docker factory branch reports
      `MOUNTS_PATH` (not the yaml key) when the docker engine is selected.
- [ ] `MOUNTS_PATH` set -> `DOCKER_HOST_VIEW.resolve(OUTPUT, "P1/x")` == `<MOUNTS_PATH>/output/P1/x`.
- [ ] No Python code reads `locations.general.mounts_path` anymore.

**Verification:** `pytest shared/tests/test_path_views.py airflow_src/tests/plugins/jobs/test_job_handler.py`;
all three suites; `pre-commit`.

**Files:** `shared/keys.py`, `shared/path_views.py`, `docker-compose.yaml`,
`{shared,airflow_src,webapp}/tests/conftest.py`, `shared/tests/test_path_views.py`,
`airflow_src/plugins/jobs/job_handler.py` (error text), `airflow_src/tests/plugins/jobs/test_job_handler.py`,
`airflow_src/plugins/jobs/docker_job_handler.py` (module docstring only).
**Scope:** M. **Depends on:** none.

### WP2: `backup.backup_base_path`, `get_backup_base_path` off `CLUSTER_VIEW`

**Description:** Add `backup.backup_base_path` to the three yamls (value = current
`locations.backup.absolute_path`) and to the `_test_` stub (`./tmp/test/backup`). `shared/yamlsettings.py`
exposes `BACKUP_BASE_PATH` (missing key raises at import, `YamlKeys.Backup.BACKUP_BASE_PATH`).
`handler_impl.get_backup_base_path` becomes `PurePosixPath(BACKUP_BASE_PATH) / get_raw_file_folder_rel_path(raw_file)`.
Consistency test asserts the key is present in every in-repo yaml. Spec 1.2.3, 2.2, 7.3 (second half).

**Acceptance criteria:**
- [ ] `get_backup_base_path` returns the same string as before for the same yaml values (regression test).
- [ ] Stub without `backup` block -> import raises naming `backup.backup_base_path`.
- [ ] `handler_impl.py` no longer imports `CLUSTER_VIEW`.

**Verification:** `pytest shared/tests/test_yamlsettings.py shared/tests/test_deployment_paths.py airflow_src/tests/dags/impl/test_handler_impl.py`; all suites; `pre-commit`.

**Files:** `envs/alphakraken.{local,sandbox,production}.yaml`, `shared/yamlsettings.py`,
`shared/tests/test_yamlsettings.py`, `airflow_src/dags/impl/handler_impl.py`,
`airflow_src/tests/dags/impl/test_handler_impl.py`, `shared/tests/test_deployment_paths.py`.
**Scope:** M. **Depends on:** none.

### WP3: `_check_content` explicit field list

**Description:** Replace the dump-everything loop with the explicit strict list of 2.6
(incl. `slurm_mem`), `software` with `allow_absolute_paths=True`, `config_params` via
`substitute_dummy_values` as today, `raw_file_path`/`settings_path`/`output_path`/`custom_command`/`slurm_time`
unchecked. One `TODO: revisit validation ...` above the list. This is the deliberate behaviour
change 1.2.2 and is independent of runners. Spec 2.6, 7.2 (second half).

**Acceptance criteria:**
- [ ] Windows strings in the absolute fields pass; `..`, `;`, `$` in relative paths, file names,
      `software`, `slurm_mem`, `config_params` are still rejected.
- [ ] Every existing `test_check_content_*` still passes.
- [ ] `shared/validation.py` untouched.

**Verification:** `pytest airflow_src/tests/dags/impl/test_processor_impl.py -k check_content`; all suites; `pre-commit`.

**Files:** `airflow_src/dags/impl/processor_impl.py`, `airflow_src/tests/dags/impl/test_processor_impl.py`.
**Scope:** S. **Depends on:** none.

### WP4: `shared/runners.py` and the `runners:` yaml block

**Description:** New module per 2.2: `OperatingSystems`, frozen `Runner(name, engine, os, view,
ssh_connection_id_prefix)`, `_build_runners(entries)` with all import-time validation,
`RUNNERS = _build_runners(YAMLSETTINGS[YamlKeys.RUNNERS])`, `get_runner(name)`. `YamlKeys.RUNNERS`
and nested keys. The three yamls and the `_test_` stub gain `runners:` (`slurm`, `docker`; stub also
`file_based`) with the current `absolute_path` values copied into `view` (slurm `view.backup` ==
`backup.backup_base_path`). Nothing consumes `RUNNERS` yet. Consistency test: every in-repo yaml
declares `runners`, engine/os known, each has `view`, slurm runner declares all five locations,
`backup_base_path == slurm view.backup`. Spec 2.1, 2.2, 2.9 (runner parts), 7.1, 9.6.

**Acceptance criteria:**
- [ ] 7.1 cases: missing `name`/`os`/`view`, duplicate `name`, unknown location, unknown `os` each
      raise naming runner and key; prefix on `docker` and runner without `slurm` accepted;
      `macos` == `linux`; windows runner resolves UNC and drive-letter strings via the layout
      functions; `get_runner` KeyError lists known runners.
- [ ] Stub without `runners:` -> import raises naming the key (9.6).
- [ ] `grep -rn '"runners"\|"view"\|"os"' shared/runners.py` -> nothing (constants only).

**Verification:** `pytest shared/tests/test_runners.py shared/tests/test_deployment_paths.py`; all suites; `pre-commit`.

**Files:** `shared/runners.py` (new), `shared/tests/test_runners.py` (new), `shared/yamlsettings.py`,
`envs/alphakraken.{local,sandbox,production}.yaml`, `shared/tests/test_deployment_paths.py`.
**Scope:** M. **Depends on:** none (WP2 first keeps the yaml diffs disjoint).

### Checkpoint 1: Foundations
- [ ] All three suites green, `pre-commit` clean, four commits.
- [ ] `envs/alphakraken.local.yaml` loads with `runners:` + `backup_base_path` + old `locations` side by side.
- [ ] Human review before Phase 2.

---

## Phase 2: Runner flows end to end

### WP5: SSH discovery by prefix argument

**Description:** `_get_cluster_ssh_connections(prefix)`, `get_cluster_ssh_hook(attempt_no, prefix, ...)`
(error text names the prefix it was given), `ssh_execute(command, ssh_connection_id_prefix, ...)`,
`SlurmSSHJobHandler(cluster_base_dir, ssh_connection_id_prefix)`. The factory passes
`CLUSTER_SSH_CONNECTION_ID_PREFIX` for now. `debug_no_cluster_ssh` shortcut untouched. Spec 2.5, 7.4.

*Transitional:* the constant survives until WP7.

**Acceptance criteria:**
- [ ] Two prefixes select disjoint connection sets (7.4).
- [ ] Every `ssh_execute`/`get_cluster_ssh_hook` call site passes a prefix; no module-level prefix read remains in `utils.py`.

**Verification:** `pytest airflow_src/tests/common/test_utils.py airflow_src/tests/plugins/sensors/test_ssh_utils.py airflow_src/tests/plugins/jobs/test_slurm_ssh_job_handler.py`; all suites; `pre-commit`.

**Files:** `airflow_src/plugins/common/utils.py`, `airflow_src/plugins/sensors/ssh_utils.py`,
`airflow_src/plugins/jobs/slurm_ssh_job_handler.py`, `airflow_src/plugins/jobs/job_handler.py` (one arg),
plus the three tests.
**Scope:** M. **Depends on:** none.

### WP6: `Settings.runner`, `QuantingEnv.runner`, webapp, migration

**Description:** `Settings.runner = StringField(required=True, max_length=64)`, `job_engine` deleted;
`create_settings(runner=...)`. `QuantingEnv.runner: str = Field(alias="_RUNNER")`. `processor_impl`
sets `runner=settings.runner`. Webapp: selectbox from `list(RUNNERS)`, first declared as default,
`SHOW_RUNNER_SELECT`, prefill key `runner`, line-509 check via `RUNNERS[runner].engine`, help texts.
Migration `shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py` per 2.8 (identity map,
`--dry-run`, prints distinct target runner names with counts). Test conftest defaults use `runner`.
Spec 2.3, 2.7, 2.8, 7.6, 9.1, 9.7.

*Transitional:* `start_job/get_job_status/get_job_result(..., engine=quanting_env.runner)` and
`ssh_sensor` pass the runner *name* where the factory still expects an engine. Valid because every
in-repo runner name equals its engine; removed in WP7.

**Acceptance criteria:**
- [ ] `grep -rn job_engine --include='*.py'` outside `shared/_migrations` -> nothing (9.1).
- [ ] `QuantingEnv.to_dict()` for the slurm runner equals the former dict except `_JOB_ENGINE` -> `_RUNNER` (7.3, first half).
- [ ] Webapp: options come from `RUNNERS`; docker-only-custom check keyed by engine of the selected runner (7.6).
- [ ] Migration dry-run on a copy of the sandbox DB lists every Settings document once (9.7, manual).

**Verification:** `pytest shared/tests/db/test_interface.py webapp/tests/pages_/test_settings.py airflow_src/tests/dags/impl/test_processor_impl.py airflow_src/tests/plugins/sensors/test_ssh_sensor.py`; all suites; `pre-commit`; manual: migration `--dry-run` per `shared/_migrations/README.migrations.md`.

**Files:** `shared/db/models.py`, `shared/db/interface.py`, `shared/tests/db/test_interface.py`,
`airflow_src/plugins/common/quanting_env.py`, `airflow_src/tests/conftest.py`,
`airflow_src/dags/impl/processor_impl.py`, `airflow_src/plugins/sensors/ssh_sensor.py`,
`airflow_src/tests/dags/impl/test_processor_impl.py`, `airflow_src/tests/plugins/sensors/test_ssh_sensor.py`,
`webapp/pages_/settings.py`, `webapp/tests/pages_/test_settings.py`,
`shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py` (new).
**Scope:** L by file count, one mechanical rename by concept. **Depends on:** WP4.

### WP7: Handler factory takes a `Runner`

**Description:** `_get_job_handler(runner: Runner)`; `start_job/get_job_status/get_job_result(..., runner_name)`
call `get_runner`. Slurm branch: `SlurmSSHJobHandler(runner.view.resolve(Locations.SLURM), runner.ssh_connection_id_prefix)`,
`AirflowFailException` naming the runner if the prefix is `None`. Docker branch unchanged
(`DockerJobHandler(DOCKER_HOST_VIEW)`). `CLUSTER_SSH_CONNECTION_ID_PREFIX` deleted. The WP6
transitional `engine=` arguments become `runner_name=quanting_env.runner`. Spec 2.5, 7.5, 9.5.

**Acceptance criteria:**
- [ ] Factory per engine with a `Runner`; unknown engine -> `ValueError`; slurm runner without prefix raises naming the runner (7.5).
- [ ] Undeclared `Settings.runner` fails the DAG with a message listing the declared runners (9.5).
- [ ] `job_handler.py` no longer imports `CLUSTER_VIEW`; the only `CLUSTER_VIEW` readers left are `processor_impl.py` and `tests/helpers.py`.

**Verification:** `pytest airflow_src/tests/plugins/jobs/test_job_handler.py airflow_src/tests/dags/impl/test_processor_impl.py airflow_src/tests/plugins/sensors/test_ssh_sensor.py`; all suites; `pre-commit`.

**Files:** `airflow_src/plugins/jobs/job_handler.py`, `airflow_src/plugins/common/constants.py`,
`airflow_src/dags/impl/processor_impl.py` (call sites), `airflow_src/plugins/sensors/ssh_sensor.py`,
`airflow_src/tests/plugins/jobs/test_job_handler.py`.
**Scope:** M. **Depends on:** WP4, WP5, WP6.

### Checkpoint 2: Runner flows
- [ ] All three suites green, `pre-commit` clean.
- [ ] No transitional line left: `grep -n "engine=" airflow_src/dags/impl/processor_impl.py airflow_src/plugins/sensors/ssh_sensor.py` -> nothing.
- [ ] Local stack: create a settings entry in the webapp with runner `slurm`, trigger a quanting DAG with `debug_no_cluster_ssh=true`, `prepare_job` XCom shows `_RUNNER`.
- [ ] Human review before Phase 3.

---

## Phase 3: Paths through the runner, yaml final shape

### WP8: `prepare_job` resolves through `runner.view`; `CLUSTER_VIEW` deleted

**Description:** `runner = get_runner(settings.runner)`; the four `CLUSTER_VIEW.resolve` calls become
`runner.view.resolve`; parameter types `PurePosixPath` -> `PurePath`; relative paths stay posix.
`CLUSTER_VIEW` and `_build_cluster_view` removed from `shared/path_views.py`; `test_cluster_view`
removed. `tests/helpers.yaml_locations()` -> `runner_view(name, **paths)` patching `RUNNERS`. Spec 2.4, 7.2, 7.3, 9.4.

**Acceptance criteria:**
- [ ] Windows runner (patched `RUNNERS`): `RAW_FILE_PATH`, `SETTINGS_PATH`, `OUTPUT_PATH`, `CUSTOM_COMMAND`,
      substituted `_CONFIG_PARAMS` are UNC/drive-letter strings and `_check_content` returns no errors (7.2, 9.4).
- [ ] Slurm runner with the former `absolute_path` values: `to_dict()` byte-identical except `_RUNNER` (7.3).
- [ ] `grep -rn CLUSTER_VIEW --include='*.py'` -> nothing.

**Verification:** `pytest airflow_src/tests/dags/impl/test_processor_impl.py shared/tests/test_path_views.py`; all suites; `pre-commit`.

**Files:** `airflow_src/dags/impl/processor_impl.py`, `shared/path_views.py`, `airflow_src/tests/helpers.py`,
`airflow_src/tests/dags/impl/test_processor_impl.py`, `shared/tests/test_path_views.py`.
**Scope:** M. **Depends on:** WP2, WP6, WP7.

### WP9: yaml `locations` -> `mounts`, `mount.sh`, keys removed

**Description:** In the three yamls: delete `locations` (incl. `general.mounts_path` and every
`absolute_path`), add `mounts.{backup,output,logs}` with the mount fields; the `_test_` stub loses
`locations`. `YamlKeys.LOCATIONS`, `ABSOLUTE_PATH`, `YamlKeys.Locations` removed; `YamlKeys.MOUNTS` added.
`mount.sh`: `ENTITY_TYPE=mounts`, `MOUNTS_PATH` from `envs/${ENV}.env` (sourced), fstab line unchanged.
Consistency test: no top-level `locations`, every `mounts.<x>` has `mount_src`/`mount_target`,
mount-target assertions iterate `mounts`. `.env` comments: `MOUNTS_PATH` absolute for the docker
runner. `docs/deployment.md` lines 232/346 stop asking to keep two keys in sync. Spec 1.2.6, 1.2.7, 2.1, 2.9, 2.9a, 7.7.

**Acceptance criteria:**
- [ ] `./mount.sh backup fstab` with `ENV=local` prints the same line as before, given `MOUNTS_PATH` in `envs/local.env` equals the old yaml value (manual, documented in the commit).
- [ ] Each 2.9 assertion shown to fail on a mutated yaml (7.7).
- [ ] `grep -rn 'YamlKeys.LOCATIONS\|ABSOLUTE_PATH\|mounts_path' --include='*.py'` outside `_migrations` -> only `shared/validation.py:ABSOLUTE_PATH_ERROR`.

**Verification:** `pytest shared`; all suites; `pre-commit`; manual `mount.sh ... fstab` diff.

**Files:** `envs/alphakraken.{local,sandbox,production}.yaml`, `shared/yamlsettings.py`,
`shared/tests/test_yamlsettings.py`, `shared/tests/test_deployment_paths.py`, `mount.sh`,
`envs/{local,sandbox,production}.env`, `docs/deployment.md` (two lines).
**Scope:** L. **Depends on:** WP1, WP8.

### Checkpoint 3: Yaml final shape
- [ ] All three suites green, `pre-commit` clean.
- [ ] `envs/alphakraken.local.yaml` has no `locations` key; local stack boots; `prepare_job` output unchanged vs. Checkpoint 2.
- [ ] Human review before Phase 4.

---

## Phase 4: Docs and closure

### WP10: Docs, comment sweep, success criteria

**Description:** `envs/alphakraken.local.yaml` as commented reference incl. one-line 2.5.1/2.5.2 next
to `ssh_connection_id_prefix`. `docs/deployment.md`: SSH section (credentials stay in Airflow, prefix
selection), standalone docker section ("runner", `runners:` block, `MOUNTS_PATH` absolute), mounting
section (`MOUNTS_PATH` absolute for `mount.sh`), upgrade notes (deploy order, migration first), line 187
(`view.slurm`). `shared/config_params.py:30,33` descriptions. The nine reword-only sites listed at the
end of SPEC §5 (TODOs reworded, not resolved). Run every §9 grep. Spec 2.10, §5 tail, §9.

**Acceptance criteria:**
- [ ] 9.1 and 9.2 greps return only migration scripts and design docs.
- [ ] Every SPEC 2.10 bullet has a corresponding diff hunk.
- [ ] No TODO resolved: `git diff --stat main -- '*.py' | grep -c TODO` unchanged in count.

**Verification:** the §9 greps; all suites; `pre-commit`.

**Files:** `envs/alphakraken.local.yaml`, `docs/deployment.md`, `shared/config_params.py`,
`shared/path_views.py`, `airflow_src/dags/impl/handler_impl.py`, `airflow_src/plugins/common/constants.py`,
`webapp/pages_/settings.py`, `airflow_src/tests/plugins/jobs/test_docker_job_handler.py`,
`airflow_src/plugins/jobs/_experimental/file_based_job_handler.py`.
**Scope:** M (text only). **Depends on:** all.

### Checkpoint 4: Complete
- [ ] SPEC §9.1-9.7 all satisfied; 9.7 run against a sandbox DB copy.
- [ ] Ten commits, each green on its own (`git rebase -x 'pytest shared' main` optional spot check).
- [ ] Ready for PR.

---

## Risks and mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| WP6 fan-out breaks tests not found by grep (fixtures building `Settings`) | Med | `pytest -x` per suite before commit; `grep -rn job_engine` incl. tests is the gate |
| Transitional `engine=quanting_env.runner` (WP6) silently works only while names == engines | Low | Named in the commit message; WP7 removes it in the next commit |
| WP8 changes an exported string by accident | High | 7.3 regression test written before the refactor, asserting the full dict |
| WP9 `mount.sh` sourcing `envs/${ENV}.env` pulls in other variables (`ENV_NAME`, passwords) | Med | Source in a subshell and export only `MOUNTS_PATH`; manual fstab diff |
| Import-time raise in WP2/WP4 breaks the webapp or monitoring for a yaml that lacks the key | Med | The three yamls and the stub change in the same commit; consistency test guards them |
| Sandbox DB holds `file_based` Settings that no yaml declares | Low | Dry-run prints target names; `_ENGINE_TO_RUNNER` is editable |

## Open questions (not blocking)

- SPEC 11.1, 11.2 assumed yes.
- D8 reading ("`MOUNTS_PATH` must be absolute for `mount.sh`") still unconfirmed; affects one docs bullet in WP10.
- WP6 and WP7 could be one commit to avoid the transitional argument; kept separate for reviewability.
