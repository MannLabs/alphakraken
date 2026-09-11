# Spec: Named runners (D3-lite)

Commit: `e0561ecf` (branch `path_refactoring_4`).
Follows `design_docs/PATH_HANDLING_DESIGN.md` option D3, cut down. D2 is landed.

## 1. Objective

One deployment runs quanting jobs on several *named runners*. A runner is a configured place
where jobs execute: an engine (how), a view (which absolute paths the job sees, in which path
flavour), and the SSH connections to reach it. Hosts whose OS and path flavour differ from the
Airflow worker's (Windows: UNC or drive letter) must be expressible.

This is an upfront refactoring. It prepares the seam for an SSH job handler; it does not add
that handler. No exported string changes for existing deployments, except where §5 says so.

### 1.1 What is wrong today

- 1.1.1 `Settings.job_engine` names *how* (slurm/docker/file_based), never *where*. Two Slurm
  clusters, or one cluster plus one Windows box, cannot be told apart
  (`shared/db/models.py:272`).
- 1.1.2 `prepare_job` resolves every exported path through the single `CLUSTER_VIEW`, whatever
  the engine (`airflow_src/dags/impl/processor_impl.py:123,174,184,266`).
- 1.1.3 SSH connections are found by one global prefix `cluster_ssh_connection`
  (`airflow_src/plugins/common/utils.py:158-168`).
- 1.1.4 `_check_content` rejects every `\` and `:`, so no Windows path can be exported
  (`processor_impl.py:273-320`, `shared/validation.py:14`).

### 1.2 Decisions taken (2026-09-02)

- 1.2.1 `Settings.job_engine` is renamed to `Settings.runner`, with a one-shot DB migration.
- 1.2.2 Validation moves to the user-controlled relative parts; resolved bases count as admin
  configuration. This is the one deliberate behaviour change.
- 1.2.3 Every runner declares its complete `view`. There is no default view and no fallback.
  `locations.<x>.absolute_path` is removed; the one non-job reader of it, the persisted display
  path in `get_backup_base_path`, reads a new key `backup.backup_base_path` in the existing
  top-level `backup` block.
  `CLUSTER_VIEW` is deleted. (Supersedes the earlier fallback decision, 2026-09-02.)
- 1.2.4 A yaml without `runners:`, or with `runners: []`, declares no runners: deployments
  without analysis need none. The webapp then offers no settings form (2.7). No other default.
  (Supersedes the import-time failure, 2026-09-04.)
- 1.2.5 The docker runner keeps binding host paths onto the paths the placeholders resolved to;
  admins copy the old `absolute_path` values into its `view` to keep exported strings equal.
- 1.2.6 With `absolute_path` gone, the per-location entries hold mount information only. They
  move to a new top-level `mounts.<x>` block. The `locations` block disappears entirely. Entries
  that had only an `absolute_path` (`settings`, `software`, `slurm`) are not carried over.
- 1.2.7 `locations.general.mounts_path` is dropped. It duplicated `MOUNTS_PATH` from
  `envs/<env>.env`, and the two already disagree in the local environment. `MOUNTS_PATH` is
  passed into the containers instead and read from the environment. It must be absolute when the
  docker runner is used, because the Docker daemon rejects relative bind sources; this is
  documented as a comment in the `.env` files, not enforced.

## 2. Design

### 2.1 Yaml

```yaml
backup:                           # existing block
  backup_type: local
  purging_verification_type: local
  backup_base_path: /fs/pool-0/alphakraken/backup   # persisted for display, cf. 2.2
  s3: ...

mounts:                           # read by mount.sh and the consistency test only; the mounts folder itself is MOUNTS_PATH in <env>.env
  backup:
    username: user
    mount_src: //mount_src/backup
    mount_target: backup
  output: ...
  logs: ...
  # settings, software, slurm are not mounted and do not appear here

runners:
  - name: slurm                   # referenced by Settings.runner; unique within the list
    engine: slurm                 # one of shared.keys.JobEngines
    os: linux                     # required; linux | macos | windows, determines the path flavour of `view`
    ssh_connection_id_prefix: cluster_ssh_connection   # required for engines that use SSH
    view:                         # the data directories as seen from this runner, complete
      backup: /fs/pool-0/alphakraken/backup
      output: /fs/pool-0/alphakraken/output
      settings: /fs/pool-0/alphakraken/settings
      software: /fs/home/kraken-read/software
      slurm: /fs/pool-0/alphakraken/slurm
  - name: docker
    engine: docker
    os: linux
    # no ssh_connection_id_prefix: the docker engine does not use SSH, the key would be ignored
    view:                         # paths inside the job container, cf. 1.2.5
      backup: /fs/pool-0/alphakraken/backup
      output: /fs/pool-0/alphakraken/output
      settings: /fs/pool-0/alphakraken/settings
      software: /fs/home/kraken-read/software
  - name: win_box                 # illustrative, not in the in-repo yamls
    engine: slurm                 # `ssh` once that handler exists; the factory rejects unknown engines.
                                  # NOTE: this entry validates but cannot run: the slurm handler builds
                                  # bash and gets a windows path. Illustrates the yaml shape only.
    os: windows
    ssh_connection_id_prefix: win_box_ssh
    view:
      backup: '\\server\share\backup'
      output: 'Z:\alphakraken\output'
      settings: 'Z:\alphakraken\settings'
      software: 'C:\alphakraken\software'
```

- `runners` is a list; each entry carries its `name`. Order is the display order in the webapp.

- Per-runner `view` is a flat `location -> path` map (no `absolute_path` sub-key: there is
  no mount information at this level). Values are opaque strings; the flavour comes from `os`.
  The key is named `view`, not `locations`, to match `Runner.view` and the `View` class.
  Two runners on one file system repeat the paths; yaml anchors are available if that hurts.
- A location a runner does not declare is unreachable and fails at `View.resolve` with the
  existing error naming view and location.
- `mounts.<x>` is not read by any Python view. `DOCKER_HOST_VIEW` derives from the
  `MOUNTS_PATH` environment variable (`EnvVars.MOUNTS_PATH`, passed through the
  `airflow-common-env` block of `docker-compose.yaml`) plus the fixed location names. A missing
  variable yields an empty view, reported by the factory when the docker engine is selected, as
  today.
- All three in-repo yamls (`envs/alphakraken.{local,sandbox,production}.yaml`) move their
  `absolute_path` values into a `slurm` and a `docker` runner and gain `backup_base_path` with
  the value of the former `locations.backup.absolute_path`, so the migration in 2.8 maps 1:1.
  `view.backup` of the `slurm` runner holds the same value; the consistency test (2.9) asserts
  the equality. The `_test_` stub in `shared/yamlsettings.py:73-90` gets `slurm`, `docker`,
  `file_based` with the current test paths, gains
  `backup: {backup_base_path: ./tmp/test/backup}` (today's `locations.backup.absolute_path`) and
  loses `general.mounts_path`; the test conftests set `MOUNTS_PATH` in the environment next to
  `ENV_NAME`.

### 2.2 `shared/runners.py` (new)

```python
class OperatingSystems(metaclass=ConstantsClass):
    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"


@dataclass(frozen=True)
class Runner:
    """A configured place where jobs execute."""

    name: str
    engine: str
    os: str  # one of OperatingSystems; kept for the future SSH handler (job script per OS)
    view: View[PurePath]
    ssh_connection_id_prefix: str | None  # optional in yaml; engines that need it check for None


def _build_runners(entries: list[dict]) -> dict[str, Runner]:
    """Validate the yaml `runners` list and build the runners, order kept."""


RUNNERS: dict[str, Runner] = _build_runners(YAMLSETTINGS[YamlKeys.RUNNERS])  # keyed by name


def get_runner(name: str) -> Runner:
    """Raises KeyError naming the known runners."""
```

- Built at import, like the views in `shared/path_views.py`. All validation lives in
  `_build_runners`, which the tests call directly (7.1). Import-time validation: list
  present and non-empty, every entry has a `name`, names unique, `engine` in `JobEngines`, `os`
  present and in `OperatingSystems`, `view` present and every key of it in `Locations`.
  `ssh_connection_id_prefix` is optional here and not interpreted: the loader knows nothing about
  which engines use SSH. Which locations an engine needs is likewise not checked here; a missing
  one fails at first use via `View.resolve`, naming view and location. No key has a default. Each
  failure names the runner and the yaml key.
- `os: linux` and `os: macos` -> `PurePosixPath`, `os: windows` -> `PureWindowsPath`. `macos`
  exists for completeness and is treated exactly like `linux`. Never `Path`: no code does
  filesystem I/O in a runner view.
- `view` is required for every runner; `Runner.view = View(name, yaml_view, path_class)`.
- `CLUSTER_VIEW` and `_build_cluster_view` are deleted from `shared/path_views.py`. Its one
  non-job reader, `handler_impl.get_backup_base_path`, becomes
  `PurePosixPath(BACKUP_BASE_PATH) / get_raw_file_folder_rel_path(raw_file)` with
  `BACKUP_BASE_PATH` read from `backup.backup_base_path` in `shared/yamlsettings.py`,
  missing key raising at import like the runners do.
- `CLUSTER_SSH_CONNECTION_ID_PREFIX` (`common/constants.py:6`) is deleted without replacement;
  the prefix always comes from the runner. The error text in `get_cluster_ssh_hook` names the
  prefix it was given.
- Lives in `shared` because the webapp needs the runner names and engines (2.7).

### 2.3 Settings and QuantingEnv

- `Settings.runner = StringField(required=True, max_length=64)`, no default. `job_engine` is
  deleted. `create_settings(runner=...)`.
- `QuantingEnv.job_engine` -> `runner_name: str = Field(alias="_RUNNER_NAME")`. Underscore prefix, so it
  is never exported to a job.

### 2.4 prepare_job

`runner = get_runner(settings.runner)`; the four `CLUSTER_VIEW.resolve` calls in
`processor_impl.py` become `runner.view.resolve`. Parameter types `PurePosixPath` -> `PurePath`.
Relative paths stay posix-separated whatever the runner OS: they are layout, not view. Only the
resolved absolute strings change flavour.

### 2.5 Handler factory and SSH

- `_get_job_handler(runner: Runner)`. `start_job/get_job_status/get_job_result(..., runner_name)`
  look the runner up. `ssh_sensor.py:58` reads `.runner_name`.
- Slurm: `SlurmSSHJobHandler(runner.view.resolve(Locations.SLURM), runner.ssh_connection_id_prefix)`.
  The factory raises `AirflowFailException` naming the runner if the prefix is `None`.
- Docker: unchanged, `DockerJobHandler(DOCKER_HOST_VIEW)`. The host view is a property of the
  worker host, not of a runner.
- `ssh_execute(command, ssh_connection_id_prefix)`, `get_cluster_ssh_hook(attempt_no, prefix, ...)`,
  `_get_cluster_ssh_connections(prefix)`. The `debug_no_cluster_ssh` shortcut is untouched.
- Unknown `engine` still raises `ValueError` in the factory. Adding the SSH handler later is:
  one `JobEngines` constant, one factory branch, one module.

#### 2.5.1 Why SSH credentials stay in Airflow Connections

Considered and rejected (2026-09-02): moving host, user and password into the runner block.

- The yaml is read by every Airflow component and by the webapp; only the workers running
  `ssh_execute` need the credentials. Airflow Connections are Fernet-encrypted in the metadata DB
  and reachable from Airflow only.
- The yaml is loaded once at import; rotating a password would need a container restart.
  Connections change live and can be tested in the UI.
- Reproducible, file-based setup is available without moving secrets:
  `AIRFLOW_CONN_<ID>=ssh://user:pw@host` in `envs/<env>.env`, next to `MONGO_PASSWORD`.

#### 2.5.2 Why a prefix, not a list of connection ids

A `ssh_connection_ids: [...]` list would make the runner self-contained, but adding or removing a
head node would then touch the yaml and restart the containers. With a prefix, connections are
added and removed in Airflow alone, and the existing round-robin discovery
(`_get_cluster_ssh_connections`) is reused unchanged. The list is a possible follow-up.

Both rationales are recorded as comments next to `ssh_connection_id_prefix` in
`envs/alphakraken.local.yaml` and in `docs/deployment.md` (SSH connection section), cf. 2.10.

### 2.6 Validation (`_check_content`)

Replace the dump-everything loop with an explicit list. Strict check (no spaces, no absolute):
`relative_raw_file_path`, `relative_output_path`, `speclib_file_name`, `fasta_file_name`,
`config_file_name`, `software_type`, `metrics_type`, `raw_file_id`, `project_id`,
`settings_name`, `year_month_folder`, `runner`, `slurm_mem` (ends up in `sbatch --mem=`).
`software` with `allow_absolute_paths=True`, as today (an absolute `software` is a valid config).
`config_params` via `substitute_dummy_values(settings.config_params)` with spaces, as today.
Not checked: `raw_file_path`, `settings_path`, `output_path`, `custom_command` (base from yaml
plus parts checked above), `slurm_time` (as today).
Add one `TODO: revisit validation: which fields need which check, and where (webapp vs. here)`
above the list.

Coverage is equivalent to today's: the absolute fields were only ever "yaml base + relative
part", and the relative part is now checked directly. The allowed character set in
`shared/validation.py` does not change.

### 2.7 Webapp (`webapp/pages_/settings.py`)

- Selectbox options `list(RUNNERS)`; default the first declared runner. `SHOW_JOB_ENGINE_SELECT`
  -> `SHOW_RUNNER_SELECT`. Prefill key `runner`. With no runners declared (1.2.4) the page shows a
  notice instead of the settings form.
- Line 509 check becomes `RUNNERS[runner].engine == JobEngines.DOCKER and software_type != CUSTOM`.
- Help texts at 314 and 415 say "runner".

### 2.8 Migration

`shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py`, same shape as
`_migrate_backfill_settings_fields.py`: for each Settings document with `job_engine` and without
`runner`, set `runner` from an editable `_ENGINE_TO_RUNNER` dict (identity by default), unset
`job_engine`. `--dry-run`; at the end, print the distinct target runner names with their counts,
so they can be compared with the yaml (the sandbox may hold `file_based` Settings, which no
in-repo yaml declares). Docstring states the precondition: the yaml must declare runners with
those names.

### 2.9 Consistency test

Extend `shared/tests/test_deployment_paths.py`: every in-repo yaml declares `runners`, each
engine and os is known, each runner has `view`, `backup.backup_base_path` is present and equals
the `slurm` runner's `view.backup`, no
top-level `locations` key exists, every `mounts.<x>` entry has `mount_src` and `mount_target`,
and each in-repo `slurm` runner declares all five locations it uses (the import-time check only
rejects unknown keys). The existing mount-target assertions (`test_deployment_paths.py:84-98`)
iterate `mounts` instead of `locations`.

### 2.9a mount.sh

`mount.sh:52-56` sets `ENTITY_TYPE` to `mounts` instead of `locations` for `backup`/`output`/
`logs`; `mounts.<x>` has the same depth as `instruments.<x>`, so `get_data` is unchanged. The
mounts folder comes from `MOUNTS_PATH` in `envs/${ENV}.env`, which the script sources, instead of
the yaml. Behaviour of the generated fstab line is unchanged.

### 2.10 Docs

- `envs/alphakraken.local.yaml` is the commented reference: document the block there, including
  a one-line version of 2.5.1 and 2.5.2 next to `ssh_connection_id_prefix`.
- `docs/deployment.md`, "Setup SSH connection" section: state that credentials stay in Airflow
  and why, and how a runner selects its connections by prefix.
- `docs/deployment.md:323-360` (standalone docker section) says "runner" instead of
  "execution engine", mentions the `runners:` block, and drops the instruction to keep
  `locations.general.mounts_path` and `MOUNTS_PATH` in sync (lines 232 and 346), replacing it
  with "`MOUNTS_PATH` must be absolute for the docker runner".
- `envs/{local,sandbox,production}.env`: comment on `MOUNTS_PATH` saying it must be absolute
  when the docker runner is used. The local value stays relative.
- `docs/deployment.md`, mounting section: `MOUNTS_PATH` must be absolute when `mount.sh` is
  used, a relative value resolves against the current directory and yields a relative fstab
  line.
- `docs/deployment.md`, upgrade notes: deploy the new yaml and code together, then run the
  migration (2.8) before any quanting DAG runs. New code without the migration fails every job
  (`Settings.runner` is unset); the new yaml on old code fails at import (no `locations`).
- `shared/config_params.py:30,33`: relative paths are "relative to the runner's backup/output
  location" instead of naming `locations.<x>.absolute_path`.

## 3. Tech stack

Python 3.11+ (Airflow 2.11 image), pydantic v2, mongoengine, streamlit, pytest, ruff (`ALL`),
`ty`. No new dependencies.

## 4. Commands

```
conda activate alphakraken2
export AIRFLOW_HOME=<writable dir>; airflow db init      # once, else 5 test_dags failures
pytest shared
pytest webapp
pytest airflow_src --ignore=airflow_src/tests/plugins/jobs/test_docker_job_handler.py   # unless `docker` is installed
pre-commit run --all-files
```

CI runs the three `pytest` commands separately (`.github/workflows/branch-checks.yaml:36-52`).

## 5. Project structure

```
shared/runners.py                        new: Runner, _build_runners, RUNNERS, get_runner, OperatingSystems
shared/path_views.py                     CLUSTER_VIEW and _build_cluster_view removed; DOCKER_HOST_VIEW reads EnvVars.MOUNTS_PATH
shared/yamlsettings.py                   YamlKeys.RUNNERS (+ nested), YamlKeys.MOUNTS, YamlKeys.Backup.BACKUP_BASE_PATH; YamlKeys.LOCATIONS, ABSOLUTE_PATH, Locations removed; BACKUP_BASE_PATH; _test_ stub
shared/keys.py                           EnvVars.MOUNTS_PATH
docker-compose.yaml                      MOUNTS_PATH added to airflow-common-env
envs/{local,sandbox,production}.env      comment: MOUNTS_PATH absolute for the docker runner
mount.sh                                 reads mounts.<x>, sources envs/${ENV}.env for MOUNTS_PATH
shared/keys.py                           JobEngines unchanged
shared/db/models.py, shared/db/interface.py   Settings.runner
shared/_migrations/from_0.9.0/_migrate_job_engine_to_runner.py
shared/tests/test_runners.py             new
shared/tests/test_deployment_paths.py    extended
airflow_src/plugins/jobs/job_handler.py  factory takes Runner
airflow_src/plugins/jobs/slurm_ssh_job_handler.py   takes ssh prefix
airflow_src/plugins/jobs/docker_job_handler.py      docstring only: MOUNTS_PATH instead of locations.general.mounts_path
airflow_src/plugins/jobs/_experimental/file_based_job_handler.py   docstring only: runner view instead of locations.software.absolute_path
airflow_src/plugins/sensors/ssh_utils.py, sensors/ssh_sensor.py
airflow_src/plugins/common/utils.py      ssh discovery by prefix argument
airflow_src/plugins/common/constants.py  CLUSTER_SSH_CONNECTION_ID_PREFIX removed
airflow_src/plugins/common/quanting_env.py   runner field
airflow_src/dags/impl/processor_impl.py  runner.view, _check_content
airflow_src/dags/impl/handler_impl.py    get_backup_base_path reads BACKUP_BASE_PATH
airflow_src/tests/helpers.py             yaml_locations() -> runner_view(name, **paths)
webapp/pages_/settings.py
envs/alphakraken.{local,sandbox,production}.yaml
docs/deployment.md
# comment/docstring rewording only, so that 9.2 holds; existing TODOs stay TODOs:
shared/path_views.py                     Locations docstring ("keys of the `locations` section")
shared/tests/test_deployment_paths.py    line 52 docstring
webapp/pages_/settings.py                TODOs at 311 (`cf. CLUSTER_VIEW`) and 381 (`locations.backup.absolute_path`)
envs/alphakraken.local.yaml              line 27 comment
airflow_src/dags/impl/handler_impl.py    line 68 comment
airflow_src/plugins/common/constants.py  line 10 comment
airflow_src/tests/plugins/jobs/test_docker_job_handler.py   line 37 comment
docs/deployment.md                       line 187 (`locations.slurm.absolute_path` -> the slurm runner's `view.slurm`)
```

## 6. Code style

Follows the existing modules. Example of the target style, from `shared/path_views.py`:

```python
def resolve(self, location: str, rel_path: PurePath | str = "") -> _P:
    """Get the absolute path of `rel_path`, which is relative to `location`, in this view."""
    if location not in self._locations:
        raise KeyError(
            f"Location '{location}' is not reachable in the '{self._name}' view, "
            f"reachable are: {sorted(self._locations)}."
        )
    return self._locations[location] / rel_path
```

- Yaml keys and engine/os names are constants (`ConstantsClass`), never literals at use sites.
- Comments say why, not what. Docstrings scoped to the public API.
- Imports at module top. Flat `shared/runners.py`, not a package (cf. plan §0.2 shadowing).
- Each chunk is its own commit, green on its own, `pre-commit` clean.

## 7. Testing strategy

pytest, tests next to the existing ones (`shared/tests`, `airflow_src/tests`, `webapp/tests`).

- 7.1 `test_runners.py`: `_build_runners` from a yaml list; missing `name`, duplicate `name`, missing `os`,
  missing `view`, unknown location key, unknown `os` each fail; a prefix on a `docker` runner and a
  runner without `slurm` are accepted; `macos` yields the same view as `linux`; windows runner resolves
  `\\server\share\backup\test1\1970_01\f.raw` and `Z:\...\out_f.raw\alphadia` from the layout
  functions; `get_runner` KeyError names known runners.
- 7.2 `test_processor_impl.py`: `prepare_job` with a windows runner (patched `RUNNERS`) yields
  the windows strings in `RAW_FILE_PATH`, `SETTINGS_PATH`, `OUTPUT_PATH`, `CUSTOM_COMMAND`,
  substituted `_CONFIG_PARAMS`, and `_check_content` returns no errors. `_check_content` still
  rejects `..`, `;`, `$` in relative paths, file names, `software`, `config_params`.
- 7.3 Regression: for a `slurm` runner whose `view` equals the former `absolute_path`
  values, `QuantingEnv.to_dict()` is byte-identical to before, except `_JOB_ENGINE` -> `_RUNNER_NAME`.
  `get_backup_base_path` yields the same string as before for the same yaml values.
- 7.4 `test_utils.py`: two prefixes select disjoint connection sets.
- 7.5 `test_job_handler.py`: factory per engine with a `Runner`; unknown engine raises; a
  `slurm` runner without `ssh_connection_id_prefix` raises naming the runner.
- 7.6 `webapp/tests`: selectbox options come from `RUNNERS`; docker-only-custom check keyed by
  engine of the selected runner.
- 7.7 Consistency test per 2.9; each assertion shown to fail on a mutated yaml.
- 7.8 `test_path_views.py`: `DOCKER_HOST_VIEW` built from `MOUNTS_PATH` in the environment; unset
  variable yields a view that reaches nothing.
- 7.9 Coverage expectation: every new branch in `runners.py` and `_check_content` is hit.

## 8. Boundaries

- **Always:** run the three pytest commands and `pre-commit` before each commit; one chunk per
  commit; keep relative paths posix; keep `get_backup_base_path` independent of any runner.
- **Ask first:** any change to the allowed character set in `shared/validation.py`; adding a
  `JobEngines` value; touching `docker_job_handler.py` beyond the constructor call site and the
  module docstring; any further change to exported env var names.
- **Never:** implement the SSH handler or a Windows job script here; add backwards
  compatibility for missing `runners:` or old `job_engine`; change the persisted DB paths
  (`RawFile.backup_base_path`, `Metrics.output_path`); generate `mount.sh`; touch
  `msqc-extractor`; commit `BOYSCOUT_*.md`.

## 9. Success criteria

- 9.1 `grep -rn job_engine --include='*.py'` outside `shared/_migrations` returns nothing.
- 9.2 `grep -rn 'CLUSTER_VIEW\|absolute_path\|mounts_path\|locations\.[a-z*]*\.\|"locations"\|^locations:\|YamlKeys.LOCATIONS'`
  over `*.py`, `*.sh`, `*.md` and `envs/*.yaml` returns only the migration scripts and the
  design docs. `InternalPaths.MOUNTS_PATH` (the container-side constant) is unaffected and not
  part of this criterion.
- 9.3 Tests 7.1 to 7.7 pass; full suite green (the 5 known `test_dags` env failures excepted
  when `AIRFLOW_HOME` is not set up).
- 9.4 A yaml with a windows runner produces a `QuantingEnv` whose absolute paths are UNC or
  drive-letter strings and that passes `_check_content`.
- 9.5 A `Settings.runner` that is not declared fails the DAG with a message listing the declared
  runners.
- 9.6 A yaml without `runners:` imports with an empty `RUNNERS`; the webapp shows no settings
  form.
- 9.7 Migration `--dry-run` on a copy of the sandbox DB reports every Settings document exactly
  once.

## 10. Out of scope

The SSH job handler and its Windows job script. Per-runner job scripts. Changing how the docker
handler binds paths (1.2.5 keeps it). Generating `mount.sh` from the yaml (only its key lookup
moves, 2.9a). DB-persisted views and the six webapp path TODOs
(D5). `QuantingEnv` view-typed fields (D4). Runner-specific `Pools` (`cluster_slots_pool` still
gates all runners).

## 11. Open questions

- 11.1 Should `settings_path` for a windows runner keep the trailing-slash-free join, i.e. does
  the future Windows job script want `Z:\settings\NAME` exactly? Assumed yes.
- 11.2 `Settings.runner` max length 64: enough for hostnames-as-names? Assumed yes.
