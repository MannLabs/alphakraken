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
- 1.2.3 A runner without `locations` uses the existing default view (`locations.*.absolute_path`).
  The docker runner keeps exporting default-view paths and binding host paths onto them.
- 1.2.4 A yaml without `runners:` fails at import with a clear error. No implicit defaults.

## 2. Design

### 2.1 Yaml

```yaml
runners:
  slurm:                          # runner name, referenced by Settings.runner
    engine: slurm                 # one of shared.keys.JobEngines
    # os: linux                   # default; determines the path flavour of `locations`
    # ssh_connection_id_prefix: cluster_ssh_connection   # default
    # locations: (absent)         # -> default view: locations.<x>.absolute_path above
  docker:
    engine: docker
  win_box:                        # illustrative, not in the in-repo yamls
    engine: slurm                 # `ssh` once that handler exists; the factory rejects unknown engines
    os: windows
    ssh_connection_id_prefix: win_box_ssh
    locations:                    # complete view, replaces (never merges with) the default view
      backup: '\\server\share\backup'
      output: 'Z:\alphakraken\output'
      settings: 'Z:\alphakraken\settings'
      software: 'C:\alphakraken\software'
```

- Per-runner `locations` is a flat `location -> path` map (no `absolute_path` sub-key: there is
  no mount information at this level). Values are opaque strings; the flavour comes from `os`.
- All three in-repo yamls (`envs/alphakraken.{local,sandbox,production}.yaml`) get a `runners:`
  block with `slurm` and `docker`, so the migration in 2.6 maps 1:1. The `_test_` stub in
  `shared/yamlsettings.py:73-90` gets `slurm`, `docker`, `file_based`.

### 2.2 `shared/runners.py` (new)

```python
class OperatingSystems(metaclass=ConstantsClass):
    LINUX = "linux"
    WINDOWS = "windows"


@dataclass(frozen=True)
class Runner:
    """A configured place where jobs execute."""

    name: str
    engine: str
    view: View[PurePath]
    ssh_connection_id_prefix: str


RUNNERS: dict[str, Runner]  # built at import from YAMLSETTINGS["runners"], insertion order kept


def get_runner(name: str) -> Runner:
    """Raises KeyError naming the known runners."""
```

- Built at import, like the views in `shared/path_views.py`. Import-time validation: block
  present and non-empty, `engine` in `JobEngines`, `os` in `OperatingSystems`, a windows runner has
  `locations`. Each failure names the runner and the yaml key.
- `os: linux` -> `PurePosixPath`, `os: windows` -> `PureWindowsPath`. Never `Path`: no code does
  filesystem I/O in a runner view.
- No `locations` -> `view = CLUSTER_VIEW`. `CLUSTER_VIEW` keeps its name and stays the view for
  paths persisted for display (`handler_impl.get_backup_base_path`).
- `DEFAULT_SSH_CONNECTION_ID_PREFIX = "cluster_ssh_connection"` moves here from
  `common/constants.py:6`; the two users are repointed.
- Lives in `shared` because the webapp needs the runner names and engines (2.7).

### 2.3 Settings and QuantingEnv

- `Settings.runner = StringField(required=True, max_length=64)`, no default. `job_engine` is
  deleted. `create_settings(runner=...)`.
- `QuantingEnv.job_engine` -> `runner: str = Field(alias="_RUNNER")`. Underscore prefix, so it
  is never exported to a job.

### 2.4 prepare_job

`runner = get_runner(settings.runner)`; the four `CLUSTER_VIEW.resolve` calls in
`processor_impl.py` become `runner.view.resolve`. Parameter types `PurePosixPath` -> `PurePath`.
Relative paths stay posix-separated whatever the runner OS: they are layout, not view. Only the
resolved absolute strings change flavour.

### 2.5 Handler factory and SSH

- `_get_job_handler(runner: Runner)`. `start_job/get_job_status/get_job_result(..., runner_name)`
  look the runner up. `ssh_sensor.py:58` reads `.runner`.
- Slurm: `SlurmSSHJobHandler(runner.view.resolve(Locations.SLURM), runner.ssh_connection_id_prefix)`.
- Docker: unchanged, `DockerJobHandler(DOCKER_HOST_VIEW)`. The host view is a property of the
  worker host, not of a runner.
- `ssh_execute(command, ssh_connection_id_prefix)`, `get_cluster_ssh_hook(attempt_no, prefix, ...)`,
  `_get_cluster_ssh_connections(prefix)`. The `debug_no_cluster_ssh` shortcut is untouched.
- Unknown `engine` still raises `ValueError` in the factory. Adding the SSH handler later is:
  one `JobEngines` constant, one factory branch, one module.

### 2.6 Validation (`_check_content`)

Replace the dump-everything loop with an explicit list. Strict check (no spaces, no absolute):
`relative_raw_file_path`, `relative_output_path`, `speclib_file_name`, `fasta_file_name`,
`config_file_name`, `software`, `software_type`, `metrics_type`, `raw_file_id`, `project_id`,
`settings_name`, `year_month_folder`, `runner`. `config_params` via
`substitute_dummy_values(settings.config_params)` with spaces, as today.
Not checked: `raw_file_path`, `settings_path`, `output_path`, `custom_command` (base from yaml
plus parts checked above), `slurm_time` (as today).

Coverage is equivalent to today's: the absolute fields were only ever "yaml base + relative
part", and the relative part is now checked directly. The allowed character set in
`shared/validation.py` does not change.

### 2.7 Webapp (`webapp/pages_/settings.py`)

- Selectbox options `list(RUNNERS)`; default the first declared runner. `SHOW_JOB_ENGINE_SELECT`
  -> `SHOW_RUNNER_SELECT`. Prefill key `runner`.
- Line 509 check becomes `RUNNERS[runner].engine == JobEngines.DOCKER and software_type != CUSTOM`.
- Help texts at 314 and 415 say "runner".

### 2.8 Migration

`shared/_migrations/from_<current_release>/_migrate_job_engine_to_runner.py`, same shape as
`_migrate_backfill_settings_fields.py`: for each Settings document with `job_engine` and without
`runner`, set `runner` from an editable `_ENGINE_TO_RUNNER` dict (identity by default), unset
`job_engine`. `--dry-run`. Docstring states the precondition: the yaml must declare runners with
those names.

### 2.9 Consistency test

Extend `shared/tests/test_deployment_paths.py`: every in-repo yaml declares `runners`, each
engine and os is known, each windows runner has `locations`.

### 2.10 Docs

- `envs/alphakraken.local.yaml` is the commented reference: document the block there.
- `docs/deployment.md:323-360` (standalone docker section) says "runner" instead of
  "execution engine", and mentions the `runners:` block.

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
shared/runners.py                        new: Runner, RUNNERS, get_runner, OperatingSystems
shared/path_views.py                     unchanged API; CLUSTER_VIEW stays
shared/yamlsettings.py                   YamlKeys.RUNNERS (+ nested keys), _test_ stub gains runners
shared/keys.py                           JobEngines unchanged
shared/db/models.py, shared/db/interface.py   Settings.runner
shared/_migrations/from_<release>/_migrate_job_engine_to_runner.py
shared/tests/test_runners.py             new
shared/tests/test_deployment_paths.py    extended
airflow_src/plugins/jobs/job_handler.py  factory takes Runner
airflow_src/plugins/jobs/slurm_ssh_job_handler.py   takes ssh prefix
airflow_src/plugins/sensors/ssh_utils.py, sensors/ssh_sensor.py
airflow_src/plugins/common/utils.py      ssh discovery by prefix argument
airflow_src/plugins/common/constants.py  CLUSTER_SSH_CONNECTION_ID_PREFIX removed
airflow_src/plugins/common/quanting_env.py   runner field
airflow_src/dags/impl/processor_impl.py  runner.view, _check_content
airflow_src/tests/helpers.py             runner_locations() helper next to yaml_locations()
webapp/pages_/settings.py
envs/alphakraken.{local,sandbox,production}.yaml
docs/deployment.md
```

## 6. Code style

Follows the existing modules. Example of the target style, from `shared/path_views.py`:

```python
def _build_cluster_view() -> View[PurePosixPath]:
    """Build the view of a machine that accesses the data via the shared file system."""
    locations: dict[str, dict[str, str]] = YAMLSETTINGS.get(YamlKeys.LOCATIONS, {})
    absolute_paths = {
        location: values[YamlKeys.ABSOLUTE_PATH]
        for location, values in locations.items()
        if YamlKeys.ABSOLUTE_PATH in values
    }
    return View("cluster", absolute_paths, PurePosixPath)
```

- Yaml keys and engine/os names are constants (`ConstantsClass`), never literals at use sites.
- Comments say why, not what. Docstrings scoped to the public API.
- Imports at module top. Flat `shared/runners.py`, not a package (cf. plan §0.2 shadowing).
- Each chunk is its own commit, green on its own, `pre-commit` clean.

## 7. Testing strategy

pytest, tests next to the existing ones (`shared/tests`, `airflow_src/tests`, `webapp/tests`).

- 7.1 `test_runners.py`: build from a yaml dict; default view fallback; windows runner resolves
  `\\server\share\backup\test1\1970_01\f.raw` and `Z:\...\out_f.raw\alphadia` from the layout
  functions; each import-time validation error; `get_runner` KeyError names known runners.
- 7.2 `test_processor_impl.py`: `prepare_job` with a windows runner (patched `RUNNERS`) yields
  the windows strings in `RAW_FILE_PATH`, `SETTINGS_PATH`, `OUTPUT_PATH`, `CUSTOM_COMMAND`,
  substituted `_CONFIG_PARAMS`, and `_check_content` returns no errors. `_check_content` still
  rejects `..`, `;`, `$` in relative paths, file names, `software`, `config_params`.
- 7.3 Regression: for the `slurm` runner without `locations`, `QuantingEnv.to_dict()` is
  byte-identical to before, except `_JOB_ENGINE` -> `_RUNNER`.
- 7.4 `test_utils.py`: two prefixes select disjoint connection sets.
- 7.5 `test_job_handler.py`: factory per engine with a `Runner`; unknown engine raises.
- 7.6 `webapp/tests`: selectbox options come from `RUNNERS`; docker-only-custom check keyed by
  engine of the selected runner.
- 7.7 Consistency test per 2.9; each assertion shown to fail on a mutated yaml.
- 7.8 Coverage expectation: every new branch in `runners.py` and `_check_content` is hit.

## 8. Boundaries

- **Always:** run the three pytest commands and `pre-commit` before each commit; one chunk per
  commit; keep `CLUSTER_VIEW` for persisted display paths; keep relative paths posix.
- **Ask first:** any change to the allowed character set in `shared/validation.py`; adding a
  `JobEngines` value; touching `docker_job_handler.py` beyond the constructor call site; any
  further change to exported env var names.
- **Never:** implement the SSH handler or a Windows job script here; add backwards
  compatibility for missing `runners:` or old `job_engine`; change the persisted DB paths
  (`RawFile.backup_base_path`, `Metrics.output_path`); generate `mount.sh`; touch
  `msqc-extractor`; commit `BOYSCOUT_*.md`.

## 9. Success criteria

- 9.1 `grep -rn job_engine --include='*.py'` outside `shared/_migrations` returns nothing.
- 9.2 `grep -rn CLUSTER_VIEW` outside `shared/path_views.py`, `shared/runners.py`,
  `handler_impl.py:get_backup_base_path` and tests returns nothing.
- 9.3 Tests 7.1 to 7.7 pass; full suite green (the 5 known `test_dags` env failures excepted
  when `AIRFLOW_HOME` is not set up).
- 9.4 A yaml with a windows runner produces a `QuantingEnv` whose absolute paths are UNC or
  drive-letter strings and that passes `_check_content`.
- 9.5 A `Settings.runner` that is not declared fails the DAG with a message listing the declared
  runners.
- 9.6 A yaml without `runners:` fails at import naming the missing key.
- 9.7 Migration `--dry-run` on a copy of the sandbox DB reports every Settings document exactly
  once.

## 10. Out of scope

The SSH job handler and its Windows job script. Per-runner job scripts. The docker runner's
view (2.2.3 keeps it). `mount.sh` generation. DB-persisted views and the six webapp path TODOs
(D5). `QuantingEnv` view-typed fields (D4). Runner-specific `Pools` (`cluster_slots_pool` still
gates all runners).

## 11. Open questions

- 11.1 Should `settings_path` for a windows runner keep the trailing-slash-free join, i.e. does
  the future Windows job script want `Z:\settings\NAME` exactly? Assumed yes.
- 11.2 `Settings.runner` max length 64: enough for hostnames-as-names? Assumed yes.
