# Path handling refactoring: implementation plan (D2)

Commit: `0823eb52` (branch `path_refactoring`).
Implements option **D2** of `PATH_HANDLING_DESIGN.md`. D3 (runner objects) is a follow-up and is
deliberately not started here; D2 only has to avoid blocking it.

## 0. Ground rules

- 0.1 Each chunk is a separate commit, green on its own. No chunk changes behavior;
  the whole plan is a pure refactoring.
- 0.2 **Module naming:** flat modules `shared/path_layout.py` and `shared/path_views.py`, not a
  `shared/paths/` package. `./shared` is on `pythonpath` (`pyproject.toml:50-58`), so a
  `shared/paths` package would also be importable as `paths` - two module objects for one file,
  two `AIRFLOW_CONTAINER_VIEW` singletons. `path_layout` / `path_views` are unshadowable.
- 0.3 **Flavor from day one:** `View` takes the path class it builds its locations with -
  `Path` for the machine this code runs on, `PurePosixPath` / `PureWindowsPath` for any other -
  even though every view is posix today. Retrofitting flavor after ~50 call sites exist is the
  expensive version.
- 0.4 Verification per chunk: `pytest shared`, `pytest airflow_src`, `pytest webapp`
  (conda env `alphakraken`, `AIRFLOW_HOME` exported and `airflow db init` run once, `--ignore`
  the docker job handler tests unless `docker` is installed).

## 1. Chunks

### C1 - `shared/path_layout.py`, output layout moved

- **Goal:** one view-free home for the output layout.
- **Do:** move `get_output_folder_rel_path` (`airflow_src/plugins/common/paths.py:39-63`)
  verbatim into `shared/path_layout.py`. Move `OUTPUT_FOLDER_PREFIX` with it, or import it.
  Repoint the 2 callers (`common/paths.py:78`, `processor_impl.py:29,180`).
- **Done when:** no logic changed, existing tests pass unmoved except for the import path.
- **Note:** `shared/_migrations/from_0.8.0/_backfill_metrics_output_path.py:31-51` holds a frozen
  copy of the same logic. Leave it - it is a historical one-shot script.

### C2 - Raw-file layout de-duplicated

- **Goal:** kill §6.3 - the `<instrument>/<YYYY_MM>[/<raw_file_id>]` layout exists 3x.
- **Do:** add to `path_layout.py`:
  - `raw_file_folder_rel_path(raw_file)` -> `<instrument_id>/<YYYY_MM>`
  - `raw_file_rel_path(raw_file)` -> `<instrument_id>/<YYYY_MM>/<raw_file_id>`

  Repoint `processor_impl.py:123-126`, `handler_impl.py:326-330`
  (`get_backup_base_path`) and `CopyPathProvider.get_target_folder_path`
  (`raw_file_wrapper_factory.py:220-224`).
- **Done when:** the three sites call the layout functions; no string literal of that layout
  survives outside `path_layout.py`.
- **Risk:** `get_backup_base_path` feeds `RawFile.backup_base_path` in the DB. Assert the
  produced strings are byte-identical in a test before and after.

### C3 - `shared/path_views.py`, no callers

- **Goal:** the view table exists and is tested, nothing uses it yet.
- **Do:**
  - `Locations` constants: `INSTRUMENTS, BACKUP, OUTPUT, SETTINGS, SOFTWARE, SLURM, LOGS`.
    A `ConstantsClass` like `InternalPaths` and `JobEngines`, not an enum: `StrEnum` needs
    Python 3.11 and the python version of `apache/airflow:2.11.0` is unverified.
  - `View(name, locations: dict[str, str], path_class)` with `.resolve(location, rel) -> PurePath`,
    `.has(location) -> bool`, and a `KeyError` naming both view and location when a location is absent
    (this is where the §2 holes become explicit).
  - `AIRFLOW_CONTAINER_VIEW` is a module constant built from `InternalPaths` (`shared/keys.py:48-56`), carrying
    `Path` because it is the only view this code does filesystem I/O in.
  - `CLUSTER_VIEW` (from `locations.<location>.absolute_path`) and `DOCKER_HOST_VIEW` (from
    `locations.general.mounts_path` plus the container-relative location names) are constants too,
    built at import. `airflow_src/tests/helpers.py:yaml_locations` overrides them by patching the
    contents of the view object, not the name, so the patch reaches every import site. Both carry
    `PurePosixPath`, which also makes a foreign path impossible to `stat()` by accident.
  - A missing `locations.general.mounts_path` yields a `DOCKER_HOST_VIEW` that reaches nothing,
    rather than an error: the key is needed by the `docker` job engine only, and raising at import
    would break every slurm-only deployment that omits it.
- **Done when:** unit tests cover resolve, the missing-location error, and a windows-flavor view
  resolving to `\\srv\share\backup\...` and `Z:\backup\...` from the same layout input.
  Needs a `shared/tests/conftest.py` setting `ENV_NAME=_test_`: CI runs `pytest shared` on its
  own, and importing the module loads the yaml.
- **Note:** `Locations`, `InternalPaths.{INSTRUMENTS,BACKUP,OUTPUT}` and `YamlKeys.Locations.*`
  are three copies of the same strings until C4/C5 delete the latter two. A test guards them
  against divergence in the meantime.
- **Note:** `Locations.LOGS` exists in the cluster and host views only for prod/sandbox; the
  container log path `/opt/airflow/logs` is outside `MOUNTS_PATH` and stays out of the table.

### C4 - Container view cutover, API unchanged

- **Goal:** `common/paths.py` stops knowing `InternalPaths`.
- **Do:** reimplement `get_internal_*` (5 functions) as one-liners over
  `AIRFLOW_CONTAINER_VIEW.resolve(...)` plus `path_layout`. Their signatures and return types stay identical,
  so the ~41 call sites in 12 files are untouched.
- **Done when:** `InternalPaths` is imported only by `path_views.py`
  (plus `docker_job_handler.py` until C6).

### C5 - Cluster view cutover, `get_path` removed

- **Goal:** the cluster view has exactly one entry point.
- **Do:** replace the 6 sites - `processor_impl.py:122,178,188,272`, `handler_impl.py:328`,
  `job_handler.py:23` - with `CLUSTER_VIEW.resolve(Locations.X, ...)`. Delete
  `shared/yamlsettings.py:get_path` and repoint its test helpers.
- **Done when:** `grep get_path(` returns nothing outside `path_views.py`.
- **Risk:** the only chunk touching the strings exported to the cluster
  (`QuantingEnv.raw_file_path`, `settings_path`, `output_path`, `custom_command`). Compare a
  full `QuantingEnv.to_dict()` before/after in a test.

### C6 - Host view cutover

- **Goal:** remove the container->host `relative_to` round-trip.
- **Do:** the handler is constructed with `DOCKER_HOST_VIEW` rather than with
  `get_host_mounts_path()` (`job_handler.py:35`); delete `get_host_mounts_path`.
  `DockerJobHandler._to_host_path` disappears entirely: `QuantingEnv` carries
  `relative_raw_file_path` and `relative_output_path`, so each side resolves in its own view and
  nothing ever crosses between views.
  - This replaces `_INTERNAL_OUTPUT_PATH` / `_INTERNAL_RAW_FILE_PATH` with
    `_RELATIVE_RAW_FILE_PATH`, which lands §6.5 (`QuantingEnv` carrying both views of the same
    object) ahead of D3/D5. Safe: both handlers strip `_`-prefixed keys, so these fields never
    leave the process.
  - Needs a `container_locations()` test helper next to `yaml_locations()`: tests used to redirect
    output to a `tmp_path` by overriding the resolved path in the env, which is no longer a field.
  - The factory reports a missing `locations.general.mounts_path` when the `docker` engine is
    selected, restoring the actionable message that C3's lenient `DOCKER_HOST_VIEW` gave up.
- **Done when:** `shared/yamlsettings.py` no longer exports any path accessor.
- **Note:** the 13 `test_docker_job_handler.py` tests need the optional `docker` package. To run
  them without it, put a stub package (`from_env`, `errors.ImageNotFound/NotFound`,
  `models.containers.Container`) on `PYTHONPATH`.

### C7 - Consistency test

- **Goal:** attack §6.6 - `InternalPaths`, `docker-compose.yaml` mount targets and yaml
  `mount_target` agree by convention only.
- **Do:** `shared/tests/test_deployment_paths.py` parses `docker-compose.yaml` and every
  `envs/alphakraken.*.yaml` and asserts what is derivable in-repo:
  - each bind below the mounts folder puts the data at the same relative path on both sides -
    this is what lets the docker host view be derived from `locations.general.mounts_path`;
  - nothing is mounted into the containers below a location the container view does not know;
  - every location of the container view is backed by at least one bind;
  - each `locations.<x>.mount_target` equals `<x>`, and each instrument's `mount_target` is
    `instruments/<instrument_id>`;
  - the one deliberate exception - the logs, mounted from `airflow_logs` to `/opt/airflow/logs`,
    outside the mounts folder - is pinned so that it stays deliberate.
- **Done when:** each assertion has been shown to fail on a mutation of the config it guards.
- **Note:** `mount_src` is deliberately not checked against `absolute_path`. The former is an SMB
  share path, the latter a cluster filesystem path; where a share is rooted on the server is not
  derivable from this repo. The production `backup` entry (`//samba-pool-1/pool-1` vs
  `/fs/pool-1/backup`) is the only one whose share path does not end in the same component as its
  absolute path. That is either a different export convention for `pool-1` (fine) or a real
  misconfiguration; `ls <mounts_path>/backup` on the worker host settles it - instrument folders
  mean fine, a nested `backup` folder means the mount is one level off.

## 2. Dependency order

```
C1 -> C2
C3 -> C4 -> C5 -> C6
C3 -> C7
```

C1/C2 and C3 are independent and can be done in either order. C4-C6 are sequential only because
they progressively empty `yamlsettings` and `InternalPaths`.

## 3. Explicitly out of scope for D2

- `Settings.job_engine`, the yaml `runners:` block, per-runner SSH connection ids (D3).
- `QuantingEnv` field changes, the `internal_` prefix convention, `_check_content`'s
  whitelist-by-field-name (D3/D4).
- `check_for_malicious_content` flavor awareness (needed before any Windows path is exported,
  cf. design §6.1 - but nothing exports one until D3).
- The DB-persisted views (`RawFile.backup_base_path`, `Metrics.output_path`,
  `RawFile.file_info` keys) and the 6 webapp TODOs (D5).
- Generating `mount.sh` from the view table.
- The msqc-extractor container.
