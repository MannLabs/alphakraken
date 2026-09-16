# Upgrading a deployment from v0.10.0

Breaking changes: the `alphakraken.yaml` format, the `ProjectSettings` scope fields, three further
DB fields, the `config_params` placeholder syntax, the output folder layout, and one REST API
field. Four migration scripts ship in `shared/_migrations/from_0.10.0/`. One breaking change has
no migration, see step 7.

**The whole upgrade is a strict cutover in a maintenance window**: stop the schedulers, migrate,
deploy, restart, in that order. There is no compatibility read for the `ProjectSettings` change.
Deploying the new code onto an un-migrated DB loads every assignment with the default `scopes`
(`["*"]`), no exclusions, and the legacy `raw_file_id_filter` string, which the resolver iterates
character by character. Every legacy assignment then fires on every instrument and on nearly
every file. Nothing errors and nothing alerts: the result is silent over-quanting.

Commands below assume `export ENV=production`; substitute your environment.

## 0. Known gap: `config_params` placeholders

Placeholders changed from bare `RAW_FILE_PATH` to `{{RAW_FILE_PATH}}`, and **no migration script
covers this**. `substitute_placeholders` (`shared/config_params.py`) only matches `{{...}}`, and
`check_for_unknown_placeholders` only flags *braced* tokens, so a legacy `--f RAW_FILE_PATH` passes
validation and reaches the quanting software verbatim. This fails silently at runtime, per settings
entry. See step 7.

## 1. Before touching anything

1. Back up `mongodb_data_${ENV}` and `airflowdb_data_${ENV}` (on the machine hosting the DBs).
2. Keep a copy of the **current** `envs/alphakraken.${ENV}.yaml`: the path migration needs
   `locations.output.absolute_path` from it, and the converter drops all comments.
3. In the Airflow UI, set `file_copy_pool` to 0 and wait for all `copy_raw_file` tasks to finish.

## 2. Convert the yaml (on the host)

```bash
PYTHONPATH=. python shared/_migrations/from_0.10.0/_convert_alphakraken_yaml.py envs/alphakraken.${ENV}.yaml
```

Writes `<name>.converted` next to the original. Then:

1. Diff old against converted and **re-add the comments** manually.
2. The converter emits only a **`slurm`** runner. If any settings entry uses `docker` or
   `file_based`, copy the runner block and adapt `name`/`engine` (the `docker` engine needs no
   `ssh_connection_id_prefix`).
3. `locations.slurm` is gone entirely.
4. `instruments.*.mount_target` is dropped; the target is always `instruments/<id>`. The converter
   aborts if an instrument deviates.

This step stays on the host: it writes to `envs/`, and the container's copy of the yaml has the
`username`/`password` lines stripped by the Dockerfile.

## 3. Move `MOUNTS_PATH` to the env file

1. `locations.general.mounts_path` no longer exists. Set `MOUNTS_PATH` in `envs/${ENV}.env` (the
   converter prints the value it found).
2. It **must be absolute**: `docker-compose.yaml` declares `MOUNTS_PATH: ${MOUNTS_PATH:?error}`, so
   compose refuses to start without it, and the `docker` job engine's bind mounts break if relative.

## 4. Cluster: move the submit script

1. `airflow_src/plugins/cluster_scripts/submit_job.sh` is now `misc/software/submit_slurm_job.sh`,
   resolved from the runner's **`software`** location (e.g. `/fs/home/kraken-read/software`), not
   from the former `locations.slurm`.
2. Copy it there and re-apply the `partition`/`nodelist` edits from the old cluster-local copy.
3. Keep it **writable by administrators only**: it now sits in the same folder users reference
   executables from, and AlphaKraken runs it on the cluster for every job.
4. Slurm logs now land in the job's **output** directory, not in the former slurm folder.

## 5. `mount.sh`: entity renamed

`./mount.sh logs fstab` becomes `./mount.sh airflow_logs fstab`. The **mount target folder is
unchanged** (`$MOUNTS_PATH/airflow_logs`), so existing fstab entries stay valid; only the CLI
argument changed.

`mount.sh` now reads `MOUNTS_PATH` from `envs/${ENV}.env`, so that file must be sourceable: replace
every `<placeholder>` first.

## 6. Stop services, pull, run the DB migrations

Run on the machine that hosts the MongoDB, after `git pull` and after rebuilding the images.

The migrations need a container with the `shared` package **and** `MONGO_USER`/`MONGO_PASSWORD`.
Only the worker services get `MONGO_USER_READWRITE` (via `x-airflow-worker`), so use an instrument
worker, e.g. `airflow-worker-astral1`:

```bash
./compose.sh run --rm --build airflow-worker-astral1 bash
```

Inside the container the working directory is `/opt/airflow` and `PYTHONPATH` already covers
`shared`, so:

```bash
python shared/_migrations/from_0.10.0/_migrate_job_engine_to_runner.py --dry-run
python shared/_migrations/from_0.10.0/_migrate_paths_to_relative.py --output-base-path /fs/pool-2/output --dry-run
python shared/_migrations/from_0.10.0/_migrate_project_settings_scopes.py --dry-run
```

Check the output, then re-run all three without `--dry-run`.

The third dry run ends with a **co-firing report**. Read it before continuing. It lists, per
project and instrument, the assignments that the old resolver overrode and that will now run
in addition. Every line there means one more quanting job per raw file for that project. Where
that is not intended, add `excluded_scopes` or a file-name exclude filter on the projects page
after the deploy (the migration cannot infer intent). The report needs the instruments from the
**converted** yaml (step 2), unlike the other two scripts.

Notes:

- `--build` matters: `shared/` is baked into the image, so a container from the pre-upgrade image
  does not contain `shared/_migrations/from_0.10.0/` at all.
- `airflow-cli`, the container documented for other maintenance tasks, does **not** work here: it
  inherits `x-airflow-common` env, which has no `MONGO_USER`/`MONGO_PASSWORD`. `connect_db` falls
  back to the test defaults and only warns, so the failure surfaces later as a mongo auth error.
- Neither migration reads the yaml (their import chain is `shared.db.{engine,models}` ->
  `shared.keys`), so they can run before or after step 2.

What they do:

- `_migrate_job_engine_to_runner.py`: `Settings.job_engine` -> `Settings.runner_name`. The default
  mapping is the identity (runner named after its engine), which matches the converted yaml. It
  prints the target names; **every one must be declared in the `runners:` block**, otherwise the
  migrated settings fail at `prepare_job`.
- `_migrate_paths_to_relative.py`: `Metrics.output_path` -> `Metrics.relative_output_path`, and
  drops `RawFile.backup_base_path`. Pass the `output` absolute path of **every runner that ever
  wrote metrics** (from step 1.2). Documents below none of them are reported and left untouched:
  add the missing base path and rerun.
- `_migrate_project_settings_scopes.py`: `ProjectSettings.scope` -> `scopes` (list),
  `excluded` -> `excluded_scopes`, `raw_file_id_filter` becomes a list, new empty
  `raw_file_id_exclude_filter`. Idempotent. Semantics change with it: **every matching assignment
  runs**, there is no "most specific scope wins" any more. Restriction is explicit via exclusions.

## 7. Fix `config_params`

Rewrite the `config_params` of every active settings entry to the `{{PLACEHOLDER}}` form, either
through the webapp (a new version per entry) or with a one-off script. The webapp's help text and
examples are already updated.

## 8. Rebuild the REST API image

`rest_api/Dockerfile` now copies `envs/alphakraken.*.yaml` into the image at build time (stripping
`username`/`password` lines), because the display paths come from the yaml. The REST API image must
be **rebuilt**, not just restarted, and rebuilt again on every future yaml change.

## 9. Notify API consumers

`RawFileResponse.backup_base_path` is now `backup_path`. Anything parsing the REST API breaks.

## 10. Output layout and capacity

The settings-specific output subfolder is now `<settings_name>_v<version>` instead of
`<software_type>`, e.g. `out_X.raw/fallback_v7/` instead of `out_X.raw/alphadia/`. Existing
folders are untouched: `Metrics.relative_output_path` stores the path per document, so old files
keep showing their old path. In `add` mode the suffixed folders are now `fallback_v7.run2`.

Two consequences of the new layout:

- Re-quanting a file with a **newer settings version** lands in a fresh folder instead of hitting
  `output_exists_mode`. That variable now means "this exact settings version already ran on this
  file". Keep it at `raise` across the deploy window so a layout surprise fails loudly.
- One raw file can now spawn N quanting jobs. Each holds a `CLUSTER_SLOTS_POOL` slot and one of the
  14 job-monitor tasks per DAG for its whole runtime, so files in flight per instrument drop to
  roughly 14/N. The docker runner reserves N times the declared memory on the Airflow host. The
  `quanting` pile-up alert threshold counts files, so it fires earlier. Tune pools and thresholds
  once real N values are known.
- Two settings with the same `metrics_type` on one file still show only one of them in the
  overview (pre-existing limitation). The projects page warns when an instrument resolves two
  settings of the same software type.

## 11. Verify

1. The webapp settings page fails loud on a bad yaml: it shows "No runners are declared" if
   `runners:` is missing or empty, and raises at import if `display_paths` lacks `backup`,
   `output`, `settings` or `software`. Good smoke test.
2. Run one file end to end before restoring `file_copy_pool`. Check that the output folder is
   `out_<file>/<settings_name>_v<version>/`.
3. Open the projects page for each project and compare the "Resolved settings per instrument"
   table against the co-firing report from step 6.

## Checklist of breaking changes

| # | Change | Migration |
|---|--------|-----------|
| 1 | yaml: `locations` -> `mounts` + `display_paths` + `runners` | `_convert_alphakraken_yaml.py` |
| 2 | `MOUNTS_PATH` moves from the yaml to `envs/${ENV}.env`, must be absolute | manual |
| 3 | `mount.sh` entity `logs` -> `airflow_logs` | manual |
| 4 | `submit_job.sh` -> `misc/software/submit_slurm_job.sh`, in the `software` location | manual |
| 5 | `Settings.job_engine` -> `Settings.runner_name` | `_migrate_job_engine_to_runner.py` |
| 6 | `Metrics.output_path` -> `relative_output_path`; `RawFile.backup_base_path` dropped | `_migrate_paths_to_relative.py` |
| 7 | `config_params` placeholders need `{{ }}` | **none, silent failure** |
| 8 | REST API `backup_base_path` -> `backup_path` | manual |
| 9 | REST API image bakes in the yaml | rebuild |
| 10 | `ProjectSettings.scope`/`excluded`/`raw_file_id_filter` -> list fields; all matching assignments run | `_migrate_project_settings_scopes.py`, read its co-firing report |
| 11 | Output subfolder `<software_type>` -> `<settings_name>_v<version>` | none needed, old paths stay stored |
