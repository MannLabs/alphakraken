# Path handling: current state

Commit: 2674104e (branch `quanting_env_to_pydantic`)

Scope: `airflow_src`, `shared`, `webapp`, `envs`, `docker-compose.yaml`, `mount.sh`.
The `msqc-extractor` docker container is deliberately out of scope.

## 1. The path "views" (same tree, different coordinate systems)

| # | View | Where it comes from | Example |
|---|---|---|---|
| 1.1 | **Relative paths** | derived, several different anchors — see §3 | `test1/2025_07/f.raw` |
| 1.2 | **Container view** (Airflow worker) | hardcoded `InternalPaths` (`shared/keys.py:48-56`) | `/opt/airflow/mounts/backup/test1/2025_07/f.raw` |
| 1.3 | **Cluster / shared-FS view** | `locations.<root>.absolute_path` in `envs/alphakraken.<env>.yaml` | `/fs/pool-1/backup/test1/2025_07/f.raw` |
| 1.4 | **SMB source view** | `mount_src` / `mount_target` per entry | `//samba-pool-1/pool-1` |
| 1.5 | **Docker-host view** | `locations.general.mounts_path` | `/home/kraken-user/alphakraken/production/mounts/backup/...` |

Only 1.1-1.3 and 1.5 exist in Python. 1.4 is consumed exclusively by `mount.sh` (bash plus inline python yaml reads).

## 2. Roots x views (the ragged matrix)

| Root | container | cluster | host | mount info |
|---|---|---|---|---|
| `instruments` | yes | no | yes | yes |
| `backup` | yes | yes | yes | yes |
| `output` | yes | yes | yes | yes |
| `settings` | no | yes | no | no |
| `software` | no | yes | no | no |
| `slurm` | no | yes | no | no |
| `logs` | (`/opt/airflow/logs`) | yes (prod/sandbox only) | yes | yes |

The holes are why there are two unrelated accessor families instead of one.

## 3. Relative-path anchors (four different ones)

- 3.1 relative to **backup root** — `Path(instrument_id)/YYYY_MM/raw_file_id`, built ad hoc in `processor_impl.py:123-126`; also the *old* `file_info` key format.
- 3.2 relative to **`backup_base_path`** — current `file_info` keys (`shared/db/models.py:137`). The old/new mismatch is patched at `file_checks.py:76-84`.
- 3.3 relative to **output root** — `get_output_folder_rel_path()` (`common/paths.py:39-63`), exported as `RELATIVE_OUTPUT_PATH`.
- 3.4 relative to **nothing** — `RemovePathProvider.get_target_folder_path()` returns `Path()` as a sentinel meaning "relative" (`raw_file_wrapper_factory.py:270-276`).

## 4. Where paths are *defined*

- 4.1 `shared/keys.py:48-56` — `InternalPaths`: container roots, hardcoded strings.
- 4.2 `shared/yamlsettings.py:107-136` — `get_path(key)` (cluster view) and `get_host_mounts_path()` (host view); `YamlKeys.Locations` are the root names.
- 4.3 `airflow_src/plugins/common/paths.py` (79 lines) — the container-view builders (`get_internal_*`) plus the one relative builder (3.3). Note it mixes views: it is the only file that knows both `InternalPaths` and raw-file layout.
- 4.4 `common/constants.py` — `OUTPUT_FOLDER_PREFIX`, `CLUSTER_BASE_WORKING_DIR_NAME`, `DEFAULT_JOB_SCRIPT_NAME` (layout fragments living apart from `paths.py`).
- 4.5 `common/settings.py:62` — `INSTRUMENT_BACKUP_FOLDER_NAME`.
- 4.6 `envs/alphakraken.*.yaml` plus `docker-compose.yaml:451-540` — the two must agree, nothing checks that.

## 5. Where paths are *used*

### Building / crossing views

- 5.1 `dags/impl/processor_impl.py:114-238` — the crossing point. Builds cluster-view *and* container-view versions of the same file/folder side by side (`raw_file_path`/`internal_raw_file_path`, `output_path`/`internal_output_path`) plus the relative one, and stuffs all six into `QuantingEnv` as `str`.
- 5.2 `common/quanting_env.py` — 6 path fields, all untyped `str`, view encoded only in the name prefix `internal_`. Serialized to env vars for `submit_job.sh`.
- 5.3 `jobs/docker_job_handler.py:186-197` — `_to_host_path()`: container -> host translation, `relative_to(InternalPaths.MOUNTS_PATH)`.
- 5.4 `jobs/job_handler.py:23,35` — picks cluster-view slurm root or host mounts root per engine.
- 5.5 `jobs/slurm_ssh_job_handler.py:69-101` — derives cluster working dir plus script path from `locations.slurm.absolute_path`.
- 5.6 `dags/impl/handler_impl.py:325-330` — `get_backup_base_path()`: cluster view, persisted to DB.

### Container view only (all real filesystem I/O)

- 5.7 `raw_file_wrapper_factory.py` — `PathProvider` subclasses (source/target folder plus file name for copy/move/remove), `_instrument_path`.
- 5.8 `file_handling.py:33`, `file_checks.py:82`, `sensors/file_sensor.py:39-45`, `sensors/acquisition_monitor.py:114`, `dags/impl/remover_impl.py:173`, `plugins/_db_migrations/_add_file_info.py`.
- 5.9 `metrics/metrics_calculator.py` plus `metrics/*` — read output dir, always container view.

### Cluster view only (never touched by Python, only emitted)

- 5.10 `plugins/cluster_scripts/submit_job.sh` — consumes `RAW_FILE_PATH`, `SETTINGS_PATH`, `OUTPUT_PATH`, derives `CONFIG_FILE_PATH`, `FASTA_FILE_PATH`, `SPECLIB_FILE_PATH`.
- 5.11 `shared/config_params.py` — 4 of the 8 user-facing `{PLACEHOLDER}`s are paths, half cluster-view half relative; docstrings hardcode yaml key names.

### Persisted (view frozen into the DB)

- 5.12 `RawFile.backup_base_path` (cluster), `RawFile.file_info` keys (relative, two formats), `RawFile.s3_upload_path` (S3 URI), `Metrics.output_path` (cluster).
- 5.13 `webapp/service/components.py:611+` `get_full_backup_path()` — re-concatenates 5.12 for display; `webapp/pages_/settings.py:99,284,295,311,381,463` and `projects.py:88` have 6 TODOs all saying "reimplement using actual {settings_path}/{output_path}" — the webapp cannot resolve paths at all today.

## 6. Problems

- 6.1 **No type distinguishes views.** Everything is `Path` or `str`; a container path in a cluster-view field fails only at runtime, on the cluster. `_check_content` (`processor_impl.py:280-300`) whitelists path fields *by string name* precisely because of this.
- 6.2 **Two parallel accessor families** (`get_internal_*` vs `get_path(YamlKeys.Locations.*)`) with no shared abstraction, so the ragged matrix of §2 is invisible and unenforced.
- 6.3 **Layout logic duplicated per view.** `instrument_id/YYYY_MM/raw_file_id` is built in `processor_impl.py:123`, `handler_impl.py:326`, and `CopyPathProvider.get_target_folder_path()` — three places, no shared function.
- 6.4 **Four relative anchors**, one of them a sentinel `Path()` (3.4), plus the old/new `file_info` ambiguity (3.2) permanently patched.
- 6.5 **`QuantingEnv` carries both views** of the same two objects, forcing every consumer to know which to pick.
- 6.6 **Container/host/cluster consistency is unverified** — `docker-compose.yaml` mount targets, `InternalPaths`, and `mount_target` in yaml must agree by convention only (the yaml comments literally say "DO NOT CHANGE").
