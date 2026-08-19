# Self-contained AlphaKraken demo (`misc/demo`)

## Context

AlphaKraken normally needs an acquisition PC, a Slurm cluster and a shared file system. The new
`docker` job engine (`airflow_src/plugins/jobs/docker_job_handler.py`) plus the containerized
`msqc-extractor` remove the cluster dependency, which makes a **single-host, end-to-end demo**
possible for the first time: real Thermo `.raw` files, real acquisition monitoring, real backup,
real metrics extraction, real webapp/MCP/REST output.

The demo must run unattended: a feeder script drops a raw file into the instrument folder every
21 minutes (mimicking a 21-minute gradient), and all four front-ends (Streamlit webapp, Airflow UI,
MCP server, REST API) are reachable behind one nginx under a single UUID path prefix
(security by obscurity — no basic auth).

Key mechanic that makes the timing work: `AcquisitionMonitor.poke()`
(`airflow_src/plugins/sensors/acquisition_monitor.py:183`) considers an acquisition finished when
**exactly one new file appears** in the instrument folder. So file N completes when file N+1 lands
— the feeder interval *is* the pipeline clock. (The fallback is 2×60 min of unchanged file size,
which is why cycling forever matters: there is always a next file.)

## Decisions

| Topic | Decision |
|---|---|
| URL layout | One UUID, path-routed: `/<uuid>/` webapp, `/<uuid>/airflow/`, `/<uuid>/mcp`, `/<uuid>/api/` |
| Environment | Own `demo` env (`ENV=demo`), separate Mongo/Postgres data dirs and mounts |
| Automation | Fully automated: Airflow pools + variables + DAG unpausing + Mongo project/settings seeding |
| Feeder | One Thermo instrument (`demo1`), cycles over the source files forever |
| TLS | Reuse the existing `./certs` (`fullchain.pem` + `privkey.pem`) |
| Retention | Rely on the pipeline's own `file_mover` → `file_remover` DAGs (via `min_free_space_gb` / `min_file_age_days`) |
| Project matching | Real project token: `ADIAMA`, which the datashare demo file names already carry as a `_`-separated token |
| Raw files | Downloaded from MPIB datashare with `alphabase.tools.data_downloader.DataShareDownloader` |

## New files: `misc/demo/`

```
misc/demo/
  README.md                          # what the demo is, how to start/stop/reset, URLs, caveats
  setup_demo.sh                      # one-shot orchestration (idempotent)
  download_raw_files.py              # alphabase DataShareDownloader over the 3 datashare URLs
  feed_instrument.sh                 # sleep -> copy next raw file with timestamp -> repeat, cycling
  seed_db.py                         # creates project ADIAMA + msqc settings + assignment
  templates/
    demo.env.template
    alphakraken.demo.yaml.template
    nginx.demo.conf.template
  raw_files/                         # download target (gitignored)
  mounts/                            # MOUNTS_PATH for ENV=demo (gitignored)
  .state/                            # generated uuid + rendered nginx.conf (gitignored)
```

### `download_raw_files.py`

Module-level `RAW_FILE_URLS` constant with the three datashare URLs
(`https://datashare.biochem.mpg.de/public.php/dav/files/WTu3rFZHNeb3uG2/20231024_OA3_TiHe_ADIAMA_HeLa_200ng_Evo01_21min_F-40_iO_before_0{1,2,3}.raw`)
and `OUTPUT_DIR = misc/demo/raw_files`. Downloads each with
`DataShareDownloader(url=..., output_dir=...).download()` — the same helper the alphadia e2e tests
use. Skips files that already exist. Requires `pip install alphabase progressbar2` (documented in
the README; not added to any requirements file since this runs on the host, not in a container).

The file names already contain `ADIAMA` as an underscore-separated token, so
`get_unique_project_id()` (`airflow_src/dags/impl/project_id_handler.py:58`) matches them to the
seeded project with **no renaming** — only the timestamp is appended.

### `feed_instrument.sh`

Constants at the top (`SOURCE_DIR`, `TARGET_DIR`, `INTERVAL_S=1260`), each overridable by an
env var of the same name. Loop:

1. `sleep "$INTERVAL_S"`
2. pick source file `i` from the sorted list, `i = (i + 1) % n` (cycles forever)
3. `cp "$src" "$TARGET_DIR/${stem}_$(date +%Y%m%d-%H%M%S).raw"`

Copied **directly** into the instrument folder, not via a temp file + `mv`: a partially written
file is exactly what a real acquisition looks like, and the acquisition monitor is built for it.
Timestamp format `%Y%m%d-%H%M%S` deliberately avoids `:` — `shared/validation.py` only allows
`a-zA-Z0-9-_+.` in raw file names.

### `seed_db.py`

Uses the existing DB interface (`shared/db/interface.py`) — no new DB code:

- `add_project(project_id="ADIAMA", name=..., description=...)`
- `create_settings(name="demo_msqc", software_type=SoftwareTypes.CUSTOM, software="alphakraken-msqc",
  job_engine=JobEngines.DOCKER, metrics_type=MetricsTypes.MSQC,
  config_params="RAW_FILE_PATH OUTPUT_PATH NUM_THREADS", slurm_cpus_per_task=2, slurm_mem="8G",
  slurm_time="01:00:00", num_threads=2)`
- `assign_settings_to_project(project_id="ADIAMA", settings_id=...)`

`software_type=custom` is required — the webapp restricts the `docker` engine to it
(`webapp/pages_/settings.py:498`), and `software` holds the plain image name (no colon, since
`check_for_malicious_content` forbids it). `slurm_mem`/`slurm_cpus_per_task` are what
`DockerJobHandler` turns into the container's `mem_limit` / `nano_cpus`.

Run without any image change or volume mount, by piping into the running webapp container:
`./compose.sh --profile demo exec -T webapp python - < misc/demo/seed_db.py`. The webapp's Mongo
user has exactly the needed `insert` rights on `project`, `settings` and `project_settings`
(`misc/init-mongo.sh`), and its `PYTHONPATH` already resolves `shared.db.interface`.
Idempotent: skip anything that already exists.

### Templates rendered by `setup_demo.sh`

`demo.env.template` → `envs/demo.env` (gitignored). Like `envs/local.env`, plus:

- `MOUNTS_PATH=./misc/demo/mounts`
- `DOCKER_GID=__DOCKER_GID__` (from `stat -c '%g' /var/run/docker.sock`)
- `STREAMLIT_SERVER_BASE_URL_PATH=__UUID__`, `WEBAPP_HEALTHCHECK_URL=http://localhost:8501/__UUID__/healthz`
- `AIRFLOW_BASE_URL=https://__DEMO_HOST__/__UUID__/airflow`, `AIRFLOW_HEALTHCHECK_URL=http://localhost:8080/__UUID__/airflow/health`
- `REST_API_ROOT_PATH=/__UUID__/api`
- app ports bound to loopback so the UUID is the only way in:
  `WEBAPP_PORT=127.0.0.1:8501`, `WEBSERVER_PORT=127.0.0.1:8080`, `MCP_PORT=127.0.0.1:8089`,
  `REST_API_PORT=127.0.0.1:8090` (these vars are only used for host publishing; the containers'
  internal ports are hardcoded in `docker-compose.yaml`)

`alphakraken.demo.yaml.template` → `envs/alphakraken.demo.yaml` (gitignored; picked up
automatically by the `envs/alphakraken.*.yaml` glob in `webapp/Dockerfile`):

- one instrument `demo1`, `type: thermo`, `min_free_space_gb`, `min_file_age_days: 0` so
  `file_remover` purges instrument-side copies promptly
- `locations.general.mounts_path: __ABS_MOUNTS_PATH__` — must be the **host** path, that is what
  `DockerJobHandler._to_host_path()` uses to bind raw file + output into the msqc container
- `locations.*.absolute_path` pointing into the demo mounts; `backup.backup_type: local`
- `general.notifications.webapp_url: https://__DEMO_HOST__/__UUID__/`

`nginx.demo.conf.template` → `misc/demo/.state/nginx.conf`. Upstreams are compose **service
names** (`webapp:8501`, `airflow-webserver:8080`, `mcp-server:8089`, `rest-api:8090`) — no
`255.255.0.x` VPN addresses, no `ip_hash`, no `auth_basic`. Longest-prefix matching orders the
locations:

| location | proxy_pass | prefix |
|---|---|---|
| `/__UUID__/airflow/` | `http://airflow-webserver:8080` | kept (Airflow `base_url` carries it) |
| `/__UUID__/mcp` | `http://mcp-server:8089/mcp` | rewritten to FastMCP's default `/mcp` |
| `/__UUID__/api/` | `http://rest-api:8090/` | stripped, `X-Forwarded-Prefix` set |
| `/__UUID__/` | `http://webapp:8501` | kept (Streamlit `baseUrlPath`) |
| `/` | — | `return 404` |

Websocket upgrade headers on the webapp, Airflow and MCP locations; `listen 80` redirects to
https; certs from `/etc/nginx/certs` (`./certs` mounted read-only, same as the production nginx).

## Changes to existing files

**`docker-compose.yaml`** — additive, all defaults preserve current behaviour:

1. Add `"demo"` to the `profiles` of the services the demo needs: `postgres-service`,
   `redis-service`, `mongodb-service`, `airflow-webserver`, `airflow-scheduler`, `webapp`,
   `mcp-server`, `rest-api`, `monitoring`, `airflow-worker-file-mover`,
   `airflow-worker-file-remover`. Then `./compose.sh --profile demo up` starts exactly the demo
   set — no `test1/2/3` workers with dangling bind mounts.
2. `airflow-webserver`: `AIRFLOW__WEBSERVER__BASE_URL: ${AIRFLOW_BASE_URL:-http://localhost:8080}`
   and healthcheck URL `${AIRFLOW_HEALTHCHECK_URL:-http://localhost:8080/health}` (Airflow mounts
   the whole app under the `base_url` path, so `/health` moves too).
3. `webapp`: `STREAMLIT_SERVER_BASE_URL_PATH: ${STREAMLIT_SERVER_BASE_URL_PATH:-}` (Streamlit reads
   every config option from `STREAMLIT_<SECTION>_<OPTION>`, so no `command:` override is needed)
   and healthcheck URL `${WEBAPP_HEALTHCHECK_URL:-http://localhost:8501/healthz}`.
4. `rest-api`: `REST_API_ROOT_PATH: ${REST_API_ROOT_PATH:-}`.
5. New `airflow-worker-demo1`, `profiles: ["demo"]`, reusing the existing `*airflow-worker` anchor —
   copy of `airflow-worker-test1` with `-q kraken_queue_demo1` and `instruments/demo1` /
   `backup/demo1` mounts, keeping the `/var/run/docker.sock` mount and `group_add: ["${DOCKER_GID:-0}"]`
   that the `docker` engine needs.
6. New `nginx-demo`, `profiles: ["demo"]`: `nginx:latest`, ports `80` + `443` only, mounting
   `./misc/demo/.state/nginx.conf` and `./certs`. A separate service rather than reusing `nginx`,
   whose extra `8501/8080/8089/8090` publishing would clash with the app services.

Rationale for editing the base compose file instead of shipping a `docker-compose.demo.yaml`
override: override files cannot reference YAML anchors defined in the base file, so
`airflow-worker-demo1` would have to duplicate ~35 lines of Airflow environment. All changes above
are additive and default to today's behaviour.

**`rest_api/main.py`** — one line so `/docs` and `openapi.json` work behind the prefix:
`root_path=os.getenv("REST_API_ROOT_PATH", "")` in the `FastAPI(...)` call. Default `""` is
today's behaviour.

**`.gitignore`** — add `misc/demo/raw_files/`, `misc/demo/mounts/`, `misc/demo/.state/`,
`envs/demo.env`, `envs/alphakraken.demo.yaml`.

## `setup_demo.sh` (idempotent)

1. Preflight: docker reachable, `./certs/fullchain.pem` + `privkey.pem` present, `envs/.env-airflow`
   exists, `DEMO_HOST` (env var, default `hostname -f`).
2. UUID: generate once into `misc/demo/.state/uuid`, reuse on re-runs so the URL is stable.
3. Render the three templates (`sed` on `__UUID__`, `__DEMO_HOST__`, `__ABS_MOUNTS_PATH__`, `__DOCKER_GID__`).
4. `mkdir -p` the mounts tree: `instruments/demo1/Backup`, `backup/demo1`, `output`,
   `settings/{config,fasta,speclib}`, `airflow_logs`.
5. `docker build -t alphakraken-msqc msqc-extractor` (name matches the seeded `software` field).
6. `ENV=demo ./compose.sh --profile dbs up airflow-init` (DB migrate + Airflow user).
7. `ENV=demo ./compose.sh --profile demo up --build -d`, then wait for the webserver and Mongo
   healthchecks.
8. Airflow config via `--profile debug run --rm airflow-cli`: `airflow pools set cluster_slots_pool 4`,
   `file_copy_pool 3`, `airflow variables set debug_no_cluster_ssh True`, then
   `airflow dags unpause` for `instrument_watcher.demo1`, `acquisition_handler.demo1`,
   `acquisition_processor.demo1`, `file_mover.demo1`, `file_remover`
   (ids are `<Dags.*>` + `DAG_DELIMITER` + instrument, `airflow_src/plugins/common/keys.py:3`).
9. Seed Mongo (piped `seed_db.py`, see above).
10. Print the four URLs and the command to start the feeder.

The feeder is **not** started by the setup script — it runs in the foreground (or under `nohup`)
so the demo operator controls it; the README shows both.

## Verification

1. `python misc/demo/download_raw_files.py` → three `.raw` files in `misc/demo/raw_files/`.
2. `misc/demo/setup_demo.sh` → all demo containers healthy
   (`ENV=demo ./compose.sh --profile demo ps`).
3. Reachability, all four behind the UUID and 404 without it:
   - `curl -sf https://$DEMO_HOST/$UUID/healthz` → Streamlit health
   - `curl -sf https://$DEMO_HOST/$UUID/airflow/health` → Airflow health JSON
   - `curl -sf https://$DEMO_HOST/$UUID/api/health` and `/api/docs` (docs must render — proves `root_path`)
   - `curl -sf -X POST https://$DEMO_HOST/$UUID/mcp -H 'Accept: text/event-stream' …` → MCP responds
   - `curl -s -o /dev/null -w '%{http_code}' https://$DEMO_HOST/` → `404`
4. First end-to-end run, with `INTERVAL_S=120` to avoid waiting 21 minutes:
   `INTERVAL_S=120 misc/demo/feed_instrument.sh`. Expect, per file:
   `instrument_watcher.demo1` picks it up → `acquisition_handler.demo1` finishes monitoring when
   the *next* file lands → checksum + copy to `misc/demo/mounts/backup/demo1/` →
   `acquisition_processor.demo1` starts a `kraken-custom-<raw_file_id>` container →
   `msqc_results.tsv` + `msqc_tic.tsv` + `log.txt` under
   `misc/demo/mounts/output/ADIAMA/out_<raw_file_id>/custom/`.
5. Data visible in all three read paths: raw file with status `done` and msqc metrics in the
   webapp, in `GET /$UUID/api/raw_files/?project_id=ADIAMA`, and via the MCP tools.
6. Confirm the project token matched: `project_id` is `ADIAMA`, not `_FALLBACK`.
7. Then restart the feeder at the real cadence (`INTERVAL_S` default 1260) and let it cycle.
8. Regression check on the unchanged path: `ENV=local ./compose.sh --profile local config` still
   renders (defaults intact), and `python -m pytest` passes.

## Risks / operator notes (for the README)

- **Demo and local cannot run at the same time.** `MONGO_PORT` is used both as the host port and
  as the in-container connect port, and postgres/redis publish fixed `5432`/`6379`. Stop `ENV=local`
  before starting the demo.
- **Disk growth.** `min_file_age_days: 0` + `min_free_space_gb` let `file_remover` purge
  instrument-side copies, but the **backup pool and the msqc output folders keep growing** — about
  68 runs/day at a 21-minute cadence. Watch `misc/demo/mounts/{backup,output}` and reset the demo
  when needed. Cheapest reset: stop the feeder, `down -v`, delete `misc/demo/mounts`,
  `mongodb_data_demo`, `airflowdb_data_demo`, re-run `setup_demo.sh`.
- **The UUID is in `envs/demo.env`, `envs/alphakraken.demo.yaml` and `misc/demo/.state/`** — all
  gitignored, but it is not a secret in any strong sense: it appears in nginx access logs and in
  browser history. Obscurity only, as requested.
- **`AIRFLOW_BASE_URL` needs the real demo host up front** (Airflow generates absolute URLs). If
  the host name changes, re-run `setup_demo.sh` with the new `DEMO_HOST`.
- **SCIEX is out of scope**: the msqc image uses the `coreclr` .NET runtime and cannot read `.wiff`.
- **Only 3 source files exist**, and cycling re-feeds the same bytes, so every metric repeats with a
  period of 3 — the webapp's QC trend plots will look like a flat sawtooth rather than realistic
  instrument drift. Fine for showing that the pipeline works; if the demo is meant to showcase the
  QC plots themselves, more (or more varied) source files are needed.
- **Settings show up as software type `custom`**, not `msqc`, because the `docker` engine is
  restricted to `custom` (`webapp/pages_/settings.py:498`). The *metrics* type is `msqc`, so the
  metrics themselves are labelled correctly.