# Airflow 2.11.0 → 3.3.1: manual steps

Code changes are on branch `airflow3_migration` (based on `design_docs/AIRFLOW3_B_MIGRATION_GUIDE.md`).
Verified without a running stack: 553 unit tests, `ty`, `ruff --select AIR`, plugin loading with the
DAGs folder off `sys.path`, `airflow config lint`, dependency resolution against `constraints-3.13.txt`.
Everything below needs a human and/or a running deployment.

## 1. Before cutover

- 1.1 On every machine, add to `envs/${ENV}.env` (templates in `envs/production.env`, `envs/sandbox.env`):
  `AIRFLOW_APISERVER_HOST`, `AIRFLOW_APISERVER_PORT`, `AIRFLOW_BASE_URL`, `AIRFLOW_JWT_SECRET`.
  - `AIRFLOW_APISERVER_HOST/PORT`: reachable from every **worker** host (= infra host IP + `WEBSERVER_PORT`),
    never a compose service name unless all services share one compose network.
  - `AIRFLOW_BASE_URL`: the URL the **browser** uses (behind nginx: `https://<host>:8080`). A wrong value shows
    up only as *Server Not Found* after login.
  - `AIRFLOW_JWT_SECRET`: one strong random string, identical on all machines.
- 1.2 Open the firewall for worker hosts → api-server port (new path; 2.11 workers only needed postgres/redis).
- 1.3 Note all SSH connection ids (Admin → Connections) for step 3.2.
- 1.4 Staging: build the image (`./compose.sh build`) and run the whole of section 3 there first.
  paramiko 5 against the cluster SSH daemon and the amazon provider bump are untested.
- 1.5 Pick an acquisition gap; keep the 2.11 image pullable for rollback.

## 2. Cutover (guide §5)

- 2.1 `./compose.sh down` on all machines.
- 2.2 Back up postgres (the only rollback path):
  `pg_dump -Fc -U "$POSTGRES_USER" -h "$POSTGRES_HOST" "$POSTGRES_DB" > airflow_pre_af3_$(date +%F).dump`
- 2.3 Still on the **old** image: `airflow db clean --clean-before-timestamp <~90 days ago>` and
  `airflow dags reserialize` (expect 0 import errors).
- 2.4 `git pull`, then `./compose.sh --profile dbs up airflow-init` (runs `airflow db migrate`; FAB tables are
  created automatically, no `fab-db migrate`).
- 2.5 Purge the 2.11 Flask sessions (pickle vs msgpack, otherwise HTTP 500 on `/auth/login/`):
  `./compose.sh exec postgres-service psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'DELETE FROM session;'`
- 2.6 `./compose.sh --profile infrastructure up --build -d` (api-server, scheduler, dag-processor), then
  `--profile workers`. Read the **whole** first-boot log of the api-server, not the tail.
- 2.7 Clear the browser cookies for the Airflow host, log in.

## 3. After cutover

- 3.1 UI reachable in a real browser after login (not curl).
- 3.2 Set Airflow Variable `cluster_ssh_connection_ids` = comma-separated ids from 1.3.
  Unlisted connections are silently unused.
- 3.3 Pools and other Variables survive the migration; verify they are present.
- 3.4 Smoke test DAGs in this order: `file_remover` (cron + worker→api path), `instrument_watcher`,
  `acquisition_handler` (checks `trigger_dag_run`; verify the file_mover run starts only after the delay),
  `acquisition_processor` (checks `get_task_states`, SSH Variable, paramiko 5, mapping), `s3_uploader`.
- 3.5 Force one branch of `acquisition_processor` to fail and check the resulting `RawFileStatus`
  (DONE / QUANTING_FAILED / ERROR). Silent if wrong.
- 3.6 Confirm `on_failure_callback` fires and finds `raw_file_id` (callbacks run in the supervisor now).
- 3.7 pandas 3: compare one real `msqc` and one `skyline` metrics output with pre-migration values
  (no test coverage for these two).
- 3.8 Rebuild and smoke-test webapp, rest_api, mcp_server images: `shared/requirements_shared.txt`
  now pins `pydantic==2.13.4` (was 2.11.7, forced by the airflow constraints).

## 4. Rollback

Schema migration is one-way: stop everything → restore the dump from 2.2 → redeploy the 2.11 image
(`git checkout` the pre-migration commit). DAG runs after cutover are lost from Airflow; MongoDB is untouched,
files are re-processed. Confirm this against `watcher_impl.get_unknown_raw_files` before relying on it.

## 5. Developer environments

- 5.1 Recreate local envs: python 3.13 for airflow, a second 3.11 env for the webapp (`docs/development.md`).
  `requirements_airflow.txt` no longer installs on python 3.11.
- 5.2 The pre-commit `ty` hook fails on a 2.11 env (`generate_run_id` signature); it passes against 3.3.1.
  Until upgraded: `SKIP=ty git commit`.
- 5.3 `misc/clear_mapped_branch.py` uses the ORM: run it only via the `airflow-cli` container.

## 6. Known gaps, not addressed

- 6.1 `pymongo==4.7.2`/`pytz==2025.2` in `shared/requirements_shared.txt` differ from the airflow constraints
  (pre-existing; pip builds the image anyway, `uv` refuses).
- 6.2 `AIRFLOW__API__SECRET_KEY` is still the hardcoded `some-random-string-here` (pre-existing).
- 6.3 `ty` in CI runs without airflow installed, so SDK type errors are not caught there.
- 6.4 README python badge says 3.13; webapp/monitoring/rest_api/mcp_server images stay on 3.11.
- 6.5 `airflow-triggerer` stays commented out (only needed for doc C features).
- 6.6 `SUPERVISOR_COMMS` (used by `trigger_dag_run`) is internal API; re-check on the next Airflow bump.
