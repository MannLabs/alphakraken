# Airflow 2.11.0 → 3.3.2: manual steps

This is a major upgrade, approach with caution!

## 1. Before cutover

- 1.1 On every machine, add to `envs/${ENV}.env`:
  `AIRFLOW_APISERVER_HOST`, `AIRFLOW_APISERVER_PORT`, `AIRFLOW_BASE_URL`, `AIRFLOW_JWT_SECRET`.
  - `AIRFLOW_APISERVER_HOST/PORT`: reachable from every **worker** host (= infra host IP + `WEBSERVER_PORT`),
    never a compose service name unless all services share one compose network.
  - `AIRFLOW_BASE_URL`: the URL the **browser** uses. Airflow 3 redirects to it after login, scopes the session
    cookie by its path and builds the log links in notifications from it; workers do not use it.
    Behind nginx (`misc/nginx.conf` terminates TLS on 8080 and proxies to the api-servers) it is
    `https://<nginx_host>:8080`: scheme and port of nginx as typed into the address bar, not of the api-server
    container, no trailing path. A wrong value shows up only as *Server Not Found* after login; curl and health
    checks stay green.
  - `AIRFLOW_JWT_SECRET`: one strong random string, identical on all machines.
- 1.2 Open the firewall for worker hosts → api-server port (new path; 2.11 workers only needed postgres/redis).
- 1.3 Rename the SSH connections (Admin → Connections) to `<ssh_connection_id_prefix>_1`, `_2`, ... without gaps,
  e.g. `cluster_ssh_connection` → `cluster_ssh_connection_1`. Workers cannot list connections any more; they probe these ids.
- 1.4 Staging: build the image (`./compose.sh build`) and run the whole of section 3 there first.
- 1.5 Pick an acquisition gap or pause instrument_watchers and wait for all downstream DAGs to clear; keep the 2.11 image pullable for rollback.

## 2. Cutover (guide §5)

- 2.1 `./compose.sh down` on all machines.
- 2.2 Back up postgres (the only rollback path):
  `pg_dump -Fc -U "$POSTGRES_USER" -h "$POSTGRES_HOST" "$POSTGRES_DB" > airflow_pre_af3_$(date +%F).dump`
- 2.3 (optional) Still on the **old** image: `airflow db clean --clean-before-timestamp <~90 days ago>` and
  `airflow dags reserialize` (expect 0 import errors).
- 2.4 `git pull`, then `./compose.sh --profile dbs up airflow-init` (runs `airflow db migrate`; FAB tables are
  created automatically, no `fab-db migrate`). Grep the init log for `Traceback`: the entrypoint runs
  `airflow db migrate || true`, so exit code 0 proves nothing. Known failure: the FAB migration
  `0001_3_5_0_fix_fab_db_inconsistencies` aborts with
  `NotNullViolation: column "view_menu_id" of relation "ab_permission_view"` (orphan rows from 2.11).
  The following `users create` step deletes them (`Deleted 1 faulty permissions`); rerun `airflow-init` in this case.
- 2.5 (optional, if troubles opening the apiserver GUI) Purge the 2.11 Flask sessions (pickle vs msgpack, otherwise HTTP 500 on `/auth/login/`):
  `./compose.sh exec postgres-service psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'DELETE FROM session;'`
- 2.6 `./compose.sh --profile infrastructure up --build -d` (api-server, scheduler, dag-processor), then
  `--profile workers`. Read the **whole** first-boot log of the api-server, not the tail.
- 2.7 Clear the browser cookies for the Airflow host, log in.
- 2.8 Rebuild and smoke-test webapp, rest_api, mcp_server images.

## 3. After cutover
Check the following:
- 3.1 UI reachable in a real browser after login (not curl), through nginx. With two api-server replicas the
  `ip_hash` upstream keeps a browser on one backend; confirm login survives a reload and a second tab.
- 3.2 Connections not following the naming of 1.3 (or after a gap in the numbering) are silently unused, remove them.
- 3.3 Smoke test DAGs in this order: `file_remover` (cron + worker→api path), `instrument_watcher`,
  `acquisition_handler` (checks `trigger_dag_run`; verify the file_mover run starts only after the delay),
  `acquisition_processor` (checks `get_task_states`, SSH connection probing, paramiko 5, mapping), `s3_uploader`.


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



# MongoDB 6.0 → 8.0

MongoDB upgrades one major version at a time: 6.0 → 7.0 → 8.0.
No data migration is needed; users and roles carry over.

Downgrading is not supported once on 7.0: the only rollback is restoring the backup (step 2),
losing all writes made since.

Driver pins (`pymongo==4.7.2`, `mongoengine==0.28.2`) work with 8.0 (without support for new 8.0 features)
and stay unchanged during the upgrade.

Test on `sandbox` first.

## Steps
On the PC (VM) hosting the dbs, `cd` to the alphakraken source directory and `export ENV=production`.

1. Stop all services that access MongoDB (workers, infrastructure, webapp, monitoring), then MongoDB:
   `./compose.sh --profile dbs stop mongodb-service`
2. Create a backup (MongoDB must be stopped, do not use `misc/backup_db.sh`):
   `sudo cp -a mongodb_data_${ENV} mongodb_data_${ENV}.bak_6.0`
3. Start MongoDB and check that the feature compatibility version is `6.0`:
   `./compose.sh --profile dbs up -d mongodb-service`
   `./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.adminCommand({getParameter: 1, featureCompatibilityVersion: 1})"'`
4. Upgrade to 7.0:
   1. In `docker-compose.yaml`, set the `mongodb-service` image to `mongo:7.0-jammy`.
   2. `./compose.sh --profile dbs up -d mongodb-service` and check `./compose.sh --profile dbs logs mongodb-service` for errors.
   3. Set the feature compatibility version:
      `./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.adminCommand({setFeatureCompatibilityVersion: \"7.0\", confirm: true})"'`
   4. Verify with the command from step 3 (expect `7.0`). Otherwise, 8.0 will refuse to start (exit code 62).
5. Upgrade to 8.0: repeat step 4 with image `mongo:8.0-noble` and `"8.0"`.
6. Verify with the command from step 3 (expect `8.0`), then restart all other services.
7. Once operation is confirmed, delete `mongodb_data_${ENV}.bak_6.0`.
