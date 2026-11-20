# Upgrade from v0.13.0

## Before deploying
Pause the `instrument_watcher`s and wait until no `acquisition_handler` or `acquisition_processor` run is active;
unpause after the deploy. Runs that started before the deploy fail afterwards (raw file status `ERROR`).

## REST API: new MongoDB user (required)
The REST API (0.2.0) connects with its own user, for reads too. `misc/init-mongo.sh` creates it only on a new database.
Without the env variables no `./compose.sh` command works; without the user, `/health` is green but every other request fails.

1. On every PC (VM): add `MONGO_USER_REST_API` and `MONGO_PASSWORD_REST_API` to `envs/<ENV>.env`.
   Optional, enables the writes and `GET /users/`: `REST_API_KEY`, identical on every PC running `rest-api`
   (cf. [API key](../deployment.md#api-key)).
2. On the PC hosting the dbs (`export ENV=production`), open a `mongosh` session
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin krakendb'
   ```
   and run
   - the `db.createRole({role: "restApiRole", ...});` statement of `misc/init-mongo.sh`,
   - `db.createUser({user: "<MONGO_USER_REST_API>", pwd: "<MONGO_PASSWORD_REST_API>", roles: ["restApiRole"]})`.
3. On the PC hosting the infrastructure: `./compose.sh up rest-api --build -d`.
4. Verify: `curl "http://<alphakraken_ip_address>:8090/projects/?limit=1"` returns a project.

## Metric key names
After deploying, rename the keys of the existing metrics. Until then, `gradient_length` and
`evosep_pump_hp_pressure_max` are missing for them (pump pressure alert, MCP server).
```
./compose.sh exec -w /opt/airflow -e PYTHONPATH=/opt/airflow airflow-worker-file-mover python shared/_migrations/from_0.12.0/_migrate_legacy_metrics_keys.py --dry-run
```

## Changes for clients
REST API and MCP server:
- Datetimes carry their UTC offset (REST API: `2024-01-02T03:04:05Z`, MCP server: `2024-01-02T03:04:00+00:00`);
  the values are unchanged.

MCP server:
- `metrics` of a raw file is a list with one entry per metrics type (key `type`), no longer one flat dict.
  `settings_name`, `settings_version` and `relative_output_path` are gone; the output folder is in `output_path`.

REST API (0.2.0):
- New endpoints for projects, settings, assignments, status and users, cf. [REST API](../deployment.md#rest-api).
- New fields: `id` and `project_id` in the assignments of `/projects/`, `owner_email` in projects and settings.
- Raw files: `collision_flag`, `status_details`, `backup_status`, `backup_display_base_path`, `s3_upload_path`,
  `instrument_file_status` and `finalized_at` are always present (`null` when unset).
- `422` is `{"detail": ["<location>: <message>", ...]}`, e.g. `["query.limit: Input should be greater than or equal to 1"]`.
- The `detail` of a `404` changed, e.g. `No project P1`.
- OpenAPI: the list schemas are named `Page_<Model>_`.
