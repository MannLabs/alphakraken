# Upgrade from v0.13.0

## REST API: new MongoDB user (required)

Since version 0.2.0 the REST API no longer connects with the read-only user but with its own, `MONGO_USER_REST_API`,
for reads too. This is not optional:

- Without the new env variables `MONGO_USER_REST_API` and `MONGO_PASSWORD_REST_API` the `mongodb-service` and
  `rest-api` containers do not start.
- With the variables, but without the user in the database, `/health` stays green while every other request fails,
  as the service cannot authenticate at the database.

`misc/init-mongo.sh` creates the user and its role only when the database is first created, so on an existing database
create them by hand. This needs no downtime; test on `sandbox` first. Only `REST_API_KEY` is optional: without it the
REST API is read-only.

## Steps

1. On every PC (VM) of the deployment, add `MONGO_USER_REST_API` and `MONGO_PASSWORD_REST_API` to `envs/<ENV>.env`.
   Optional, only to enable the writes and `GET /users/`: add `REST_API_KEY`, with the same value on every PC (VM)
   that runs the `rest-api` service (cf. [API key](../deployment.md#api-key)).
2. On the PC (VM) hosting the dbs, `cd` to the alphakraken source directory, `export ENV=production` and create the role:
   open a `mongosh` session on the database as admin
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin krakendb'
   ```
   and paste the `db.createRole({role: "restApiRole", ...});` statement of `misc/init-mongo.sh`, the only place where the
   role is defined.
3. Create the user (replace the two placeholders):
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").createUser({user: \"<MONGO_USER_REST_API>\", pwd: \"<MONGO_PASSWORD_REST_API>\", roles: [\"restApiRole\"]})"'
   ```
4. On the PC (VM) hosting the infrastructure, restart the REST API with the new version: `./compose.sh up rest-api --build -d`.
5. Confirm that the REST API reaches the database: `curl "http://<alphakraken_ip_address>:8090/projects/?limit=1"`
   returns a project (`/health` does not show this, it does not access the database).
6. If `REST_API_KEY` is set, confirm that `curl "http://<alphakraken_ip_address>:8090/users/" -H "X-API-Key: <api_key>"`
   returns the users.

## Changes visible to REST API clients (version 0.2.0)

- New endpoints to manage projects, settings and their assignments, and to read the status and the users, cf.
  [REST API](../deployment.md#rest-api). The writes and `GET /users/` need the header `X-API-Key`.
- The assignments in `/projects/` have two new fields: `id`, which the assignment endpoints take, and `project_id`.
- Projects and settings have the new field `owner_email`.
- Raw files: `collision_flag`, `status_details`, `backup_status`, `backup_display_base_path`, `s3_upload_path`,
  `instrument_file_status` and `finalized_at` are always present, `null` when unset (before: present only when
  set, and missing from the OpenAPI schema).
- Errors: every `422` is `{"detail": ["<location>: <message>", ...]}`, e.g. `["query.limit: Input should be greater
  than or equal to 1"]` (before: FastAPI's list of objects). The `detail` text of a `404` changed, e.g. `No project P1`.
- OpenAPI: the list schemas are named `Page_RawFileResponse_`, `Page_ProjectResponse_` and `Page_SettingsResponse_`;
  their shape is unchanged.
