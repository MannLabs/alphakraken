# Alert issue records: MongoDB user for the monitoring service

The monitoring now records every issue it finds in the new `alert_issue` collection, together
with when it was last notified. It therefore connects as a new user `MONGO_USER_ALERTING`
that may write to that collection; `MONGO_USER_READ` is no longer used by the monitoring.
`init-mongo.sh` only runs on a fresh DB, so existing deployments need the steps below.

Without the user, compose refuses to start the monitoring. With the user but without the role,
every check fails with an authorization error and is reported as `exception` to the ops channel.

## Steps
On the PC (VM) hosting the dbs, `cd` to the alphakraken source directory and `export ENV=production`.

1. Add `MONGO_USER_ALERTING` and `MONGO_PASSWORD_ALERTING` to `envs/${ENV}.env` on all machines
   running the monitoring (`docker-compose.yaml` requires them).
2. Create the role, the user and the index:
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").createRole({role: \"alertingRole\", privileges: [{resource: {db: \"krakendb\", collection: \"alert_issue\"}, actions: [\"find\", \"insert\", \"update\"]}], roles: [\"read\"]})"'
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").createUser({user: \"<MONGO_USER_ALERTING>\", pwd: \"<MONGO_PASSWORD_ALERTING>\", roles: [\"alertingRole\"]})"'
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").alert_issue.createIndex({first_seen: 1}, {expireAfterSeconds: 7776000})"'
   ```
   The index expires the records 90 days after the issue was first seen.
3. Deploy the new monitoring version on all machines.

Note: there is no longer a quiet first check after a restart. Every issue that is open when the
new version starts is notified once, then recorded.

New databases get role, user and index from `misc/init-mongo.sh` and need none of these steps.
