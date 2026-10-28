# Upgrade from v0.12.0

## Backup location per raw file

`RawFile.backup_display_base_path` records `display_paths.backup` at copy time, and is used to display the backup path.
Hence, change `display_paths.backup` only when the backup file system changes.

After deploying, backfill existing raw files with the current `display_paths.backup`:
```
./compose.sh exec -w /opt/airflow -e PYTHONPATH=/opt/airflow airflow-worker-file-mover python shared/_migrations/from_0.12.0/_backfill_backup_display_base_path.py --dry-run
```

## Output location per metrics

Likewise, `Metrics.output_display_base_path` records `display_paths.output` when the metrics are written.
Change `display_paths.output` only when the output file system changes.

Backfill existing metrics with the current `display_paths.output`:
```
./compose.sh exec -w /opt/airflow -e PYTHONPATH=/opt/airflow airflow-worker-file-mover python shared/_migrations/from_0.12.0/_backfill_output_display_base_path.py --dry-run
```

## Adding users (owner of projects and settings)

The webapp now writes to the new `user` collection. On databases created before this change,
the webapp's MongoDB role (`webappRole`) lacks write access to it.
The grant is idempotent and does not need downtime. Test on `sandbox` first.

## Steps
On the PC (VM) hosting the dbs, `cd` to the alphakraken source directory and `export ENV=production`.

1. Grant the webapp role access to the `user` collection:
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").grantPrivilegesToRole(\"webappRole\", [{resource: {db: \"krakendb\", collection: \"user\"}, actions: [\"find\", \"insert\", \"update\", \"remove\"]}])"'
   ```
2. Verify that `user` is listed:
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").getRole(\"webappRole\", {showPrivileges: true}).privileges.map(p => p.resource.collection)"'
   ```
   Expected: `[ 'project', 'settings', 'project_settings', 'user' ]`.
3. Deploy the new webapp version.
4. As admin, add a user on the admin page to confirm that writing works.

New databases get the grant from `misc/init-mongo.sh` and need none of these steps.


# Alert refactoring: issue records, messenger ids, suppressions, active instance

The monitoring records the issues it finds in MongoDB, sends direct messages to the messenger id
stored with each user, reads alert suppressions from the database, and runs the DB-backed checks
on one elected instance only. All of this needs the steps below on existing deployments; new
databases get roles, users, grants and indexes from `misc/init-mongo.sh`.

## Steps

Before deploying, note down on each deployment:

1. the entries of `INSTRUMENT_USER_SLACK_IDS` in `monitoring/alerts/config.py`, if edited,
2. the rules in `monitoring/suppressions.yaml`, if any,
3. the instrument ids in `INSTRUMENT_STALL_INSTRUMENT_IDS` in `monitoring/alerts/config.py`, if edited.

Then, on the PC (VM) hosting the dbs, `cd` to the alphakraken source directory and
`export ENV=production`.

4. Add `MONGO_USER_ALERTING` and `MONGO_PASSWORD_ALERTING` to `envs/${ENV}.env` on all machines
   running the monitoring (`docker-compose.yaml` requires them). `MONGO_USER_READ` is no longer
   used by the monitoring.
5. Create the alerting role and user, grant the collections, and create the index:
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").createRole({role: \"alertingRole\", privileges: [{resource: {db: \"krakendb\", collection: \"alert_issue\"}, actions: [\"find\", \"insert\", \"update\"]}, {resource: {db: \"krakendb\", collection: \"alerting_active_instance\"}, actions: [\"find\", \"insert\", \"update\"]}], roles: [\"read\"]})"'
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").createUser({user: \"<MONGO_USER_ALERTING>\", pwd: \"<MONGO_PASSWORD_ALERTING>\", roles: [\"alertingRole\"]})"'
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").grantPrivilegesToRole(\"webappRole\", [{resource: {db: \"krakendb\", collection: \"alert_suppression\"}, actions: [\"find\", \"insert\", \"update\", \"remove\"]}])"'
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").alert_issue.createIndex({first_seen: 1}, {expireAfterSeconds: 7776000})"'
   ```
6. Fill in noted slack ids, supressions and instrument ids for stall alert


# Notifications via apprise

The monitoring sends all messages through [apprise](https://github.com/caronc/apprise), so any of its
supported services can be configured by URL. In `envs/alphakraken.${ENV}.yaml`, under `general.notifications`:

1. Rename `ops_alerts_webhook_url` to `ops_alerts_url` and `business_alerts_webhook_url` to
   `business_alerts_url`. For Slack, use `slacktext://<bot token>/<channel id>` and invite the bot to the
   channel (full-width messages); a Slack webhook URL works too but posts narrower attachments.
   MS Teams Power Automate URLs work unchanged.
2. Replace `slack_bot_token: "xoxb-..."` with `user_alerts_url: "slacktext://xoxb-.../@{{ALPHAKRAKEN_MESSENGER_ID}}"`.
