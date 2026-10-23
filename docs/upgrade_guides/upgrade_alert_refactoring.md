# Alert refactoring: issue records, messenger ids, suppressions, active instance

The monitoring records the issues it finds in MongoDB, sends direct messages to the messenger id
stored with each user, reads alert suppressions from the database, and runs the DB-backed checks
on one elected instance only. All of this needs the steps below on existing deployments; new
databases get roles, users, grants and indexes from `misc/init-mongo.sh`.

Behavior changes to know about:

- There is no quiet first check after a restart. Every issue that is open when the new version
  starts is notified once, then recorded in `alert_issue`. The records expire 90 days after the
  issue was first seen.
- Of several monitoring instances, only the active one (recorded in `alerting_active_instance`)
  runs the DB-backed checks and notifies. The others only run the checks that need no DB (local
  disk, webapp health) and report DB connection errors. If the claim cannot be recorded (e.g.
  missing grant), every instance runs the checks, as before.
- Direct messages (e.g. queue stops) go to the messenger id of the user; the hard-coded
  `INSTRUMENT_USER_SLACK_IDS` is removed. The queue-stop alert recognizes the initials of all
  active users with a messenger id in raw file names. User initials must be 2 to 8 letters.
- Alert suppressions are managed on the webapp's "Admin" page and picked up with the next check;
  `monitoring/suppressions.yaml` is no longer read.
- The instruments watched by the stall alert are configured in the yaml settings
  (`general.notifications.instrument_stall_instrument_ids`) instead of `monitoring/alerts/config.py`.

Without `MONGO_USER_ALERTING`, compose refuses to start the monitoring. With the user but without
the role, every check fails with an authorization error and is reported as `exception` to the ops
channel.

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
6. Set `general.notifications.instrument_stall_instrument_ids` in `envs/alphakraken.${ENV}.yaml`
   to the noted instrument ids, if any.
7. Deploy the new version on all machines.
8. On the admin page, make sure a user exists for each of the noted initials and set the noted
   Slack member ID as its messenger id.
9. On the admin page, re-create the noted suppression rules. Each rule now needs a comment; fields
   left out in the yaml correspond to `*`.
