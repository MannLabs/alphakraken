# Alerting active instance

Of several running monitoring instances, only the active one, recorded in the new
`alerting_active_instance` collection, runs the DB-backed checks and notifies. The others only
run the checks that need no DB (local disk, webapp health) and report DB connection errors.
A takeover is warm, as the issue records live in the DB (cf. `upgrade_alert_issue_state.md`).

If the claim cannot be recorded (e.g. missing grant), every instance runs the checks, as before.

## Steps
On the PC (VM) hosting the dbs, `cd` to the alphakraken source directory and `export ENV=production`.

1. Grant the alerting role access to the new collection:
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").grantPrivilegesToRole(\"alertingRole\", [{resource: {db: \"krakendb\", collection: \"alerting_active_instance\"}, actions: [\"find\", \"insert\", \"update\"]}])"'
   ```
2. Deploy the new monitoring version on all machines.

New databases get the grant from `misc/init-mongo.sh` and need none of these steps.
