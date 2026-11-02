# Alert suppressions: managed in the webapp

Alert suppressions moved from `monitoring/suppressions.yaml` to the `alert_suppression` collection
in MongoDB. They are managed on the webapp's "Admin" page and picked up by the monitoring with
its next check, without restart. `suppressions.yaml` is no longer read.

## Steps
On the PC (VM) hosting the dbs, `cd` to the alphakraken source directory and `export ENV=production`.

1. Note down the rules from `monitoring/suppressions.yaml` of each deployment, if any.
2. Grant the webapp role access to the new collection:
   ```bash
   ./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.getSiblingDB(\"krakendb\").grantPrivilegesToRole(\"webappRole\", [{resource: {db: \"krakendb\", collection: \"alert_suppression\"}, actions: [\"find\", \"insert\", \"update\", \"remove\"]}])"'
   ```
3. Deploy, then re-create the noted rules on the "Admin" page. Each rule now needs a comment.
   Fields left out in the yaml correspond to `*`.

New databases get the grant from `misc/init-mongo.sh` and need none of these steps.
