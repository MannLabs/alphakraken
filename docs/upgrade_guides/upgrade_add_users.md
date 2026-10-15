# Adding users (owner of projects and settings)

The webapp now writes to the new `user` collection. On databases created before this change,
the webapp's MongoDB role (`webappRole`) lacks write access to it. Without access, adding a user
on the admin page fails with an authorization error. Everything else keeps working.

There is no data migration. Existing projects and settings keep having no owner.

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
