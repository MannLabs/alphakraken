# Alert suppressions: managed in the webapp

Alert suppressions moved from `monitoring/suppressions.yaml` to the `alert_suppression` collection
in MongoDB. They are managed on the webapp's "Admin" page and picked up by the monitoring with
its next check, without restart. `suppressions.yaml` is no longer read.

1. Note down the rules from `monitoring/suppressions.yaml` of each deployment, if any.
2. Allow the webapp to write the new collection, logged into the DB as admin user
   (cf. `docs/maintenance.md`):

   ```js
   use krakendb;
   db.grantPrivilegesToRole("webappRole", [
       {
           resource: { db: "krakendb", collection: "alert_suppression" },
           actions: ["find", "insert", "update", "remove"]
       },
   ]);
   ```

3. Deploy, then re-create the noted rules on the "Admin" page. Each rule now needs a comment.
   Fields left out in the yaml correspond to `*`.
