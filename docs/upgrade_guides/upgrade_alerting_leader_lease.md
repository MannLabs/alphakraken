# Alerting leader lease: MongoDB user for the monitoring service

Of several running monitoring instances, only the one holding a lease in the new
`alerting_leader_lease` collection sends alerts. The monitoring service therefore now connects
as a new user `MONGO_USER_ALERTING` that may write to that collection. `init-mongo.sh` only runs
on a fresh DB, so on existing deployments:

1. Add `MONGO_USER_ALERTING` and `MONGO_PASSWORD_ALERTING` to `envs/${ENV}.env` on all machines
   (`docker-compose.yaml` requires them, so compose refuses to start otherwise).
2. Create role and user, logged into the DB as admin user (cf. `docs/maintenance.md`):

   ```js
   use krakendb;
   db.createRole({
       role: "alertingRole",
       privileges: [
           {
               resource: { db: "krakendb", collection: "alerting_leader_lease" },
               actions: ["find", "insert", "update"]
           },
       ],
       roles: ["read"]
   });
   db.createUser({user: "<MONGO_USER_ALERTING>", pwd: "<MONGO_PASSWORD_ALERTING>", roles: ["alertingRole"]});
   ```

If the lease cannot be written (e.g. user not yet created), every instance sends alerts, as before.
