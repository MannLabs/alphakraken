# Alerting active instance: MongoDB user for the monitoring service

Of several running monitoring instances, only the active one, recorded in the new
`alerting_active_instance` collection, sends alerts. The monitoring service therefore now connects
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
               resource: { db: "krakendb", collection: "alerting_active_instance" },
               actions: ["find", "insert", "update"]
           },
       ],
       roles: ["read"]
   });
   db.createUser({user: "<MONGO_USER_ALERTING>", pwd: "<MONGO_PASSWORD_ALERTING>", roles: ["alertingRole"]});
   ```

If the active instance cannot be recorded (e.g. user not yet created), every instance sends alerts, as before.
