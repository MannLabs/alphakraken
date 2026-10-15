# MongoDB 6.0 → 8.0

MongoDB upgrades one major version at a time: 6.0 → 7.0 → 8.0.
No data migration is needed; users and roles carry over.

Downgrading is not supported once on 7.0: the only rollback is restoring the backup (step 2),
losing all writes made since.

Driver pins (`pymongo==4.7.2`, `mongoengine==0.28.2`) work with 8.0 (without support for new 8.0 features)
and stay unchanged during the upgrade.

Test on `sandbox` first.

## Steps
On the PC (VM) hosting the dbs, `cd` to the alphakraken source directory and `export ENV=production`.

1. Stop all services that access MongoDB (workers, infrastructure, webapp, monitoring), then MongoDB:
   `./compose.sh --profile dbs stop mongodb-service`
2. Create a backup (MongoDB must be stopped, do not use `misc/backup_db.sh`):
   `sudo cp -a mongodb_data_${ENV} mongodb_data_${ENV}.bak_6.0`
3. Start MongoDB and check that the feature compatibility version is `6.0`:
   `./compose.sh --profile dbs up -d mongodb-service`
   `./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.adminCommand({getParameter: 1, featureCompatibilityVersion: 1})"'`
4. Upgrade to 7.0:
   1. In `docker-compose.yaml`, set the `mongodb-service` image to `mongo:7.0-jammy`.
   2. `./compose.sh --profile dbs up -d mongodb-service` and check `./compose.sh --profile dbs logs mongodb-service` for errors.
   3. Set the feature compatibility version:
      `./compose.sh --profile dbs exec mongodb-service sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin --quiet --eval "db.adminCommand({setFeatureCompatibilityVersion: \"7.0\", confirm: true})"'`
   4. Verify with the command from step 3 (expect `7.0`). Otherwise, 8.0 will refuse to start (exit code 62).
5. Upgrade to 8.0: repeat step 4 with image `mongo:8.0-noble` and `"8.0"`.
6. Verify with the command from step 3 (expect `8.0`), then restart all other services.
7. Once operation is confirmed, delete `mongodb_data_${ENV}.bak_6.0`.
