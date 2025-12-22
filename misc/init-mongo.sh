# Initialization script for MongoDB to create users and roles.

# IMPORTANT: This script is run only once when the MongoDB container is initialized.
# If you change it after the DB has been created, the added commands need to be run manually using mongosh (cf. Readme).

# https://stackoverflow.com/a/53522699
q_MONGO_USER_READWRITE=`jq --arg v "$MONGO_USER_READWRITE" -n '$v'`
q_MONGO_PASSWORD_READWRITE=`jq --arg v "$MONGO_PASSWORD_READWRITE" -n '$v'`
q_MONGO_USER_READ=`jq --arg v "$MONGO_USER_READ" -n '$v'`
q_MONGO_PASSWORD_READ=`jq --arg v "$MONGO_PASSWORD_READ" -n '$v'`
q_MONGO_USER_WEBAPP=`jq --arg v "$MONGO_USER_WEBAPP" -n '$v'`
q_MONGO_PASSWORD_WEBAPP=`jq --arg v "$MONGO_PASSWORD_WEBAPP" -n '$v'`

mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin <<EOF
    use krakendb;
    db.createUser({
        user: $q_MONGO_USER_READWRITE,
        pwd: $q_MONGO_PASSWORD_READWRITE,
        roles: ["readWrite"],
    });
    db.createUser({
        user: $q_MONGO_USER_READ,
        pwd: $q_MONGO_PASSWORD_READ,
        roles: ["read"],
    });
    db.createRole({
        role: "webappRole",
        privileges: [
            {
                resource: { db: "krakendb", collection: "project" },
                actions: ["find", "insert", "update", "remove"]
            },
            {
                resource: { db: "krakendb", collection: "settings" },
                actions: ["find", "insert", "update", "remove"]
            },
        ],
        roles: ["read"]
    });
    db.createUser({
        user: $q_MONGO_USER_WEBAPP,
        pwd: $q_MONGO_PASSWORD_WEBAPP,
        roles: ["webappRole"]
    });

    db.settings.createIndex({name: 1, version: 1}, {unique: true});
EOF
