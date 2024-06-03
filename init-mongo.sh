# https://stackoverflow.com/a/53522699
q_MONGO_USER=<mongo_user>
q_MONGO_PASSWORD=<password>

mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin <<EOF
    use krakendb;
    db.createUser({
        user: $q_MONGO_USER,
        pwd: $q_MONGO_PASSWORD,
        roles: ["readWrite"],
    });
EOF
