# https://stackoverflow.com/a/53522699
q_MONGO_USER=`jq --arg v "$MONGO_USER" -n '$v'`
q_MONGO_PASSWORD=`jq --arg v "$MONGO_PASSWORD" -n '$v'`

mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" admin <<EOF
    use krakendb;
    db.createUser({
        user: $q_MONGO_USER,
        pwd: $q_MONGO_PASSWORD,
        roles: ["readWrite"],
    });
EOF
