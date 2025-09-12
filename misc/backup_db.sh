#!/bin/bash

# create backup of the mongodb data folder to a target folder

set -e -u

TARGET_FOLDER="/home/kraken-user/alphakraken/production/mounts/db_backups"
MONGODB_FOLDER="/path/to/mongodb_data_production"
TYPE_OF_BACKUP=$1  # "nightly" or "hourly"

TARGET="${TARGET_FOLDER}$(date +\%A | tr '[:upper:]' '[:lower:]')"
if [ "$TYPE_OF_BACKUP" == "hourly" ]; then
    rsync -rltvz --delete $MONGODB_FOLDER    $TARGET_FOLDER/hourly >> $TARGET_FOLDER/dbbackups_hourly.log  2>&1
else
    rsync -rltvz --delete $MONGODB_FOLDER   "$TARGET/" >> $TARGET_FOLDER/dbbackups.log  2>&1
fi

# cron configuration:
# 0 0 * * * /path/to/backup_db.sh nightly
# 0 * * * * /path/to/backup_db.sh hourly
