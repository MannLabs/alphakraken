#!/bin/bash

# create backup of the mongodb data folder to a target folder

TARGET_FOLDER="/home/kraken-user/alphakraken/production/mounts/db_backups"
MONGODB_FOLDER="/path/to/mongodb_data_production"

set -e -u

TYPE_OF_BACKUP=$1  # "nightly" or "hourly"

START_DATE=$(date)
echo Starting backup: $START_DATE

TARGET="${TARGET_FOLDER}/$(date +\%A | tr '[:upper:]' '[:lower:]')"
if [ "$TYPE_OF_BACKUP" == "hourly" ]; then
    rsync -rltvz --delete $MONGODB_FOLDER    $TARGET_FOLDER/hourly >> $TARGET_FOLDER/dbbackups_hourly.log  2>&1
else
    rsync -rltvz --delete $MONGODB_FOLDER   "$TARGET/" >> $TARGET_FOLDER/dbbackups.log  2>&1
fi


echo End backup (started $START_DATE) on $(date)

# cron configuration:
# 0 0 * * * /path/to/backup_db.sh nightly
# 0 * * * * /path/to/backup_db.sh hourly
