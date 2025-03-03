#!/bin/bash

# A little helper for mounting.
# Will read data from environmental-specific alphakraken.yaml file and do the mounting.

# IMPORTANT NOTE: it is absolutely crucial that the mounts are set correctly in the respective alphakraken.yaml file!
# Make sure the data (user names, ip addresses) are always up to date!
# If you need to change this script, do so with caution and test it on a sandbox first!

# This could be used to loop over all instruments:
# for i in $(python -c "import yaml, sys; print(' '.join(yaml.safe_load(open(sys.argv[1]))[sys.argv[2]].keys()))" envs/alphakraken.$ENV.yaml instruments); do echo $i; done

if [ "$ENV" == "" ]; then
  echo "Please set the ENV variable, e.g. 'export ENV=local' (or 'export ENV=sandbox', 'export ENV=production')"
  exit 1
fi

set -e -u

if [ -z "${1:-}" ] ; then
  echo "Usage: $0 <entity> [umount]"
  echo "<entity> can be an instrument name (e.g. test1, ..) or a special folder (logs, backup, or output)."
  echo "If 'unmount' is passed, the target folder will be unmounted first."
  echo
  echo "Example 1: $0 test1"
  echo "Example 2: $0 backup umount"
  exit 1
fi

# arguments:
ENTITY=$1 # instrument name as defined in alphakraken.yaml, e.g. test1
DO_UMOUNT=${2:-}  # set to "umount" for unmounting first

FILE_NAME=envs/alphakraken.$ENV.yaml
if [ ! -f $FILE_NAME ]; then
   echo Configuration file $FILE_NAME not found.
   exit 1
fi

get_data() {
  # get a certain information (key) for an entity from alphakraken.yaml ($FILE_NAME)
  local entity_type=$1
  local entity=$2
  local key=$3
  python3 -c 'import yaml, sys; print(yaml.safe_load(open(sys.argv[1]))[sys.argv[2]][sys.argv[3]][sys.argv[4]])' "$FILE_NAME" "$entity_type" "$entity" "$key"
}

# a little hack to look up the correct information
if [[ "$ENTITY" == "backup" || "$ENTITY" == "output" || "$ENTITY" == "logs" ]]; then
  ENTITY_TYPE="locations"
else
  ENTITY_TYPE="instruments"
fi

MOUNTS_PATH=$(get_data locations general mounts_path)
USERNAME=$(get_data $ENTITY_TYPE $ENTITY username)
PASSWORD=$(get_data $ENTITY_TYPE $ENTITY password) || echo Password not found, needs to be entered manually
MOUNT_SRC="$(get_data $ENTITY_TYPE $ENTITY mount_src)"
MOUNT_TARGET="$(get_data $ENTITY_TYPE $ENTITY mount_target)"

MOUNT_TARGET=$MOUNTS_PATH/$MOUNT_TARGET

echo MOUNT_SRC=$MOUNT_SRC
echo MOUNT_TARGET=$MOUNT_TARGET
echo USERNAME=$USERNAME
echo PASSWORD=$PASSWORD
echo ENV=$ENV

echo "Are you sure you want to continue? (ENTER to continue, CTRL-C to abort)"
read IGNORE


# to prevent mounting into the wild the user needs to create the mounts dir
if [ ! -e $MOUNT_TARGET ]; then
  echo Mounts directory does not exist. Check if it is correct: \'${MOUNT_TARGET}\'. Create it if desired.
  exit 1
fi

isMounted() { findmnt "$1" > /dev/null && echo 1 || echo 0; }

if [ -n "$(find $MOUNT_TARGET -mindepth 1 -maxdepth 1)" ] && [ $(isMounted $MOUNT_TARGET) == 0 ]; then
  echo "Mount target path is not a mount and is not empty: '${MOUNT_TARGET}'"
  echo "Check if data has been written to a local folder by accident and take care of it (e.g. move it away)."
  exit 1
fi

echo mounting "$MOUNT_SRC" to "$MOUNT_TARGET"

if [ "${DO_UMOUNT}" == "umount" ]; then
  if [ $(isMounted $MOUNT_TARGET) == 0 ]; then
    echo "Nothing to unmount, $MOUNT_TARGET is not mounted."
    exit
  fi
  echo unmounting "$MOUNT_TARGET" ...
  sudo umount "$MOUNT_TARGET"
fi

# pass user id (uid) and group id (gid) otherwise would be mounted as root
if [ -n "$PASSWORD" ]; then
  sudo mount -t cifs -o username=$USERNAME,password=$PASSWORD,uid=$(id -u),gid=$(id -g) "$MOUNT_SRC" "$MOUNT_TARGET"
else
  sudo mount -t cifs -o username=$USERNAME,uid=$(id -u),gid=$(id -g) "$MOUNT_SRC" "$MOUNT_TARGET"
fi

echo contents of  "$MOUNT_TARGET":
ls "$MOUNT_TARGET"
