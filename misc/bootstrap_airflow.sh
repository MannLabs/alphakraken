#!/bin/bash

# Populate the Airflow Variables and Pools that AlphaKraken requires with defaults.
# Existing entries are never touched, so this script can be re-run at any time.
#
# Usage: export ENV=local; ./misc/bootstrap_airflow.sh [--init]
#
#   --init  additionally initialize the airflow database and create the UI user before bootstrapping
#
# Values mirror docs/maintenance.md#airflow-variables and constants.py:Pools.

set -e -u

RUN_INIT=false
if [ $# -gt 0 ]; then
  if [ "$1" == "--init" ] && [ $# -eq 1 ]; then
    RUN_INIT=true
  else
    echo "Usage: $0 [--init]"
    exit 1
  fi
fi

if [ "${ENV:-}" == "" ]; then
  echo "Please set the ENV variable, e.g. 'export ENV=local' (or 'export ENV=sandbox', 'export ENV=production')"
  exit 1
fi

echo "Make sure you run this on the machine hosting the Airflow postgres DB (Ctrl-C to cancel)"
read -r -p "Press Enter to continue .. "

# the local setup has no cluster to connect to
DEBUG_NO_CLUSTER_SSH=False
if [ "${ENV}" == "local" ]; then
  DEBUG_NO_CLUSTER_SSH=True
fi

# key|value
VARIABLES=(
  "consider_old_files_acquired|False"
  "checksum_overwrite_file_id|"
  "backup_overwrite_file_id|"
  "output_exists_mode|raise"
  "min_free_space_gb|-1"
  "min_file_age_to_remove_in_days|14"
  "debug_no_cluster_ssh|${DEBUG_NO_CLUSTER_SSH}"
  "debug_max_file_age_in_hours|-1"
)

# name|slots|description
POOLS=(
  "file_copy_pool|3|limits file copying across all instruments"
  "cluster_slots_pool|30|limits the number of concurrent jobs on the cluster"
  "s3_upload_pool|2|limits S3 uploads across all instruments"
)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ "${RUN_INIT}" == true ]; then
  echo "Initializing the airflow database .."
  "${REPO_ROOT}/compose.sh" --profile dbs up airflow-init
fi

remote_script=""
for entry in "${VARIABLES[@]}"; do
  key="${entry%%|*}"
  value="${entry#*|}"
  remote_script+="if airflow variables get '${key}' > /dev/null 2>&1; then echo \"variable ${key}: exists, skipping\"; else echo \"variable ${key}: setting to '${value}'\"; airflow variables set '${key}' '${value}'; fi"$'\n'
done

for entry in "${POOLS[@]}"; do
  name="${entry%%|*}"
  rest="${entry#*|}"
  slots="${rest%%|*}"
  description="${rest#*|}"
  remote_script+="if airflow pools get '${name}' > /dev/null 2>&1; then echo \"pool ${name}: exists, skipping\"; else echo \"pool ${name}: creating with ${slots} slots\"; airflow pools set '${name}' '${slots}' '${description}'; fi"$'\n'
done

echo "$remote_script" | "${REPO_ROOT}/compose.sh" run --rm -T airflow-cli bash -s

echo
echo "Done. Review the values in the Airflow UI, they are deliberately conservative:"
echo " - cluster_slots_pool must match the capacity of your cluster or standalone host"
echo " - min_free_space_gb=-1 disables file removal; production should use e.g. 300"
