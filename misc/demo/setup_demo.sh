#!/bin/bash

# Sets up the self-contained AlphaKraken demo from scratch. Safe to re-run: the demo UUID is
# generated once and kept, everything else is rendered or reconciled.
#
# Set DEMO_HOST to the host name the demo is reached under (Airflow needs it up front, as it
# generates absolute URLs). Defaults to the fully qualified host name.
#
# Does not start the feeder, cf. misc/demo/README.md.

set -e -u

DEMO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "${DEMO_DIR}/../.." && pwd)

STATE_DIR=${DEMO_DIR}/.state
MOUNTS_DIR=${DEMO_DIR}/mounts
INSTRUMENT=demo1

DEMO_HOST=${DEMO_HOST:-$(hostname -f)}

MSQC_IMAGE=alphakraken-msqc

# mirrors compose.sh, which also assumes docker needs elevated privileges
DOCKER="sudo docker"

cd "$REPO_DIR"

# ########################################### PREFLIGHT

for cert in certs/fullchain.pem certs/privkey.pem; do
  if [ ! -f "$cert" ]; then
    echo "Missing ${cert}. The demo serves https and reuses the existing certificate."
    exit 1
  fi
done

if [ ! -f envs/.env-airflow ]; then
  echo "Missing envs/.env-airflow. Create it with: echo \"AIRFLOW_UID=\$(id -u)\" > envs/.env-airflow"
  exit 1
fi

if [ ! -S /var/run/docker.sock ]; then
  echo "Missing /var/run/docker.sock. The 'docker' job engine starts the quanting containers through it."
  exit 1
fi
DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)

# compose.sh needs sudo, and the retry loops below run it with their output suppressed, where a
# password prompt would not be visible. Ask for it once, up front.
sudo -v

# ########################################### RENDER CONFIGURATION

mkdir -p "$STATE_DIR"

if [ ! -f "${STATE_DIR}/uuid" ]; then
  uuidgen | tr '[:upper:]' '[:lower:]' > "${STATE_DIR}/uuid"
  echo "Generated a new demo UUID."
fi
UUID=$(cat "${STATE_DIR}/uuid")

render() {
  sed -e "s|__UUID__|${UUID}|g" \
    -e "s|__DEMO_HOST__|${DEMO_HOST}|g" \
    -e "s|__ABS_MOUNTS_PATH__|${MOUNTS_DIR}|g" \
    -e "s|__DOCKER_GID__|${DOCKER_GID}|g" \
    "$1" > "$2"
  echo "  rendered $2"
}

echo "Rendering configuration for ${DEMO_HOST}:"
render "${DEMO_DIR}/templates/demo.env.template" envs/demo.env
render "${DEMO_DIR}/templates/alphakraken.demo.yaml.template" envs/alphakraken.demo.yaml
render "${DEMO_DIR}/templates/nginx.demo.conf.template" "${STATE_DIR}/nginx.conf"

mkdir -p "${MOUNTS_DIR}/instruments/${INSTRUMENT}/Backup" \
  "${MOUNTS_DIR}/backup/${INSTRUMENT}" \
  "${MOUNTS_DIR}/output" \
  "${MOUNTS_DIR}/settings/config" \
  "${MOUNTS_DIR}/settings/fasta" \
  "${MOUNTS_DIR}/settings/speclib" \
  "${MOUNTS_DIR}/airflow_logs" \
  "${DEMO_DIR}/raw_files"

# ########################################### BUILD AND START

echo "Building the ${MSQC_IMAGE} image .."
$DOCKER build -t "$MSQC_IMAGE" msqc-extractor

export ENV=demo

echo "Initializing the Airflow database .."
./compose.sh --profile dbs up airflow-init

echo "Starting the demo .."
./compose.sh --profile demo up --build -d

# ########################################### CONFIGURE

retry() {
  local description=$1
  shift
  for _ in $(seq 30); do
    if "$@" > /dev/null 2>&1; then
      return 0
    fi
    sleep 10
  done
  echo "Timed out waiting for: ${description}"
  return 1
}

echo "Waiting for the Airflow webserver .."
retry "airflow webserver" curl -sf "http://127.0.0.1:8080/${UUID}/airflow/health"

airflow_cli() {
  ./compose.sh --profile debug run --rm airflow-cli bash -c "$1"
}

echo "Configuring Airflow .."
airflow_cli "airflow pools set cluster_slots_pool 4 'concurrent quanting jobs on the demo host'"
airflow_cli "airflow pools set file_copy_pool 3 'concurrent file transfers'"
airflow_cli "airflow variables set debug_no_cluster_ssh True"

for dag_id in "instrument_watcher.${INSTRUMENT}" \
  "acquisition_handler.${INSTRUMENT}" \
  "acquisition_processor.${INSTRUMENT}" \
  "file_mover.${INSTRUMENT}" \
  file_remover; do
  echo "  unpausing ${dag_id}"
  retry "DAG ${dag_id} to be parsed" airflow_cli "airflow dags unpause ${dag_id}"
done

# not going through retry(): the seed script's stdin has to be re-read on every attempt, and its
# output is worth seeing
echo "Seeding the database .."
for _ in $(seq 30); do
  if ./compose.sh --profile demo exec -T webapp python - < "${DEMO_DIR}/seed_db.py"; then
    seeded=true
    break
  fi
  sleep 10
done
if [ "${seeded:-false}" != "true" ]; then
  echo "Timed out waiting for: the database to accept the seed"
  exit 1
fi

# ########################################### REPORT

cat <<EOF

The demo is up:

  webapp    https://${DEMO_HOST}/${UUID}/
  airflow   https://${DEMO_HOST}/${UUID}/airflow/   (airflow / airflow)
  mcp       https://${DEMO_HOST}/${UUID}/mcp
  rest api  https://${DEMO_HOST}/${UUID}/api/docs

Start the acquisition feeder (once misc/demo/raw_files is populated by download_raw_files.py):

  nohup misc/demo/feed_instrument.sh > ${STATE_DIR}/feeder.log 2>&1 &

Bound the disk usage by adding this to your crontab (\`crontab -e\`):

  0 * * * * ${DEMO_DIR}/prune_demo_data.sh >> ${STATE_DIR}/prune.log 2>&1
EOF
