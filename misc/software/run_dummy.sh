#!/bin/bash

# Dummy quanting software to smoke-test a runner with the `simple_ssh` or `pueue_ssh` engine:
# reports what the job handler passed in, waits, and reports one metric.
#
# Usage:
#   run_dummy.sh [any arguments]
#
# Set it as `software` of a settings entry with software type `custom` and metrics type `custom`;
# the arguments come from `config_params`.

set -u

SLEEP_SECONDS=20
METRIC_NAME=dummy_metric
METRIC_VALUE=0.42
METRICS_FILE_NAME=metrics.csv

echo "host:    $(hostname)"
echo "user:    $(whoami)"
echo "workdir: $(pwd)"
echo "args:    $*"

# the whole environment, not just the AlphaKraken variables: the point is to see what a
# non-interactive session on the runner actually gets (PATH, proxies, conda, ...)
echo "--- environment ---"
env | sort
echo "--- end of environment ---"

# fails unless the mounts of the runner match the runner's `view`
ls -ld "${RAW_FILE_PATH}" || echo "RAW_FILE_PATH not readable"
ls -ld "${SETTINGS_PATH}" || echo "SETTINGS_PATH not readable"
ls -ld "${OUTPUT_PATH}" || echo "OUTPUT_PATH not readable"

# long enough for the sensor to see the job in state RUNNING
sleep ${SLEEP_SECONDS}

printf '%s\n%s\n' "${METRIC_NAME}" "${METRIC_VALUE}" > "${OUTPUT_PATH}/${METRICS_FILE_NAME}"

echo "done"
