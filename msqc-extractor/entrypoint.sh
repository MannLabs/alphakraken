#!/bin/bash

# entrypoint of the msqc-extractor container
#
# Arguments are passed on to main.py unchanged. If none are given, they are taken from the
# environment variables RAW_FILE_PATH, OUTPUT_PATH and NUM_THREADS, which the `docker` job engine
# sets for every job.

set -u -e

if [ $# -eq 0 ]; then
	set -- "${RAW_FILE_PATH}" "${OUTPUT_PATH}" "${NUM_THREADS}"
fi

echo "Running: python /app/main.py $*"

exec python /app/main.py "$@"
