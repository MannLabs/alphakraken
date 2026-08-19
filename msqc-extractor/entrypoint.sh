#!/bin/bash

# entrypoint of the msqc-extractor container
#
# Required environment variables: RAW_FILE_PATH, OUTPUT_PATH, NUM_THREADS

set -u -e

python /app/main.py "${RAW_FILE_PATH}" "${OUTPUT_PATH}" "${NUM_THREADS}"
