#!/bin/bash

# wrapper script to run msqc
#
# Usage:
#   run_msqc.sh
#
# Required environment variables:  RAW_FILE_ID, OUTPUT_PATH, NUM_THREADS

set -u -e
CONDA_ENV=msqc-extractor
SOFTWARE_DIR=/fs/home/kraken-read/software/alphakraken

conda run -n $CONDA_ENV python $SOFTWARE_DIR/msqc-extractor/main.py "${RAW_FILE_PATH}" "${OUTPUT_PATH}" $NUM_THREADS
