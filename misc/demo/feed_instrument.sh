#!/bin/bash

# Fakes an instrument acquiring one raw file every INTERVAL_S seconds: sleeps, then copies the next
# source file into the demo instrument folder with a timestamp appended to its name. Cycles through
# the source files forever.
#
# Note that an acquisition is considered finished once the *next* file shows up in the instrument
# folder (cf. AcquisitionMonitor), so this interval is what paces the whole pipeline.
#
# Run in the foreground to watch it, or detached:
#   nohup misc/demo/feed_instrument.sh > misc/demo/.state/feeder.log 2>&1 &

set -e -u

DEMO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

SOURCE_DIR=${SOURCE_DIR:-${DEMO_DIR}/raw_files}
TARGET_DIR=${TARGET_DIR:-${DEMO_DIR}/mounts/instruments/demo1}
# 21 minutes, matching the gradient length of the demo files
INTERVAL_S=${INTERVAL_S:-1260}

SOURCE_FILES=()
while IFS= read -r source_file; do
  SOURCE_FILES+=("$source_file")
done < <(find "$SOURCE_DIR" -maxdepth 1 -name '*.raw' 2> /dev/null | sort)

if [ ${#SOURCE_FILES[@]} -eq 0 ]; then
  echo "No .raw files in ${SOURCE_DIR}. Run 'python misc/demo/download_raw_files.py' first."
  exit 1
fi

if [ ! -d "$TARGET_DIR" ]; then
  echo "Target folder ${TARGET_DIR} does not exist. Run 'misc/demo/setup_demo.sh' first."
  exit 1
fi

echo "Feeding ${#SOURCE_FILES[@]} raw file(s) from ${SOURCE_DIR} to ${TARGET_DIR} every ${INTERVAL_S}s."

i=0
while true; do
  sleep "$INTERVAL_S"

  source_file=${SOURCE_FILES[$i]}
  stem=$(basename "$source_file" .raw)
  # ':' is not allowed in raw file names, cf. shared/validation.py
  target_file="${TARGET_DIR}/${stem}_$(date +%Y%m%d-%H%M%S).raw"

  echo "$(date +%Y-%m-%dT%H:%M:%S) acquiring $(basename "$target_file")"
  # copied in place rather than written to a temp file and moved: a partially written file is what a
  # real acquisition looks like, and the acquisition monitor is built to handle it
  cp "$source_file" "$target_file"

  i=$(((i + 1) % ${#SOURCE_FILES[@]}))
done
