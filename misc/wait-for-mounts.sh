#!/bin/bash

# Verify that required mount paths contain data before starting the service.
# Set MOUNT_CHECK_PATHS as a comma-separated list of container-internal paths.
# If any path is empty or missing, exit 1 so Docker's restart policy retries.

if [ -n "$MOUNT_CHECK_PATHS" ]; then
  IFS=',' read -ra PATHS <<< "$MOUNT_CHECK_PATHS"
  for path in "${PATHS[@]}"; do
    if [ ! -d "$path" ] || [ -z "$(ls -A "$path" 2>/dev/null)" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') Mount not ready: $path"
      sleep 5
      exit 1
    fi
  done
  echo "$(date '+%Y-%m-%d %H:%M:%S') All mounts verified."
fi

exec "$@"
