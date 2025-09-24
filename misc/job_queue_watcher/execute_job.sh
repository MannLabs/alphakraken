#!/bin/bash
# Unix shell script to execute jobs independently from the Python watcher
# Usage: execute_job.sh temp_file_path

set -e

# Read parameters from temporary file to avoid command line parsing issues
TEMP_FILE="$1"

# Read parameters from temp file (one per line)
CUSTOM_COMMAND=$(sed -n '1p' "$TEMP_FILE")
OUTPUT_PATH=$(sed -n '2p' "$TEMP_FILE")
RAW_FILE_ID=$(sed -n '3p' "$TEMP_FILE")

# Debug: Show parameters as received from temp file
echo "DEBUG: Parameters read from temp file:"
echo "DEBUG: CUSTOM_COMMAND: [$CUSTOM_COMMAND]"
echo "DEBUG: OUTPUT_PATH: [$OUTPUT_PATH]"
echo "DEBUG: RAW_FILE_ID: [$RAW_FILE_ID]"
echo "DEBUG: Temp file: $TEMP_FILE"
echo

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_PATH"

# Create log file path
LOG_FILE="$OUTPUT_PATH/job_status.log"

# Get current timestamp
timestamp=$(date '+%H:%M:%S')

# Write execution start to log
echo "[$timestamp] Executing: $CUSTOM_COMMAND" >> "$LOG_FILE"

# Execute the custom command and redirect all output to log file
if eval "$CUSTOM_COMMAND" >> "$LOG_FILE" 2>&1; then
    # Command succeeded
    EXIT_CODE=0
    timestamp=$(date '+%H:%M:%S')
    echo "[$timestamp] Process completed successfully (exit code: $EXIT_CODE)" >> "$LOG_FILE"
    echo "COMPLETED" >> "$LOG_FILE"
else
    # Command failed
    EXIT_CODE=$?
    timestamp=$(date '+%H:%M:%S')
    echo "[$timestamp] Process failed with exit code: $EXIT_CODE" >> "$LOG_FILE"
    echo "FAILED" >> "$LOG_FILE"
fi

# Clean up temporary file
if [ -f "$TEMP_FILE" ]; then
    rm "$TEMP_FILE" 2>/dev/null || true
fi

# Exit with the same code as the original command
exit $EXIT_CODE
