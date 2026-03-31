#!/bin/bash

# wrapper script to run skyline in apptainer to be set as a custom command
#
# Usage:
#   run_skyline.sh --in <sky_file> --irt-database-path <irtdb_file> [--report-add <skyr_file>]
#
# Required environment variables: SETTINGS_PATH, RAW_FILE_PATH, RAW_FILE_ID, OUTPUT_PATH

IMAGE_PATH=/fs/home/kraken-read/software/skyline/pwiz-skyline.sif

# image needs to be created once:
# apptainer pull $IMAGE_PATH docker://proteowizard/pwiz-skyline-i-agree-to-the-vendor-licenses

set -u -e

# parse arguments
REPORT_TEMPLATE=""
while [[ $# -gt 0 ]]; do
	case $1 in
		--in) SKY_FILE="$2"; shift 2 ;;
		--irt-database-path) IRT_DATABASE="$2"; shift 2 ;;
		--report-add) REPORT_TEMPLATE="$2"; shift 2 ;;
		*) echo "Unknown argument: $1"; exit 1 ;;
	esac
done

echo SETTINGS_PATH=$SETTINGS_PATH
echo RAW_FILE_PATH=$RAW_FILE_PATH
echo RAW_FILE_ID=$RAW_FILE_ID
echo IMAGE_PATH=$IMAGE_PATH
echo SKY_FILE=$SKY_FILE
echo IRT_DATABASE=$IRT_DATABASE
echo REPORT_TEMPLATE=$REPORT_TEMPLATE

REPORT_ADD_FLAG=""
if [[ -n "$REPORT_TEMPLATE" ]]; then
	REPORT_ADD_FLAG="--report-add=$REPORT_TEMPLATE"
fi

apptainer exec  --bind "$SETTINGS_PATH":/data \
		--bind "$RAW_FILE_PATH":/input/"$RAW_FILE_ID" \
		--bind "$OUTPUT_PATH":/output \
		--pwd /output \
		"$IMAGE_PATH" wine SkylineCmd \
		--dir=/data/ \
		--import-file=/input/"$RAW_FILE_ID" \
		--in="$SKY_FILE" \
		--remove-all  \
		--report-name=custom_iRT_report \
		--report-file=/output/custom_iRT_report.csv \
		--report-invariant \
		--irt-database-path="$IRT_DATABASE" \
		$REPORT_ADD_FLAG
