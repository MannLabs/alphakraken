#!/bin/bash

# wrapper script to run skyline in apptainer to be set as a custom command

IMAGE_PATH=/fs/home/kraken-read/software/skyline/pwiz-skyline.sif

# image needs to be created once:
# apptainer pull $IMAGE_PATH docker://proteowizard/pwiz-skyline-i-agree-to-the-vendor-licenses


set -u -e

echo SETTINGS_PATH=$SETTINGS_PATH
echo RAW_FILE_PATH=$RAW_FILE_PATH
echo RAW_FILE_ID=$RAW_FILE_ID
echo IMAGE_PATH=$IMAGE_PATH

apptainer exec  --bind $SETTINGS_PATH:/data \
		--bind $RAW_FILE_PATH:/input/$RAW_FILE_ID \
		--bind $OUTPUT_PATH:/output \
		--pwd /output \
		$IMAGE_PATH wine SkylineCmd \
		--dir=/data/ \
		--import-file=/input/$RAW_FILE_ID \
		--in=iRT_windows.sky \
		--remove-all  \
		--report-name=custom_iRT_report \
		--report-file=/output/custom_iRT_report.csv \
		--report-invariant \
		--irt-database-path=irt_c18_official.irtdb \
		--report-add=custom_iRT_report.skyr
