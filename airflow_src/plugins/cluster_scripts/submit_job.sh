#!/usr/bin/env bash
#SBATCH --job-name=alphakraken_test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=128G
#SBATCH --time=04:00:00
#SxBATCH --partition=p.<node>

set -u -e

# INPUT taken from environment variables
# RAW_FILE_NAME  # e.g. "20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# INSTRUMENT_SUBFOLDER # e.g. "pool-backup/Test2"
# OUTPUT_FOLDER_REL_PATH # e.g. "output/PID123/out_20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# SPECLIB_FILE_NAME # e.g."hela_hybrid.small.hdf"
# FASTA_FILE_NAME # e.g. 2024_01_12_human.fasta
# CONFIG_FILE_NAME #e .g."config.yaml"
# SOFTWARE # e.g. alphadia-1.6.2
# PROJECT_ID # e.g. A123
# IO_POOL_FOLDER # e.g. "pool-projects/alphakraken_sandbox

POOL_FS="/fs/pool/"  # probably okay to hardcode this
POOL_PROJECTS="${POOL_FS}/${IO_POOL_FOLDER}"

# these are determined by convention:
CONDA_ENV=$SOFTWARE
SETTINGS_PATH="${POOL_PROJECTS}/settings/${PROJECT_ID}"
OUTPUT_PATH="${POOL_PROJECTS}/${OUTPUT_FOLDER_REL_PATH}"
INSTRUMENT_BACKUP_FOLDER="${POOL_FS}/${INSTRUMENT_SUBFOLDER}"
RAW_FILE_PATH="${INSTRUMENT_BACKUP_FOLDER}/${RAW_FILE_NAME}"
CONFIG_FILE_PATH="${SETTINGS_PATH}/${CONFIG_FILE_NAME}"

echo CONDA_ENV=${CONDA_ENV}
echo SETTINGS_PATH=${SETTINGS_PATH}
echo OUTPUT_PATH=${OUTPUT_PATH}
echo INSTRUMENT_BACKUP_FOLDER=${INSTRUMENT_BACKUP_FOLDER}
echo RAW_FILE_PATH=${RAW_FILE_PATH}
echo CONFIG_FILE_PATH=${CONFIG_FILE_PATH}

echo OUTPUT_PATH=${OUTPUT_PATH}

# here we assume that at least one of these is set
SPECLIB_COMMAND=""
FASTA_COMMAND=""
if [ -n "$FASTA_FILE_NAME" ]; then
  FASTA_FILE_PATH="${SETTINGS_PATH}/${FASTA_FILE_NAME}"
  echo FASTA_FILE_PATH=${FASTA_FILE_PATH}
  FASTA_COMMAND="--fasta ${FASTA_FILE_PATH}"
fi
if [ -n "$SPECLIB_FILE_NAME" ]; then
  SPECLIB_FILE_PATH="${SETTINGS_PATH}/${SPECLIB_FILE_NAME}"
  echo SPECLIB_FILE_PATH=${SPECLIB_FILE_PATH}
  SPECLIB_COMMAND="--library ${SPECLIB_FILE_PATH}"
fi

mkdir -p ${OUTPUT_PATH}
cd ${OUTPUT_PATH}

echo "Running alphadia.."
echo "Check the logs in ${OUTPUT_PATH}/log.txt"

# TODO how to handle potential overwriting of output data on a second run?
conda run -n $CONDA_ENV alphadia \
    --file "${RAW_FILE_PATH}" \
    ${SPECLIB_COMMAND} \
    ${FASTA_COMMAND} \
    --config "${CONFIG_FILE_PATH}" \
    --output "${OUTPUT_PATH}"

# some other useful commands:
# --directory ${RAW_FOLDER}
# --config-dict '{"fdr": {"inference_strategy": "heuristic"}}'

echo EXIT CODE:
echo $?
