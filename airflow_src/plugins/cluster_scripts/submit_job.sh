#!/usr/bin/env bash
#SBATCH --job-name=alphakraken_test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SxBATCH --partition=p.<node>
# TODO replace directives with command-line args

set -u -e

# INPUT taken from environment variables
# RAW_FILE_NAME  # e.g. "20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# POOL_BACKUP_INSTRUMENT_SUBFOLDER # e.g. "pool-backup/Test2"
# OUTPUT_FOLDER_NAME # e.g. "out_20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# SPECLIB_FILE_NAME # e.g."hela_hybrid.small.hdf"
# FASTA_FILE_NAME # e.g. 2024_01_12_human.fasta
# CONFIG_FILE_NAME #e .g."config.yaml"
# SOFTWARE # e.g. alphadia-1.6.2
# PROJECT_ID # e.g. A123

# TODO make dynamic
POOL_FS="/fs/pool/" # TODO get from .env
POOL_PROJECTS="${POOL_FS}/pool-projects/alphakraken_test"  # TODO get from .env

# TODO document this:
#### Add settings
#Upload fasta files, spectral libraries and config files in subfolders
#`fasta`, `speclib`, and `config`, respectively, of POOL_PROJECTS}/settings.

# these are determined by convention:
CONDA_ENV=$SOFTWARE
SETTINGS_PATH="${POOL_PROJECTS}/settings/${PROJECT_ID}"
OUTPUT_PATH="${POOL_PROJECTS}/output/${PROJECT_ID}"
INSTRUMENT_BACKUP_FOLDER="${POOL_FS}/${POOL_BACKUP_INSTRUMENT_SUBFOLDER}"
RAW_FILE_PATH="${INSTRUMENT_BACKUP_FOLDER}/${RAW_FILE_NAME}"
CONFIG_FILE_PATH="${SETTINGS_PATH}/${CONFIG_FILE_NAME}"
FASTA_FILE_PATH="${SETTINGS_PATH}/${FASTA_FILE_NAME}"
SPECLIB_FILE_PATH="${SETTINGS_PATH}/${SPECLIB_FILE_NAME}"
OUTPUT_PATH="${OUTPUT_PATH}/${OUTPUT_FOLDER_NAME}"

echo CONDA_ENV=${CONDA_ENV}
echo SETTINGS_PATH=${SETTINGS_PATH}
echo OUTPUT_PATH=${OUTPUT_PATH}
echo INSTRUMENT_BACKUP_FOLDER=${INSTRUMENT_BACKUP_FOLDER}
echo RAW_FILE_PATH=${RAW_FILE_PATH}
echo CONFIG_FILE_PATH=${CONFIG_FILE_PATH}
echo SPECLIB_FILE_PATH=${SPECLIB_FILE_PATH}
echo FASTA_FILE_PATH=${FASTA_FILE_PATH}
echo OUTPUT_PATH=${OUTPUT_PATH}

mkdir -p ${OUTPUT_PATH}
cd ${OUTPUT_PATH}

echo "Running alphadia.."
echo "Check the logs in ${OUTPUT_PATH}/log.txt"

# TODO how to handle potential overwriting on a second run?
conda run -n $CONDA_ENV alphadia \
    --file "${RAW_FILE_PATH}" \
    --library "${SPECLIB_FILE_PATH}" \
    --config "${CONFIG_FILE_PATH}" \
    --output "${OUTPUT_PATH}"
# TODO enable fasta

# some other useful commands:
# --directory ${RAW_FOLDER}
# --fasta $FASTA_FILE_PATH
# --config-dict '{"fdr": {"inference_strategy": "heuristic"}}'

echo EXIT CODE:
echo $?
