#!/usr/bin/env bash
#SBATCH --job-name=alphakraken_test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=10
#SBATCH --mem=16G
#SBATCH --time=21-04:00:00
#SxBATCH --partition=p.<node>

# TODO replace directives with command-line args

# TODO how to bring this file to the cluster?

set -u -e

# INPUT from environment
# RAW_FILE_NAME=  # e.g. "20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"

# TODO make dynamic
POOL_BACKUP_INSTRUMENT_SUBFOLDER="Test2/2024_06/"
FASTA_FILE_NAME="2024_01_12_human.fasta"
SPECLIB_FILE_NAME="hela_hybrid.small.hdf"
CONFIG_FILE_NAME="config.yaml"
POOL_MS="/fs/pool/pool-backup"
POOL_PROJECTS="/fs/pool/pool-projects/alphakraken_test"
CONDA_ENV="alphadia-1.6.2"



# these are determined by convention
ALPHAKRAKEN_SETTINGS="${POOL_PROJECTS}/settings"
ALPHAKRAKEN_OUTPUT="${POOL_PROJECTS}/output"
INSTRUMENT_BACKUP_FOLDER="${POOL_MS}/${POOL_BACKUP_INSTRUMENT_SUBFOLDER}"
RAW_FILE_PATH="${INSTRUMENT_BACKUP_FOLDER}/${RAW_FILE_NAME}"

CONFIG_FOLDER="${ALPHAKRAKEN_SETTINGS}/config"
SPECLIB_FOLDER="${ALPHAKRAKEN_SETTINGS}/speclib"
FASTA_FOLDER="${ALPHAKRAKEN_SETTINGS}/fasta"

CONFIG_FILE_PATH="${FASTA_FOLDER}/${CONFIG_FILE_NAME}"
FASTA_FILE_PATH="${FASTA_FOLDER}/${FASTA_FILE_NAME}"
SPECLIB_FILE_PATH="${SPECLIB_FOLDER}/${SPECLIB_FILE_NAME}"

echo ALPHAKRAKEN_SETTINGS=${ALPHAKRAKEN_SETTINGS}
echo ALPHAKRAKEN_OUTPUT=${ALPHAKRAKEN_OUTPUT}
echo INSTRUMENT_BACKUP_FOLDER=${INSTRUMENT_BACKUP_FOLDER}
echo RAW_FILE_PATH=${RAW_FILE_PATH}
echo CONFIG_FOLDER=${CONFIG_FOLDER}
echo SPECLIB_FOLDER=${SPECLIB_FOLDER}
echo FASTA_FOLDER=${FASTA_FOLDER}

echo CONFIG_FILE_PATH=${CONFIG_FILE_PATH}
echo SPECLIB_FILE_PATH=${SPECLIB_FILE_PATH}
echo FASTA_FILE_PATH=${FASTA_FILE_PATH}


OUTPUT_FOLDER="${ALPHAKRAKEN_OUTPUT}/out_${RAW_FILE_NAME}"
echo OUTPUT_FOLDER=${OUTPUT_FOLDER}

mkdir -p ${OUTPUT_FOLDER}
cd ${OUTPUT_FOLDER}

conda run -n $CONDA_ENV \
    alphadia \
    --file ${RAW_FILE_PATH} \
    -o ${OUTPUT_FOLDER} \
    --library ${SPECLIB_FILE_PATH}
    --config ${CONFIG_FILE_PATH}

# some other useful commands:
# --directory ${RAW_FOLDER}
# --fasta $FASTA_FILE_PATH
# --config-dict '{"fdr": {"inference_strategy": "heuristic"}}'
