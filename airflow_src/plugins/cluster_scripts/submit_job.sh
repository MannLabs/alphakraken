#!/usr/bin/env bash
#SBATCH --job-name=alphakraken
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SxBATCH --partition=p.<node>

set -u -e

# INPUT taken from environment variables:
# RAW_FILE_PATH # e.g. "/fs/pool/pool-backup/Test2/2024_07/20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# SETTINGS_PATH # e.g. "/fs/pool/pool-alphakraken/settings/PID123"
# OUTPUT_PATH # e.g. "/fs/pool/pool-alphakraken/output/PID123/out_20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# SPECLIB_FILE_NAME # e.g."hela_hybrid.small.hdf"
# FASTA_FILE_NAME # e.g. 2024_01_12_human.fasta
# CONFIG_FILE_NAME #e.g. "config.yaml"
# SOFTWARE # e.g. alphadia-1.6.2

# these are determined by convention:
CONDA_ENV=$SOFTWARE

CONFIG_FILE_PATH="${SETTINGS_PATH}/${CONFIG_FILE_NAME}"


echo RAW_FILE_PATH=${RAW_FILE_PATH}
echo SETTINGS_PATH=${SETTINGS_PATH}
echo OUTPUT_PATH=${OUTPUT_PATH}
echo CONFIG_FILE_PATH=${CONFIG_FILE_PATH}
echo CONDA_ENV=${CONDA_ENV}

echo INPUT INFORMATION ">>>>>>"
if [ -d "$RAW_FILE_PATH" ]; then
  # RAW_FILE_PATH is a directory
  du -s ${RAW_FILE_PATH}/*
  find ${RAW_FILE_PATH} -type f -exec md5sum {} +
  stat ${RAW_FILE_PATH}/*
else
  # RAW_FILE_PATH is a file
  du -s ${RAW_FILE_PATH}
  md5sum ${RAW_FILE_PATH}
  stat ${RAW_FILE_PATH}
fi
echo CONFIG_FILE:
echo size and md5sum: $(du -s ${CONFIG_FILE_PATH}) $(md5sum ${CONFIG_FILE_PATH})
cat ${CONFIG_FILE_PATH}
echo "<<<<<<"

# here we assume that at least one of these is set
SPECLIB_COMMAND=""
FASTA_COMMAND=""
if [ -n "$FASTA_FILE_NAME" ]; then
  FASTA_FILE_PATH="${SETTINGS_PATH}/${FASTA_FILE_NAME}"
  echo FASTA_FILE_PATH=${FASTA_FILE_PATH}
  echo FASTA_FILE size and md5sum: $(du -s ${FASTA_FILE_PATH}) $(md5sum ${FASTA_FILE_PATH})
  FASTA_COMMAND="--fasta ${FASTA_FILE_PATH}"
fi
if [ -n "$SPECLIB_FILE_NAME" ]; then
  SPECLIB_FILE_PATH="${SETTINGS_PATH}/${SPECLIB_FILE_NAME}"
  echo SPECLIB_FILE_PATH=${SPECLIB_FILE_PATH}
  echo SPECLIB size and md5sum: $(du -s ${SPECLIB_FILE_PATH}) $(md5sum ${SPECLIB_FILE_PATH})
  SPECLIB_COMMAND="--library ${SPECLIB_FILE_PATH}"
fi

echo CONDA_ENV ">>>>>>"
conda info
conda run -n $CONDA_ENV pip freeze
echo "<<<<<<"

echo MONO_VERSION ">>>>>>"
conda run -n $CONDA_ENV mono --version
echo "<<<<<<"

echo Creating output path ..
mkdir -p ${OUTPUT_PATH}
cd ${OUTPUT_PATH}

# output directory could already exists at this stage of overwrite flag it set
echo OUTPUT_PATH ">>>>>>"
set +e
du -s ${OUTPUT_PATH}/*
md5sum ${OUTPUT_PATH}/*
stat ${OUTPUT_PATH}/*
set -e
echo "<<<<<<"

echo "Running alphadia.."
echo "Check the logs in ${OUTPUT_PATH}/log.txt"

set +e
conda run -n $CONDA_ENV alphadia \
    --file "${RAW_FILE_PATH}" \
    ${SPECLIB_COMMAND} \
    ${FASTA_COMMAND} \
    --config "${CONFIG_FILE_PATH}" \
    --output "${OUTPUT_PATH}"
alphadia_exit_code=$?  # this line must immediately follow the `conda run ..` command
set -e

echo OUTPUT_PATH ">>>>>>"
set +e
du -s ${OUTPUT_PATH}/*
md5sum ${OUTPUT_PATH}/*
stat ${OUTPUT_PATH}/*
set -e
echo "<<<<<<"

echo ALPHADIA EXIT CODE ">>>>>>"
echo $alphadia_exit_code
echo "<<<<<<"

if [ ! "$alphadia_exit_code" -eq 0 ]; then
    echo got nonzero exit code $alphadia_exit_code
    exit $alphadia_exit_code
fi
