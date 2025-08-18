#!/usr/bin/env bash
#SBATCH --job-name=alphakraken
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=62G
#SBATCH --time=02:00:00
#SBATCH --partition=p.<node>
####SBATCH --nodelist=<node>02,<node>03
####SBATCH --nice=9001

NTHREADS=8

set -u -e

# cluster-specific code:
module purge
module load anaconda/3/2023.03

export TQDM_MININTERVAL=10  # avoid lots of tqdm outputs

# INPUT taken from environment variables:
# RAW_FILE_PATH # e.g. "/fs/pool/pool-backup/Test2/2024_07/20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# SETTINGS_PATH # e.g. "/fs/pool/pool-alphakraken/settings/PID123"
# OUTPUT_PATH # e.g. "/fs/pool/pool-alphakraken/output/PID123/out_20240606_OA1_Evo12_16min_JBMR_ADIAMA_HeLa_5ng_F-40_01.raw"
# SPECLIB_FILE_NAME # e.g."hela_hybrid.small.hdf"
# FASTA_FILE_NAME # e.g. 2024_01_12_human.fasta
# CONFIG_FILE_NAME #e.g. "config.yaml"
# SOFTWARE # e.g. alphadia-1.6.2
# SOFTWARE_TYPE # e.g. "alphadia" or "custom"
# CUSTOM_COMMAND # e.g. "/fs/home/alphakraken/software/diann1.8.1 --qvalue 0.01 --f /path/to/file.raw ..."

# these are determined by convention:
CONDA_ENV=$SOFTWARE

CONFIG_FILE_PATH="${SETTINGS_PATH}/${CONFIG_FILE_NAME}"


echo RAW_FILE_PATH=${RAW_FILE_PATH}
echo SETTINGS_PATH=${SETTINGS_PATH}
echo OUTPUT_PATH=${OUTPUT_PATH}
echo SOFTWARE_TYPE=${SOFTWARE_TYPE}
echo CONFIG_FILE_PATH=${CONFIG_FILE_PATH}
echo CONDA_ENV=${CONDA_ENV}
echo CUSTOM_COMMAND=${CUSTOM_COMMAND}

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

echo "<<<<<<"


echo Creating output path ..
mkdir -p ${OUTPUT_PATH}
cd ${OUTPUT_PATH}

# output directory could already exists at this stage if 'output_exists_mode' variable it set
echo OUTPUT_PATH ">>>>>>"
set +e
du -s ${OUTPUT_PATH}/*
md5sum ${OUTPUT_PATH}/*
stat ${OUTPUT_PATH}/*
set -e
echo "<<<<<<"

##################### SCIEX HACK pt. 1 ###############
# Sciex files cannot be opened in read-only mode, so we create
# a temporary directory in the output folder and copy the files there.
# After quanting, this temporary directory is removed (see pt. 2)

strip_all_extensions() {
    # strip all extensions off a path, e.g. "/fs/pool/filename.ext1.ext2" -> "/fs/pool/filename"
    local path="$1"
    while [[ "$path" == *.* ]]; do
        path="${path%.*}"
    done
    echo "$path"
}

if [[ ${RAW_FILE_PATH} == *.wiff ]]; then
  echo copying over raw files to tmp_raw_data ..
  mkdir -p "${OUTPUT_PATH}/tmp_raw_data"

  stripped_path=$(strip_all_extensions "$RAW_FILE_PATH")
  cp "${stripped_path}".wiff "${OUTPUT_PATH}/tmp_raw_data"
  cp "${stripped_path}".wiff2 "${OUTPUT_PATH}/tmp_raw_data"
  cp "${stripped_path}".wiff.scan "${OUTPUT_PATH}/tmp_raw_data"
  echo .. copying done

  raw_file_name_stem=$(basename $stripped_path)
  RAW_FILE_PATH="${OUTPUT_PATH}/tmp_raw_data/${raw_file_name_stem}.wiff"
  echo updated RAW_FILE_PATH=${RAW_FILE_PATH}

  du -s "${OUTPUT_PATH}/tmp_raw_data/"*
  md5sum "${OUTPUT_PATH}/tmp_raw_data/"*
  stat "${OUTPUT_PATH}/tmp_raw_data/"*

fi
##################### SCIEX HACK END #####################

if [ "$SOFTWARE_TYPE" = "alphadia" ]; then

    echo CONFIG_FILE:
    echo size and md5sum: $(du -s ${CONFIG_FILE_PATH}) $(md5sum ${CONFIG_FILE_PATH})
    cat ${CONFIG_FILE_PATH}

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

    echo "Running alphadia.."
    echo "Check the logs in ${OUTPUT_PATH}/log.txt"

    set +e
    conda run -n $CONDA_ENV alphadia \
        --file "${RAW_FILE_PATH}" \
        ${SPECLIB_COMMAND} \
        ${FASTA_COMMAND} \
        --config "${CONFIG_FILE_PATH}" \
        --output "${OUTPUT_PATH}" \
        --config-dict "{\"general\": {\"thread_count\": $NTHREADS}}"
    software_exit_code=$?  # this line must immediately follow the `conda run ..` command
    set -e
else
    echo "Running custom software.."
    echo "Command: ${CUSTOM_COMMAND}"
    echo "Check the logs in ${OUTPUT_PATH}/log.txt"

    set +e
    ${CUSTOM_COMMAND} > ${OUTPUT_PATH}/log.txt 2>&1
    software_exit_code=$?  # this line must immediately follow the command
    set -e
fi

echo OUTPUT_PATH ">>>>>>"
set +e
du -s ${OUTPUT_PATH}/*
md5sum ${OUTPUT_PATH}/*
stat ${OUTPUT_PATH}/*
set -e
echo "<<<<<<"

echo SOFTWARE EXIT CODE ">>>>>>"
echo $software_exit_code
echo "<<<<<<"


##################### SCIEX HACK pt. 2 ###############
# remove very defensively (file by file, then empty dir)
if [[ ${RAW_FILE_PATH} == *.wiff ]]; then
  echo removing tmp_raw_data ..
  ls -l "${OUTPUT_PATH}/tmp_raw_data/"*
  rm "${OUTPUT_PATH}/tmp_raw_data/${raw_file_name_stem}.wiff"
  rm "${OUTPUT_PATH}/tmp_raw_data/${raw_file_name_stem}.wiff2"
  rm "${OUTPUT_PATH}/tmp_raw_data/${raw_file_name_stem}.wiff.scan"
  echo .. done removing
fi
##################### SCIEX HACK END #####################

if [ ! "$software_exit_code" -eq 0 ]; then
    echo got nonzero exit code $software_exit_code
    exit $software_exit_code
fi
