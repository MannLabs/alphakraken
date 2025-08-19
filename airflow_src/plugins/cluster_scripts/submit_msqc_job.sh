#!/usr/bin/env bash
#SBATCH --job-name=msqc-alphakraken
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=16G
#SBATCH --time=00:10:00
#SBATCH --partition=p.<node>
####SBATCH --nodelist=<node>02,<node>03
####SBATCH --nice=9001
####SBATCH --cpus-per-task=8  # set by calling command

set -u -e

# cluster-specific code:
module purge
module load anaconda/3/2023.03

echo RAW_FILE_PATH=${RAW_FILE_PATH}
echo OUTPUT_PATH=${OUTPUT_PATH}

echo Creating output path ..
mkdir -p ${OUTPUT_PATH}
cd ${OUTPUT_PATH}

# TODO dynamic
CONDA_ENV=msqc-extractor
SOFTWARE_DIR=/fs/home/kraken-read/software

set +e
conda run -n $CONDA_ENV python SOFTWARE_DIR/msqc-extractor/main.py \
    "${RAW_FILE_PATH}" "${OUTPUT_PATH}"
software_exit_code=$?  # this line must immediately follow the `conda run ..` command
set -e

echo SOFTWARE EXIT CODE ">>>>>>"
echo $software_exit_code
echo "<<<<<<"

if [ ! "$software_exit_code" -eq 0 ]; then
    echo got nonzero exit code $software_exit_code
    exit $software_exit_code
fi
