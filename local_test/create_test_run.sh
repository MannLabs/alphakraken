#!/bin/bash

# This script creates a test run for the Airflow test folders.
# It creates a new raw file in the instruments folder and fakes the acquistion and metrics generation.
# The script is meant to be run from the root of the repository.
# Parameters:
#   $1: name of instrument (test1->thermo, test2->bruker, test3->sciex)


set -e -u

INSTRUMENT=${1:-test1}

BASE_FOLDER=local_test

# one-time creation of folder structure
mkdir -p ${BASE_FOLDER}/instruments/$INSTRUMENT/Backup
mkdir -p ${BASE_FOLDER}/backup/$INSTRUMENT
mkdir -p ${BASE_FOLDER}/settings/config ${BASE_FOLDER}/settings/fasta ${BASE_FOLDER}/settings/speclib
mkdir -p ${BASE_FOLDER}/output

FILE_PREFIX=test_file_SA_P123_${INSTRUMENT}_
I=1

# TYPE = thermo
if [ "$INSTRUMENT" = "test1" ]; then

  RAW_FILE_NAME=${FILE_PREFIX}${I}.raw
  while [ -f ${BASE_FOLDER}/instruments/$INSTRUMENT/${RAW_FILE_NAME} ]; do
    I=$((I+1))
    RAW_FILE_NAME=${FILE_PREFIX}${I}.raw
  done

  echo $RAW_FILE_NAME
  echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/$RAW_FILE_NAME

# TYPE = bruker
elif [ "$INSTRUMENT" = "test2" ]; then

    RAW_FILE_NAME=${FILE_PREFIX}${I}.d
    while [ -d ${BASE_FOLDER}/instruments/$INSTRUMENT/${RAW_FILE_NAME} ]; do
      I=$((I+1))
      RAW_FILE_NAME=${FILE_PREFIX}${I}.d
    done

    echo $RAW_FILE_NAME
    mkdir -p ${BASE_FOLDER}/instruments/$INSTRUMENT/$RAW_FILE_NAME/1234.m
    echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/$RAW_FILE_NAME/analysis.tdf_bin
    echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/$RAW_FILE_NAME/analysis.tdf
    echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/$RAW_FILE_NAME/1234.m/some_file.txt

# TYPE = sciex
elif [ "$INSTRUMENT" = "test3" ]; then

  RAW_FILE_STEM=${FILE_PREFIX}${I};
  RAW_FILE_NAME=${RAW_FILE_STEM}.wiff
  while [ -f ${BASE_FOLDER}/instruments/$INSTRUMENT/${RAW_FILE_NAME} ]; do
    I=$((I+1))
    RAW_FILE_STEM=${FILE_PREFIX}${I};
    RAW_FILE_NAME=${RAW_FILE_STEM}.wiff
  done

  echo $RAW_FILE_NAME
  echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/$RAW_FILE_NAME
  echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/${RAW_FILE_STEM}.wiff2
  echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/${RAW_FILE_STEM}.wiff.scan
  echo some_raw_data > ${BASE_FOLDER}/instruments/$INSTRUMENT/${RAW_FILE_STEM}.timeseries.data

else
  echo "Unknown type $TYPE"
  exit 1
fi

echo Faking acquistion of $RAW_FILE_NAME .. done.
echo To speed up things, use the Airflow UI to mark the "monitor_acquistion" task in the "acquistion_handler" DAG as "success".


echo Faking metrics generation of $RAW_FILE_NAME ..

#YEAR_MONTH=$(date +%Y_%m)
NEW_OUTPUT_FOLDER=${BASE_FOLDER}/output/P123/out_$RAW_FILE_NAME
mkdir -p $NEW_OUTPUT_FOLDER
cp ${BASE_FOLDER}/_data/* $NEW_OUTPUT_FOLDER


echo .. done
