#!/bin/bash

# A thin wrapper around docker compose

# example:
# ./compose.sh [compose-commands]

if [ "$ENV" == "" ]; then
  echo "Please set the ENV variable, e.g. 'export ENV=local' (or 'export ENV=sandbox', 'export ENV=production')"
  exit 1
fi

sudo docker compose --env-file=envs/.env-airflow --env-file=envs/${ENV}.env "$@"
