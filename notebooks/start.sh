#!/usr/bin/env bash

APP_ID='aou-res-curation-test'
KEY_FILE=''

USAGE="start.sh --key_file <Path to service account key> [--app_id <Application ID. Defaults to ${APP_ID}.>]"

while true; do
  case "$1" in
    --key_file) KEY_FILE=$2; shift 2;;
    --app_id) APP_ID=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done
if [ -z "${KEY_FILE}" ]
then
  echo "Specify key file location. Usage: $USAGE"
  exit 1
fi

BIN_PATH='notebooks_env/bin'
if test -d notebooks_env/Scripts
then
    BIN_PATH='notebooks_env/Scripts'
fi

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
export APPLICATION_ID="${APP_ID}"
export PROJECT_ID="${APP_ID}"
gcloud config set project "${APP_ID}"
pybin="$(which python2.7 | which python)"

virtualenv -p ${pybin} notebooks_env
${BIN_PATH}/pip install -U pip
source ${BIN_PATH}/activate
pip install -r requirements.txt
jupyter notebook --config=jupyter_notebook_config.py
