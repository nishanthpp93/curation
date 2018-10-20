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

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
gcloud config set project "${APP_ID}"

virtualenv -p $(which python2.7) notebooks_env
notebooks_env/bin/pip install -U pip
source notebooks_env/bin/activate
pip install -r requirements.txt
jupyter notebook --config=jupyter_notebook_config.py
