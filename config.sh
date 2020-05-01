#!/usr/bin/env bash
BASEDIR=$(dirname $0)

RED='\033[0;31m'
GREEN='\033[0;32m'
LIGHT_CYAN='\033[0;96m'
BLUE='\033[0;34m'
NOCOLOR='\033[0m'

function print_green() {
    echo -e "${GREEN}$1${NOCOLOR}"
}

function print_blue() {
    echo -e "${BLUE}$1${NOCOLOR}"
}

function print_error() {
    echo -e "${RED}$1${NOCOLOR}"
}

function print_cyan() {
    echo -e "${LIGHT_CYAN}$1${NOCOLOR}"
}


print_cyan "   _    _        __  _                ___       _                   "
print_cyan "  /_\  (_) _ _  / _|| | ___ __ __ __ / __| ___ | |_  _  _  _ __     "
print_cyan " / _ \ | || '_||  _|| |/ _ \\ V  V / \__ \/ -_)|  _|| || || '_ \    "
print_cyan "/_/ \_\|_||_|  |_|  |_|\___/ \_/\_/  |___/\___| \__| \_,_|| .__/    "
print_cyan "                                                          |_|      "

read -r -p "$(print_cyan 'Your AWS_ACCESS_KEY_ID:') " AWS_ACCESS_KEY_ID
read -r -p "$(print_cyan 'Your AWS_SECRET_ACCESS_KEY:') " AWS_SECRET_ACCESS_KEY
read -r -p "$(print_cyan 'Your CODE_BUCKET:') " CODE_BUCKET
read -r -p "$(print_cyan 'Your OUTPUT_BUCKET:') " OUTPUT_BUCKET
read -r -p "$(print_cyan 'Your LOG_BUCKET:') " LOG_BUCKET
read -r -p "$(print_cyan 'Your INPUT_DATA:') " INPUT_DATA
read -r -p "$(print_cyan 'Your OUTPUT_DATA:') " OUTPUT_DATA
read -r -p "$(print_cyan 'Your ROLE_NAME:') " ROLE_NAME


echo "
[AWS]
AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

[S3]
CODE_BUCKET=$CODE_BUCKET
OUTPUT_BUCKET=$OUTPUT_BUCKET
LOG_BUCKET=$LOG_BUCKET

[SPARK]
FILE_PATH=$BASEDIR/plugins/aws_utils/spark_jobs/etl.py

[DATALAKE]
INPUT_DATA=$INPUT_DATA
OUTPUT_DATA=$OUTPUT_DATA

[IAM]
ROLE_NAME=$ROLE_NAME
" > "$BASEDIR/airflow_home/dl.cfg"

print_green "File dl.cfg created in $BASEDIR/airflow_home/"

print_cyan "Creating AIRFLOW_HOME environment variable in ~/.bashrc"

echo "export AIRFLOW_HOME=$BASEDIR/airflow-pyspark-emr/airflow_home" >> ~/.bashrc