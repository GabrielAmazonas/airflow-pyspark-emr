#!/usr/bin/env bash

echo "$AIRFLOW_HOME"
airflow initdb;
airflow scheduler &
airflow webserver;