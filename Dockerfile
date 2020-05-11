##Remember to change your dl.config using the config script before build this image.
FROM ubuntu:20.04
SHELL [ "/bin/bash", "-c"]

RUN apt-get update
RUN apt-get install -y python3 python3-dev python3-pip python3-venv

ENV AIRFLOW_HOME=/home/airflow-pyspark-emr/airflow_home
ENV AWS_ACCESS_KEY_ID=
ENV AWS_SECRET_ACCESS_KEY=

COPY . /home/airflow-pyspark-emr

WORKDIR /home/airflow-pyspark-emr

RUN pip3 install -r requirements.txt

EXPOSE 8080

RUN ["chmod", "+x", "/home/airflow-pyspark-emr/start.sh"]

ENTRYPOINT ["/home/airflow-pyspark-emr/start.sh"]