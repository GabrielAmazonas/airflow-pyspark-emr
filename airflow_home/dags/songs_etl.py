# NOTE: Importing from aws_utils - custom code folder inside plugins
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from aws_utils.emr_operations import create_emr_cluster, wait_emr_job
from aws_utils.iam_operations import create_iam_role
from aws_utils.s3_operations import check_output_quality, upload_code, create_s3_buckets

# Creates the songs_etl dag, to be executed daily.
dag = DAG(dag_id='songs_etl', description='Simple tutorial DAG',
          schedule_interval='@daily',
          start_date=datetime(2017, 3, 20), catchup=False)

# Usage of PythonOperators with the imported custom code from plugins
create_emr_role = PythonOperator(task_id='create_emr_role',
                                 python_callable=create_iam_role,
                                 dag=dag
                                 )

create_buckets = PythonOperator(task_id='create_buckets',
                                python_callable=create_s3_buckets,
                                dag=dag
                                )

upload_code = PythonOperator(task_id='upload_code',
                             python_callable=upload_code,
                             dag=dag
                             )

run_on_emr = PythonOperator(task_id='run_on_emr',
                            python_callable=create_emr_cluster,
                            dag=dag)

check_songs_quality = PythonOperator(task_id='check_songs_quality',
                                     python_callable=check_output_quality,
                                     dag=dag)

wait_emr = PythonOperator(task_id='wait_emr_job',
                          python_callable=wait_emr_job,
                          dag=dag)

# Definition of the DAG execution order
create_emr_role
create_buckets
create_buckets >> upload_code
run_on_emr << upload_code
run_on_emr << create_emr_role
run_on_emr >> wait_emr >> check_songs_quality
