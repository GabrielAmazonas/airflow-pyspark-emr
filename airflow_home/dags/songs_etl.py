from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from aws_utils.emr_deployment import create_iam_role, create_s3_buckets, upload_code, create_emr_cluster


dag = DAG(dag_id='songs_etl', description='Simple tutorial DAG',
          schedule_interval='@daily',
          start_date=datetime(2017, 3, 20), catchup=False)

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

create_emr_role
create_buckets
create_buckets >> upload_code
run_on_emr << upload_code
run_on_emr << create_emr_role
