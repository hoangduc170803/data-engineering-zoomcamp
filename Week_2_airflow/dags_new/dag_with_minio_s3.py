from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_arg = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
    }

with DAG(
    dag_id = "dag_with_minio_s3",
    start_date = datetime(2025, 2, 4),
    default_args = default_arg,
    schedule_interval = "@daily"
) as dag:
    task_1 = S3KeySensor(
        task_id = "sensor_minio_s3",
        bucket_name = 'airflow',
        bucket_key = 'yellow_tripdata_2021-01.csv',
        aws_conn_id = 'minio_s3_conn'
    )