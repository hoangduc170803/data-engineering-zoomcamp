import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from  google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pandas as pd
import pyarrow.csv 
import pyarrow.parquet
import requests
import gzip
import shutil

PROJECT_ID = "terraform-demo-447612"
BUCKET = "nyc_taxi_data__parquet_bucket"
BIGQUERY_DATASET = "taxi_data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def download_and_unzip(csv_name_gz, csv_name, url):
    response = requests.get(url)
    if response.status_code == 200:
        with open(csv_name_gz, 'wb') as f_out:
            f_out.write(response.content)
    else:
        print(f"Error downloading file: {response.status_code}")
        return False        
    with gzip.open(csv_name_gz, 'rb') as f_in:
        with open(csv_name,'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return True

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pyarrow.csv.read_csv(src_file)
    pyarrow.parquet.write_table(table, src_file.replace('.csv','.parquet'))

def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        filename=local_file,
        object_name=object_name
    )


default_args ={
    'owner': 'airflow',
    'retries': 5,
    'retries_delay': timedelta(minutes=5),
    'max_active_runs':3,
    'catchup':True
}

dag = DAG(
    dag_id = 'ingest_data_to_gcp',
    default_args=default_args,
    description='Ingest data to GCP',
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
    schedule_interval='@monthly',
    catchup=True
) 

table_name_template ='yellow_taxi{{ execution_date.strftime(\'%Y_%m\')}}'
csv_name_gz_template = 'output_yellow_taxi{{ execution_date.strftime(\'%Y-%m\')}}.csv.gz'
csv_name_template='output_yellow_taxi{{ execution_date.strftime(\'%Y-%m\')}}.csv'
parquet_file = csv_name_template.replace('.csv','.parquet')
url_template = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"


download_task = PythonOperator(
    task_id='download_and_unzip',
    python_callable = download_and_unzip,
    op_kwargs={
        'csv_name_gz':csv_name_gz_template,
        'csv_name':csv_name_template,
        'url':url_template
    },
    dag=dag
)

process_task = PythonOperator(
    
    task_id='format_to_parquet',
    python_callable=format_to_parquet,
    op_kwargs={
        'src_file':csv_name_template
    },
    dag=dag
)


local_to_gcs_task = PythonOperator(
    task_id='local_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={
        'bucket':BUCKET,
        'object_name':f'raw/{parquet_file}',
        'local_file':f'{path_to_local_home}/{parquet_file}',
        'gcp_conn_id':'gcp-airflow'
    },
    dag=dag
)    

bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    task_id='bigquery_external_table_task',
    # Đây là cấu hình chi tiết của bảng BigQuery:
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": table_name_template,
        },
        "externalDataConfiguration" : {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"]
        }
    },
    gcp_conn_id="gcp-airflow",
    dag=dag
)
download_task >> process_task >> local_to_gcs_task >> bigquery_external_table_task

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

fhv_url_template = 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\')}}.parquet'
fhv_parquet_file = 'output_fhv_trip_{{ execution_date.strftime(\'%Y-%m\')}}.parquet'
fhv_table_name_template ='output_fhv_trip{{ execution_date.strftime(\'%Y_%m\')}}'
def download_fhv(fhv_parquet_file, url):
    response = requests.get(url)
    if response.status_code == 200:
        with open(fhv_parquet_file, 'wb') as f_out:
            f_out.write(response.content)
            return True
    else:
        print(f"Error downloading file: {response.status_code}")
        return False    
    
    
fhv_taxi_data_dag = DAG(
    dag_id = 'ingest_fhv_taxi_data_to_gcp',
    default_args=default_args,
    description='Ingest FHV taxi data to GCP',
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 31),
    schedule_interval='@monthly',
    catchup=True
)        

download_fhv_task = PythonOperator(
    task_id='download_fhv',
    python_callable=download_fhv,
    op_kwargs={
        'fhv_parquet_file':fhv_parquet_file,
        'url':fhv_url_template
    },
    dag=fhv_taxi_data_dag
)

upload_fhv_to_gcs_task = PythonOperator(
    task_id='upload_fhv_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={
         'bucket':BUCKET,
        'object_name':f'raw/{fhv_parquet_file}',
        'local_file':f'{path_to_local_home}/{fhv_parquet_file}',
        'gcp_conn_id':'gcp-airflow'
    },
    dag=fhv_taxi_data_dag
)

fhv_bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    task_id='fhv_bigquery_external_table_task',
    # Đây là cấu hình chi tiết của bảng BigQuery:
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": fhv_table_name_template,
        },
        "externalDataConfiguration" : {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{BUCKET}/raw/{fhv_parquet_file}"]
        }
    },
    gcp_conn_id="gcp-airflow",
    dag=fhv_taxi_data_dag
)
download_fhv_task >> upload_fhv_to_gcs_task >> fhv_bigquery_external_table_task

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

zone_lookup_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'  
zone_csv_name_template='output_zone_lookup_{{ execution_date.strftime(\'%Y-%m\')}}.csv'   
zone_parquet_file = zone_csv_name_template.replace('.csv','.parquet')
zone_table_name_template ='output_zone_lookup'

def download_zone(zone_csv_name_template, zone_lookup_url):
    response = requests.get(zone_lookup_url)
    if response.status_code == 200:
        with open(zone_csv_name_template, 'wb') as f_out:
            f_out.write(response.content)
            return True
    else:
        print(f"Error downloading file: {response.status_code}")
        return False        

zone_taxi_data_dag = DAG(
    dag_id = 'ingest_zone_lookup_to_gcp',
    default_args=default_args,
    description='Ingest FHV taxi data to GCP',
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=True
)       

download_zone_task = PythonOperator(
    task_id='download_zone',
    python_callable=download_zone,
    op_kwargs={
        'zone_csv_name_template':zone_csv_name_template,
        'zone_lookup_url':zone_lookup_url
    },
    dag=zone_taxi_data_dag
)

zone_process_task = PythonOperator(
    
    task_id='format_to_parquet',
    python_callable=format_to_parquet,
    op_kwargs={
        'src_file':zone_csv_name_template
    },
    dag=zone_taxi_data_dag
)

upload_zone_to_gcs_task= PythonOperator(
    task_id='upload_zone_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={
        'bucket':BUCKET,
        'object_name':f'raw/{zone_parquet_file}',
        'local_file':f'{path_to_local_home}/{zone_parquet_file}',
        'gcp_conn_id':'gcp-airflow'
    },
    dag=zone_taxi_data_dag
)

zone_bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    task_id='zone_bigquery_external_table_task',
    # Đây là cấu hình chi tiết của bảng BigQuery:
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": zone_table_name_template,
        },
        "externalDataConfiguration" : {
            "sourceFormat": "PARQUET",
            "sourceUris": [f"gs://{BUCKET}/raw/{zone_parquet_file}"]
        }
    },
    gcp_conn_id="gcp-airflow",
    dag=zone_taxi_data_dag
)

download_zone_task >> zone_process_task >> upload_zone_to_gcs_task >> zone_bigquery_external_table_task