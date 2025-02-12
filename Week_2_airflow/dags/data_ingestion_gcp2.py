import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests


# This DAG is for homework of module 3
# Ingest data for green 2022 taxi 

# Make sure the values ​​match your terraform main.tf file
PROJECT_ID="zoomcamp-airflow-444903"
BUCKET="zoomcamp_datalake"
BIGQUERY_DATASET = "airflow2025"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


# Utility functions
def download(file, url):

    # Download  file
    response = requests.get(url)
    if response.status_code == 200:
        with open(file, 'wb') as f_out:
            f_out.write(response.content)
    else:
        print(f"Error downloading file: {response.status_code}")
        return False



def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file
    )


# Defining the DAG
dag = DAG(
    "GCP_ingestion_multiple_files",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 12, 5),
    catchup=True, 
    max_active_runs=1,
)

table_name_template = 'green_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
file_template = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.parquet'
consolidated_table_name = "green_2022"
url_template = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# Task 1: Download file
download_task = PythonOperator(
    task_id="download",
    python_callable=download,
    op_kwargs={
        'file': file_template,
        'url': url_template
    },
    dag=dag
)



# Task 2: Upload file to google storage
local_to_gcs_task = PythonOperator(
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
        "bucket": BUCKET,
        "object_name": f"raw/{file_template}",
        "local_file": f"{path_to_local_home}/{file_template}",
        "gcp_conn_id": "gcp-airflow"
    },
    dag=dag
)

# Task 3: Create final table
create_final_table_task = BigQueryInsertJobOperator(
    task_id="create_final_table_task",
    gcp_conn_id="gcp-airflow",
    configuration={
        "query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BIGQUERY_DATASET}.{consolidated_table_name}`
                (
                    unique_row_id BYTES,
                    filename STRING,      
                    VendorID INT64,
                    lpep_pickup_datetime TIMESTAMP,
                    lpep_dropoff_datetime TIMESTAMP,
                    store_and_fwd_flag STRING,
                    RatecodeID FLOAT64,
                    PULocationID INT64,
                    DOLocationID INT64,
                    passenger_count FLOAT64,
                    trip_distance FLOAT64,
                    fare_amount FLOAT64,
                    extra FLOAT64,
                    mta_tax FLOAT64,
                    tip_amount FLOAT64,
                    tolls_amount FLOAT64,
                    ehail_fee INT64,
                    improvement_surcharge FLOAT64,
                    total_amount FLOAT64,
                    payment_type FLOAT64,
                    trip_type FLOAT64,
                    congestion_surcharge FLOAT64
                )    
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

# Task 4: Create external monthly table
create_external_table_task = BigQueryInsertJobOperator(
    task_id="create_external_table_task",
    gcp_conn_id="gcp-airflow",
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name_template}_ext`
                (
                    VendorID INT64,
                    lpep_pickup_datetime TIMESTAMP,
                    lpep_dropoff_datetime TIMESTAMP,
                    store_and_fwd_flag STRING,
                    RatecodeID FLOAT64,
                    PULocationID INT64,
                    DOLocationID INT64,
                    passenger_count FLOAT64,
                    trip_distance FLOAT64,
                    fare_amount FLOAT64,
                    extra FLOAT64,
                    mta_tax FLOAT64,
                    tip_amount FLOAT64,
                    tolls_amount FLOAT64,
                    ehail_fee INT64,
                    improvement_surcharge FLOAT64,
                    total_amount FLOAT64,
                    payment_type FLOAT64,
                    trip_type FLOAT64,
                    congestion_surcharge FLOAT64
                )
                OPTIONS (
                    uris = ['gs://{BUCKET}/raw/{file_template}'],
                    format = 'PARQUET'
                );
            """,
            "useLegacySql": False,
        }
    },
    dag=dag
)

# Task 5: Create native monthly table
create_temp_table_task = BigQueryInsertJobOperator(
    task_id="create_temp_table_task",
    gcp_conn_id="gcp-airflow",
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name_template}_tmp`
                AS
                SELECT
                    MD5(CONCAT(
                        COALESCE(CAST(VendorID AS STRING), ""),
                        COALESCE(CAST(lpep_pickup_datetime AS STRING), ""),
                        COALESCE(CAST(lpep_dropoff_datetime AS STRING), ""),
                        COALESCE(CAST(PULocationID AS STRING), ""),
                        COALESCE(CAST(DOLocationID AS STRING), "")
                    )) AS unique_row_id,
                    "{file_template}" AS filename,
                    *
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name_template}_ext`;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)


# Task 6: Merge
merge_to_final_table_task = BigQueryInsertJobOperator(
    task_id="merge_to_final_table_task",
    gcp_conn_id="gcp-airflow",
    configuration={
        "query": {
            "query": f"""
                MERGE INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.{consolidated_table_name}` T
                USING `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name_template}_tmp` S
                ON T.unique_row_id = S.unique_row_id
                WHEN NOT MATCHED THEN
                    INSERT (unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge)
                    VALUES (S.unique_row_id, S.filename, S.VendorID, S.lpep_pickup_datetime, S.lpep_dropoff_datetime, S.store_and_fwd_flag, S.RatecodeID, S.PULocationID, S.DOLocationID, S.passenger_count, S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee, S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge);
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)


download_task >> local_to_gcs_task >> create_final_table_task >> create_external_table_task >> create_temp_table_task >> merge_to_final_table_task