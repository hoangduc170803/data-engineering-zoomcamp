from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd

PROJECT_ID = "terraform-demo-447612"
BUCKET = "nyc_taxi_data__parquet_bucket"
BIGQUERY_DATASET = "taxi_data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

fhv_url_template = 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\')}}.parquet'
fhv_parquet_file = 'output_fhv_trip_{{ execution_date.strftime(\'%Y-%m\')}}.parquet'
fhv_table_name_template ='output_fhv_trip{{ execution_date.strftime(\'%Y_%m\')}}'
LOCAL_FILE_PATH = f"{path_to_local_home}/{fhv_parquet_file}"
def download_fhv(fhv_parquet_file, url):
    response = requests.get(url)
    if response.status_code == 200:
        with open(fhv_parquet_file, 'wb') as f_out:
            f_out.write(response.content)
            return True
    else:
        print(f"Error downloading file: {response.status_code}")
        return False    
    

def processing_fhv_data(fhv_parquet_file):
    # Đọc file parquet đã download
    df = pd.read_parquet(fhv_parquet_file)
    
    # Chuyển đổi kiểu dữ liệu
    df['dispatching_base_num'] = df['dispatching_base_num'].astype('string')
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    df['PUlocationID'] = df['PUlocationID'].astype('float64')
    df['DOlocationID'] = df['DOlocationID'].astype('float64')
    df['SR_Flag'] = df['SR_Flag'].astype('float64')
    df['Affiliated_base_number'] = df['Affiliated_base_number'].astype('string')
    
    # Ghi dữ liệu đã xử lý trở lại file Parquet (có thể ghi đè file ban đầu)
    df.to_parquet(fhv_parquet_file, index=False)
    print(f"Processed data saved to {fhv_parquet_file}")
    return fhv_parquet_file



def upload_to_gcs(bucket, local_file, object_name, gcp_conn_id="gcp-airflow"):
    """
    Upload file từ local lên Google Cloud Storage.
    """
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        filename=local_file,
        object_name=object_name
    )
    print(f"Uploaded {local_file} to gs://{bucket}/{object_name}")
    
def delete_local_file(local_file):
    """
    Xóa file ở local sau khi đã upload thành công.
    """
    if os.path.exists(local_file):
        os.remove(local_file)
        print(f"Deleted local file {local_file}")
    else:
        print(f"File {local_file} does not exist")


    
fhv_taxi_data_dag = DAG(
    dag_id = 'ingest_fhv',
    default_args=default_args,
    description='Ingest FHV taxi data to GCP',
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 31),
    schedule_interval='@monthly',
    catchup=True,
    dagrun_timeout=timedelta(minutes=10)
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

processing_fhv_data_task = PythonOperator(
    task_id='processing_fhv_data',
    python_callable=processing_fhv_data,
    op_kwargs={
        'fhv_parquet_file':fhv_parquet_file
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

# Task xóa file ở local
delete_file_task = PythonOperator(
    task_id='delete_local_file',
    python_callable=delete_local_file,
    op_kwargs={
        'local_file': LOCAL_FILE_PATH
    },
    dag=fhv_taxi_data_dag
)


download_fhv_task >> processing_fhv_data_task >> upload_fhv_to_gcs_task >> delete_file_task
