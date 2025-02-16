from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Các thông số cấu hình
PROJECT_ID = "terraform-demo-447612"
BUCKET = "yellow_ny_taxi_2024"
BIGQUERY_DATASET = "taxi_data"

# Đường dẫn lưu file trên local (đồng nhất cho download, upload và delete)
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
# File được đặt tên với Jinja templating để có thể thay đổi theo execution_date
PARQUET_FILE = 'output_yellow_taxi_{{ execution_date.strftime("%Y-%m") }}.parquet'
# Đường dẫn tuyệt đối tới file sẽ được sử dụng ở các task sau
LOCAL_FILE_PATH = f"{PATH_TO_LOCAL_HOME}/{PARQUET_FILE}"

# URL download file, sử dụng Jinja để thay thế execution_date
URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime("%Y-%m") }}.parquet'

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='Ingest_yellow_taxi_2024',
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
    schedule_interval="0 6 2 * *",
    catchup=True,
    dagrun_timeout=timedelta(minutes=10)

)

def download_yellow_taxi_data(url, parquet_file):
    """
    Download file từ URL và lưu tại đường dẫn tuyệt đối dựa trên AIRFLOW_HOME.
    """
    # Kết hợp đường dẫn lưu file: PATH_TO_LOCAL_HOME + tên file (đã được template)
    local_file_path = os.path.join(PATH_TO_LOCAL_HOME, parquet_file)
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_file_path, 'wb') as f_out:
            f_out.write(response.content)
        print(f"Downloaded file to {local_file_path}")
        return True
    else:
        print(f"Error downloading file: {response.status_code}")
        return False

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

# Task download file
download_task = PythonOperator(
    task_id='download_yellow_taxi',
    python_callable=download_yellow_taxi_data,
    op_kwargs={
        'url': URL_TEMPLATE,
        'parquet_file': PARQUET_FILE  # Đây là tên file theo template, sẽ được Airflow render
    },
    dag=dag
)

upload_data_to_gcs = PythonOperator(
    task_id='upload_to_gcp',
    python_callable=upload_to_gcs,
    op_kwargs={
        'bucket': BUCKET,
        'object_name': f"raw/{PARQUET_FILE}",  # Sử dụng template cho tên file trong bucket
        'local_file': f"{PATH_TO_LOCAL_HOME}/output_yellow_taxi_{{{{ execution_date.strftime('%Y-%m') }}}}.parquet",
        'gcp_conn_id': 'gcp-airflow'
    },
    dag=dag
)

# Task xóa file ở local
delete_file_task = PythonOperator(
    task_id='delete_local_file',
    python_callable=delete_local_file,
    op_kwargs={
        'local_file': LOCAL_FILE_PATH
    },
    dag=dag
)

# Thiết lập thứ tự thực thi: download -> upload -> delete
download_task >> upload_data_to_gcs >> delete_file_task
