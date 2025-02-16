PROJECT_ID = "terraform-demo-447612"
BUCKET = "nyc_taxi_data__parquet_bucket"
BIGQUERY_DATASET = "taxi_data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")



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
    

# Task xóa file ở local
delete_file_task = PythonOperator(
    task_id='delete_local_file',
    python_callable=delete_local_file,
    op_kwargs={
        'local_file': LOCAL_FILE_PATH
    },
    dag=dag
)

def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        filename=local_file,
        object_name=object_name
    )


    
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


download_fhv_task >> upload_fhv_to_gcs_task 
