from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import requests
import gzip
import shutil

import psycopg2
import io



# This info is from docker-compose-lesson1.yaml
user = "root2"
password = "root2"
host = "pgdatabase"
port = "5432"
db = "ny_taxi"


taxi_type = "green"


def download_and_unzip(csv_name_gz, csv_name, url):

    # Download the CSV.GZ file
    response = requests.get(url)
    if response.status_code == 200:
        with open(csv_name_gz, 'wb') as f_out:
            f_out.write(response.content)
    else:
        print(f"Error downloading file: {response.status_code}")
        return False

    # Unzip the CSV file
    with gzip.open(csv_name_gz, 'rb') as f_in:
        with open(csv_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    
    return True


def process_and_insert_to_db_with_copy(csv_name, user, password, host, port, db, table_name):

    # Connection to the PostgreSQL
    conn = psycopg2.connect(
        dbname=db, user=user, password=password, host=host, port=port
    )

    # Creates a cursor object for executing SQL queries
    cursor = conn.cursor()

    # Create the specified table if it does not already exist.
    if taxi_type == "yellow":
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "VendorID" TEXT,
            "tpep_pickup_datetime" TIMESTAMP,
            "tpep_dropoff_datetime" TIMESTAMP,
            "passenger_count" REAL,
            "trip_distance" REAL,
            "RatecodeID" TEXT,
            "store_and_fwd_flag" TEXT,
            "PULocationID" TEXT,
            "DOLocationID" TEXT,
            "payment_type" REAL,
            "fare_amount" REAL,
            "extra" REAL,
            "mta_tax" REAL,
            "tip_amount" REAL,
            "tolls_amount" REAL,
            "improvement_surcharge" REAL,
            "total_amount" REAL,
            "congestion_surcharge" REAL
        );
        """
    elif taxi_type == "green":
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "VendorID"               TEXT,
            "lpep_pickup_datetime"   TIMESTAMP,
            "lpep_dropoff_datetime"  TIMESTAMP,
            "store_and_fwd_flag"      TEXT,
            "RatecodeID"             TEXT,
            "PULocationID"           TEXT,
            "DOLocationID"           TEXT,
            "passenger_count"        REAL,
            "trip_distance"          REAL,
            "fare_amount"            REAL,
            "extra"                  REAL,
            "mta_tax"                REAL,
            "tip_amount"             REAL,
            "tolls_amount"           REAL,
            "ehail_fee"              REAL,
            "improvement_surcharge"  REAL,
            "total_amount"           REAL,
            "payment_type"           REAL,
            "trip_type"              REAL,
            "congestion_surcharge"   REAL
        );
        """


    # Runs the query
    cursor.execute(create_table_query)
    # Commits the changes to the database, ensuring the table is created.
    conn.commit()
  
    # Read the CSV file in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=300000)
    for i, chunk in enumerate(df_iter):
        
        if taxi_type == "yellow":

            chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'])
            chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'])

        elif taxi_type == "green":

            chunk['lpep_pickup_datetime'] = pd.to_datetime(chunk['lpep_pickup_datetime'])
            chunk['lpep_dropoff_datetime'] = pd.to_datetime(chunk['lpep_dropoff_datetime'])            

        # Memory buffer is created using io.StringIO() to hold the chunk as a CSV
        buffer = io.StringIO()
        chunk.to_csv(buffer, index=False, header=False)
        # Moves the pointer to the beginning of the buffer to prepare it for reading
        buffer.seek(0)  

        # Insert the data from the buffer into the PostgreSQL table.
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV NULL ''", buffer)

        print(f"Inserted chunk {i + 1}")

        # Commits the changes to the database after each chunk is inserted.
        conn.commit()

    # After all chunks are processed, cursor and connection are closed
    cursor.close()
    conn.close()




# Defining the DAG
dag = DAG(
    f"{taxi_type}_taxi_local_ingestion",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 3, 28),
    catchup=True, # True means run past missed jobs
    max_active_runs=1,
)

table_name_template = f'{taxi_type}_taxi_{{{{ execution_date.strftime(\'%Y_%m\') }}}}'
csv_name_gz_template = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.csv.gz'
csv_name_template = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.csv'
url_template = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{{{{ execution_date.strftime(\'%Y-%m\') }}}}.csv.gz"

# Task 1
download_task = PythonOperator(
    task_id="download_and_unzip",
    python_callable=download_and_unzip,
    op_kwargs={
        'csv_name_gz': csv_name_gz_template,
        'csv_name': csv_name_template,
        'url': url_template
    },
    dag=dag
)

# Task 2:
process_task = PythonOperator(
    task_id="process_and_insert_to_db",
    python_callable=process_and_insert_to_db_with_copy,
    op_kwargs={
        'csv_name': csv_name_template,
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'db': db,
        'table_name': table_name_template
     
    },
    dag=dag
)

# Establish the sequence of tasks
download_task >> process_task