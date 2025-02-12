import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retries_delay': timedelta(minutes=5)   
}

def postgres_to_s3():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM yellow_taxi_2021_03 LIMIT 100;")
    
    file_path = "D:/DataZoomCamp/airflow/dags_new/yellow_taxi_data.txt"

    
    try:
        with open(file_path, "w") as f:
            column_names = [desc[0] for desc in cursor.description]
            f.write("\t".join(column_names) + "\n")
            
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    f.write("\t".join(str(value) for value in row) + "\n")
            else:
                logging.warning("No data found in the query result.")
            
            logging.info(f"Data has been saved to {file_path}")
    
    except IOError as e:
        logging.error(f"IOError occurred while writing to {file_path}: {str(e)}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id="dag_with_postgres_hook",
    default_args=default_args,
    start_date=datetime(2025, 2, 4),
    schedule_interval="@daily"
) as dag:
    task_1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )