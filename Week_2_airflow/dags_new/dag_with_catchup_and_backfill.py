from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = "dag_with_catchup_and_backfill",
    default_args = default_args,
    start_date = datetime(2021, 1, 1),
    schedule_interval = "@daily",
    catchup = False
    ) as dag:
    task_1 = BashOperator(
        task_id = "task1",
        bash_command = "echo This is a simple bash command!",
    )
    
    
    
