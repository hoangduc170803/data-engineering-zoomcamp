from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': "airflow",
    'retries': 5,
    'retry_delay': timedelta(minutes=2),  
}

with DAG (
    dag_id="our_first_dag",
    default_args=default_args,
    description="Our first DAG that we write",
    schedule="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 3, 28)
) as dag:
    task_1 = BashOperator(
        task_id = "first_task",
        bash_command = "echo hello world, this is the first task!",
    )
    
    task_2 = BashOperator(
        task_id = "second_task",
        bash_command = "echo hey, I am task 2 and will be running after task 1"
    )
    
    task_3 = BashOperator(
        task_id = "third_task",
        bash_command = "echo hey, I am task 3 and will be running after task 2"
    )
    
    task_1 >> [task_2, task_3]
