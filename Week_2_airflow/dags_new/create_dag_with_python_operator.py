from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name',key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name',key='last_name')
    age = ti.xcom_pull(task_ids='get_age',key='age')
    print(f'Hello world! My name is {first_name} {last_name}, and I am {age} years old!')
    
def get_name(ti):
    ti.xcom_push(key='first_name', value='Duc')
    ti.xcom_push(key='last_name', value='Nham')    

def get_age(ti):{
    ti.xcom_push(key='age', value=22)
}
with DAG(
    dag_id='our_dag_with_python_operator_v01',  
    default_args=default_args,  
    description='Our DAG using PythonOperator',
    start_date=datetime(2022, 2, 2, 1),
    schedule='@daily'
) as dag:
    task_1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={'age': 22}
    )
    
    task_2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name        
    )
    
    task_3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    [task_2,task_3] >> task_1

