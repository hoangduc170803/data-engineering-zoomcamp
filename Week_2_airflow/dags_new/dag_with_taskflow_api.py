from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='our_dag_with_taskflow_api_v01', 
     default_args=default_args, 
     description='Our DAG using TaskFlow API', 
     schedule="0 6 2 * *",
     start_date=datetime(2021, 1, 1),
     end_date=datetime(2021, 3, 28))
def hello_world_etl():
    
    
    @task(multiple_outputs=True)
    def get_name():
        return{
            'first_name': 'Duc',
            'last_name': 'Nham'
        } 
    
    @task()
    def get_age():
        return 22
    
    @task()
    def greet(first_name,last_name, age):
        print(f'Hello world! My name is {first_name} {last_name}, and I am {age} years old!')
        
        
    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['firstname'],
          last_name=name_dict['last_name'],
          age=age)    
greet_dag = hello_world_etl()    
    