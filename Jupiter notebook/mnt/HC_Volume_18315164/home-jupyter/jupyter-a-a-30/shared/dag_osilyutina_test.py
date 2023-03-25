from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'osilyutina',                  
    'depends_on_past': False,            
    'start_date': datetime(2018, 1, 1),  
    'retries': 0                         
}
 
dag = DAG('test_jupyter_py',        
    default_args=default_args, 
    catchup=False,              
    schedule_interval='00 20   * * *')  
    
def hello():
    print('hello, world!')
    
run_this = PythonOperator(
    task_id='hello',
    python_callable=hello,
    dag=dag
)
