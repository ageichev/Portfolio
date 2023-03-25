import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print('hello world')
    
def hello_2():
    print('hello again')

# --- Initialising DAG

default_args = {
    'owner': 'a-posazhennikov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 02, 05),
    'schedule_interval': '60 * * * *'
}
dag = DAG('a_pos_hell_0', default_args=default_args)

# --- Initialising tasks

t1 = PythonOperator(task_id='hello',
                    python_callable=hello,
                    dag=dag)
t2 = PythonOperator(task_id='hello_2',
                    python_callable=hello_2,
                    dag=dag)



# --- Making order of execution

t1 >> t2

