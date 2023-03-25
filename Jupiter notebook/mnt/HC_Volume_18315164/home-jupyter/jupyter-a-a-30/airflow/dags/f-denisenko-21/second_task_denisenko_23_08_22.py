import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

#Подгружаем данные

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

#Таски
#считываем 
def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    length = top_data_df
    length['letters_count']=length.domain.apply(lambda x: len(x))
    max_value=length.letters_count.max()
    the_longest_len =length.query("letters_count == @max_value")
    with open('the_longest_len.csv', 'w') as f:
        f.write(the_longest_len.to_csv(index=False, header=False))




def print_data(ds): # передаем глобальную переменную airflow
    with open('the_longest_len.csv', 'r') as f:
        all_data = f.read()
    date = ds

    print(f'It was done {date}')
    print(all_data)

# В Airflow есть свои глобальные переменные, список которых можно посмотреть  в документации

#Инициализируем DAG

default_args = {
    'owner': 'f-denisenko-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': '0 17 * * *'
}
dag = DAG('denisenko_2_task', default_args=default_args)

#Инициализируем таски

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

#Задаем порядок выполнения

t1 >> t2 >> t3
