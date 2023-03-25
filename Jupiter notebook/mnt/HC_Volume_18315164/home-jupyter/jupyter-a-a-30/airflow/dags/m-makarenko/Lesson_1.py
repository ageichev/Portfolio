
# Будем парсить статистику о самых посещаемых доменах

import requests as req
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datatime import timedelta
from datatime import datetime


from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http^//s3.amazonaws.com/alexa-static/top-1m.csv.zip' #ссылка на файл
TOP_1M_DOMAINS_FILE = 'top-1m.csv' #файл будет называться так


def get_data():
    top_doms = req.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domains'])
    top_data_top_10 = top_data_df[top_data_df['domains'].str.endswith('.ru')]
    top_data_top_10 = top_data_top_10.head(10)

    with open ('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))



def print_data():
    with open('top_data_top_10.csv', 'r') as f:
        all_data=f.read()
    print('Top domains in .RU')
    print(all_data)


default_args = {
    'owner': 'm.makarenko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 22),
    'schedule_interval': '0 12 * * *' #'@daily', '@weekly', timedelta(days=1)
}
dag = DAG('top_10_ru', default_args=default_args)

task1 = PythonOperator(task_id='get_data',
                       python_callabe=get_data,
                       dag=dag)

task2 = PythonOperator(task_id='get_stat',
                       python_callabe=get_stat,
                       dag=dag)

task3 = PythonOperator(task_id='print_data',
                       python_callabe=print_data,
                       dag=dag)


task11 >> task2 >> task3

# то же самое можно записать так:
# task1.set_downstream(task2)
# task2.set_downstream(task3)