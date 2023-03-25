# Импортируем библиотеки

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Подгружаем данные

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# Таски

# Получаем данные в файл top-1m.csv

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# Найти топ-10 доменных зон по численности доменов

def top_domain():
    top_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain = top_domain['domain'].apply(lambda x: x.split('.')[1]) \
                                    .value_counts() \
                                    .to_frame() \
                                    .reset_index() \
                                    .head(10)
    with open('top_domain.csv', 'w') as f:
        f.write(top_domain.to_csv(index=False, header=False))

def max_len_domain():
    max_len_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len_domain['domain_len'] = max_len_domain['domain'].apply(lambda x: len(x))
    max_len_domain = max_len_domain.sort_values(['domain_len', 'domain'], ascending = [False, True]).iloc[0].domain
    with open('max_len_domain.txt', 'w') as f:
        f.write(str(max_len_domain))

def airflow_rank():
    airflow_rank = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if 'airflow.com' in airflow_rank['domain']:
        result = int(airflow_rank[airflow_rank['domain'] == 'airflow.com']['rank'])
    else:
        result = 'airflow.com not in the list'
    with open('result.txt', 'w') as f:
        f.write(str(result))

def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_domain_zone = f.read()
    with open('max_len_domain.txt', 'r') as f:
        max_len_domain = f.read()
    with open('result.txt', 'r') as f:
        result = f.read()
    date = ds

    print(f'Top 10 domain zones by count for  date {date}:')
    print(top_10_domain_zone)

    print(f'Longest domain:')
    print(max_len_domain)

    print('Rank airwlof.com is:')
    print(result)

# Инициализируем DAG    
    
default_args = {
    'owner': 'e-artjuhov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1),
    'schedule_interval': '0 12 * * *'
}

dag = DAG('e-artjuhov-22_lesson_2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_domain',
                    python_callable=top_domain,
                    dag=dag)

t3 = PythonOperator(task_id='max_len_domain',
                    python_callable=max_len_domain,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
