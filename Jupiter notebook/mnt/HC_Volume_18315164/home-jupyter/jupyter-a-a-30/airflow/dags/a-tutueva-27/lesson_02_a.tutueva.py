import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top10():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top10 = df.domain.apply(lambda x: x.split('.')[-1]).value_counts().head(10).index.to_list()
    top10 = ", ".join(top10)
    with open('top_10.txt', 'w') as f:
        f.write(top10)


def get_longest():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_length = df.domain.str.len().max()
    longest = df.loc[df.domain.apply(lambda x: len(x) == max_length)].sort_values('domain').head(1).reset_index().domain[0]
    with open('longest.txt', 'w') as f:
        f.write(longest)


def airflow_index():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        airflow_index = str(df[df.domain == 'airflow.com'].index[0])
    except:
        airflow_index = 'Entry not found.'
    with open('airflow.txt', 'w') as f:
        f.write(airflow_index)


def print_data(ds):
    with open('top_10.txt', 'r') as f:
        top10 = f.read()
    with open('longest.txt', 'r') as f:
        longest = f.read()
    with open('airflow.txt', 'r') as f:
        airflow_index = f.read()
    date = ds

    print(f'Top 10 domains by quantity for date {date}')
    print(top10)

    print(f'Longest domain for date {date}')
    print(longest)

    print(f'Airflow.com position for date {date}')
    print(airflow_index)


default_args = {
    'owner': 'a-tutueva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 19),
}
schedule_interval = '0 12 * * *'

dag = DAG('a-tutueva', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_top10',
                    python_callable=get_top10,
                    dag=dag)

t2_long = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)

t2_airflow = PythonOperator(task_id='airflow_index',
                        python_callable=airflow_index,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_long, t2_airflow] >> t3
