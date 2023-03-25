import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['country_domain'] = df['domain'].str.split('.').str[-1]
    top_10 = df.groupby('country_domain').domain.count().sort_values(ascending=False).reset_index().head(10)
    with open('top_10.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))

def get_longest():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_length'] = df['domain'].str.len()
    longest = df.domain[df.domain_length == df.domain_length.max()].sort_values().head(1)
    with open('longest.csv', 'w') as f:
        f.write(longest.to_csv(index=False, header=False))

def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    af_rank = df[df.domain == 'airflow.com']['rank']
    with open('af_rank.csv', 'w') as f:
        f.write(af_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('longest.csv', 'r') as f:
        longest = f.read()
    with open('af_rank.csv', 'r') as f:
        af_rank = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top_10)
    print(f'The longest domain for date {date}: ', longest)
    print(f'airflow.com rank for date {date}: ', af_rank)

default_args = {
    'owner': 'o-lokotilova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 30),
}
schedule_interval = '*/30 * * * *'

dag = DAG('olga_loko_first_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest',
                    python_callable=get_longest,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2,t3,t4] >> t5