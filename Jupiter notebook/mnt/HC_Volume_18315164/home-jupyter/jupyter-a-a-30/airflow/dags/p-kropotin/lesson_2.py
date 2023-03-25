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


def get_top_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_zone = top_data_df[['rank', 'domain']]
    top_data_zone['domain'] = top_data_zone['domain'].str.split('.').apply(lambda x: x[-1])
    top_data_zone = top_data_zone.groupby('domain', as_index=False).nunique('rank').sort_values('rank', ascending=False).head(10)
    with open('top_data_zone.csv', 'w') as f:
        f.write(top_data_zone.to_csv(index=False, header=False))

def get_top_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_data_length = top_data_df[['rank', 'domain']]
    top_data_length['length'] = top_data_length['domain'].str.len()
    top_data_length = top_data_length[top_data_length.length == top_data_length.length.max()]
    with open('top_data_length.csv', 'w') as f:
        f.write(top_data_length.to_csv(index=False, header=False))

def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    data_rank_airflow = top_data_df[top_data_df['domain'].str.startswith('airflow.com')]
    with open('data_rank_airflow.csv', 'w') as f:
        f.write(data_rank_airflow.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_data_zone.csv', 'r') as f:
        data_1 = f.read()
    with open('top_data_length.csv', 'r') as f:
        data_2 = f.read()
    with open('data_rank_airflow.csv', 'r') as f:
        data_3 = f.read()
    date = ds

    print(f'Top domains zone for date {date}')
    print(data_1)
    
    print(f'Top lenght domain for date {date}')
    print(data_2)
    
    print(f'Rank domain Airflow for date {date}')
    print(data_3)


default_args = {
    'owner': 'p.kropotin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 18),
}
schedule_interval = '00 12 * * *'

dag = DAG('p-kropotin_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top_zone',
                    python_callable=get_top_zone,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_top_length',
                    python_callable=get_top_length,
                    dag=dag)

t2_3 = PythonOperator(task_id='get_rank_airflow',
                    python_callable=get_rank_airflow,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3


