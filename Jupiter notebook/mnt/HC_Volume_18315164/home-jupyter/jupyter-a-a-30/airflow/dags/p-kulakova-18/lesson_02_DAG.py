import requests
import pandas as pd
import numpy as np
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


def get_stat_top_10_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_1 = top_data_df['domain'].apply(lambda text: '.'.join(text.split('.')[-1:])).to_frame()
    df_1['index'] = df_1.index
    df_2 = df_1.groupby('domain').count().sort_values('index', ascending=False).reset_index()
    df_3 = df_2.domain.head(10)
    df_3.to_csv('df_3.csv', index=False, header=False)


def get_max_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_1 = top_data_df['domain'].apply(lambda text: '.'.join(text.split('.')[-1:])).to_frame()
    df_4 = df_1.domain.to_list()
    max_length = max(df_4, key = len)
    with open('max_length.csv', 'w') as f:
        f.write(max_length)
        
def airflow_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    idx_0 = top_data_df[top_data_df["domain"] == 'airflow.com']
    idx_1 = idx_0.index
    idx_2 = idx_1 + 1
    idx = top_data_df.iloc[idx_2]['rank'].to_string()
    with open('idx.csv', 'w') as f:
        f.write(idx)


def print_data(ds):
    with open('df_3.csv', 'r') as f:
        all_data_top_10 = f.read()
    with open('max_length.csv', 'r') as f:
        all_data_max = f.read()
    with open('idx.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data_top_10)

    print(f'Max length domain for date {date}')
    print(all_data_max)
    
    print(f'Airflow.com index for date {date}')
    if all_data_airflow == 'Series([], )':
        print('Domain does not exist')
    else:
        print(all_data_airflow)


default_args = {
    'owner': 'p-kulakova-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 14),
}
schedule_interval = '0 9 * * *'

dag = DAG('dag_lesson2_p-kulakova-18', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_top_10_domain',
                    python_callable=get_stat_top_10_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_length',
                        python_callable=get_max_length,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_com',
                        python_callable=airflow_com,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

