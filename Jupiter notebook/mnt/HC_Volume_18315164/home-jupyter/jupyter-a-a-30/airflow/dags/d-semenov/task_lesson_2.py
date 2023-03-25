#!/usr/bin/env python
# coding: utf-8

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_domains_zone():
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_df.domain = top_domain_df.domain.apply(lambda x: x.split('.')[-1])
    top_domain_df = top_domain_df.groupby('domain', as_index=False) \
                                 .agg({'rank': 'count'}) \
                                 .sort_values('rank', ascending=False).head(10)
    with open('top_domain_zone_df_top_10.csv', 'w') as f:
        f.write(top_domain_df.to_csv(index=False, header=False))


def get_longest_domain():
    domain_len_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_len_df['domain_len'] = domain_len_df.domain.str.len()
    domain_len_df.sort_values(['domain_len', 'domain'], ascending=[False, True], inplace=True)
    domain_len_df.reset_index(drop=True, inplace=True)
    longest_domain = domain_len_df.head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))


def get_airflow_pos():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        airflow_index = df.loc[df.domain == 'airflow.com'].index.item()
    except:
        airflow_index = 'No in base'
    with open('airflow_pos.txt', 'w') as f:
        f.write(airflow_index)


def print_data(ds):
    with open('top_domain_zone_df_top_10.csv', 'r') as f:
        top_10_domains_zone = f.read()
    
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    
    with open('airflow_pos.txt', 'r') as f:
        airflow_pos = f.read()

    date = ds
    print(f'Top domains for date {date}')
    print(top_10_domains_zone)

    print(f'Longest_domain for date {date}')
    print(longest_domain)

    print(f'Airflow domain position for date {date}')
    print(airflow_pos)

default_args = {
    'owner': 'd-semenov', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 6, 2) # Дата начала выполнения DAG 
}

schedule_interval = '10 12 * * *' # cron выражение, также можно использовать '@daily', '@weekly'

dag = DAG('d-semenov_task_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='get_top_domains_zone',
                    python_callable=get_top_domains_zone,
                    dag=dag)

t2_long = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_pos',
                        python_callable=get_airflow_pos,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_long, t2_airflow] >> t3
