#!/usr/bin/env python
# coding: utf-8


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import re

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
        
def top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['top_domains'] = top_data_df.domain.apply(lambda x: re.findall(r'\..*', x)[0].split('.')[-1])
    top_10_domains = top_data_df.groupby('top_domains', as_index=False)                                .agg({'domain': 'count'})                                .sort_values('domain', ascending=False).head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))
        
        
def max_length_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['names_length'] = top_data_df['domain'].str.len()
    max_length_name = top_data_df.sort_values('domain')\
                        .sort_values('names_length', ascending=False)\
                        .head(1)['domain']
    with open('max_length_name.csv', 'w') as f:
        f.write(max_length_name.to_csv(index=False, header=False))
        

def airflow_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if len(top_data_df.query('domain == "airflow.com"')) > 0:
        text = f'''Airflow is on {top_data_df.query('domain == "airflow.com"')['rank'].to_list()[-1]} position'''
    else:
        text = 'Domain does not exist'
    with open('airflow_domain.csv', 'w') as f:
        f.write(text)

        
def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        top_domain_data = f.read()
    with open('max_length_name.csv', 'r') as f:
        name_length_data = f.read()
    with open('airflow_domain.txt', 'r') as f:
        airflow_position_data = f.read()   
    date = ds

    print(f'Top 10 domains zones for date {date}')
    print(top_domain_data)

    print(f'Max domain name length for date {date}')
    print(name_length_data)
    
    print(f'Airflow domain: {date}')
    print(airflow_position_data)
    
default_args = {
    'owner': 'n_temirova_20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 6, 27),
}
schedule_interval = '0 10 * * *'

dag = DAG('n-temirova_20', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domains',
                    python_callable=top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='max_length_name',
                        python_callable=max_length_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_domain',
                        python_callable=airflow_domain,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

