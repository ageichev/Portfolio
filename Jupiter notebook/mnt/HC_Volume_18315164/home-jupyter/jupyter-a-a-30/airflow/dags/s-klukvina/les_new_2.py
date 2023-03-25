#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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

def get_zones():
    df = pd.read_csv(TOP_1M_DOMAINS, header=None, names=['number', 'domain'])
    df['zone']=df.domain.str.split('.').str[1:].apply(lambda x: '.'.join(x))
    top_10_zones=df.groupby('zone', as_index=False).agg({'domain':'count'}) \
        .rename(columns={'domain':'count'}).sort_values('count', ascending=False).head(10)['zone']
    
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))

def get_max_len_domen():
    df = pd.read_csv(TOP_1M_DOMAINS, header=None, names=['number', 'domain'])
    df['length'] = df['domain'].str.len()
    df_max_length = df.sort_values(['domain', 'length'],ascending = False) \
        .sort_values('length', ascending = False).iloc[0]['domain']

    with open('max_length_name_domain.csv', 'w') as f:
        f.write(max_length_domain.to_csv(index=False, header=False))

def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS, header=None, names=['number', 'domain'])
    airflow_rank = df.query('domain == "airflow.com"').iloc[0]['number']
    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        all_data_zones = f.read()   
    with open('max_length_name_domain.csv', 'r') as f:
        all_data_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_rank_airflow = f.read()
    date = ds

    print(f'Top 10 domen zones {date}')
    print(all_data_zones)

    print(f'Domen with the longest name -  {date}')
    print(all_data_length)
    
    print(f'Airflows domen rang - {date}')
    print(all_rank_airflow)

default_args = {
    'owner': 's-klukvina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 29),
}
schedule_interval = '30 * * * *'

dag = DAG('s-klukvina-lesson2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zones',
                    python_callable=get_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len_domen',
                        python_callable=get_max_len_domen,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

