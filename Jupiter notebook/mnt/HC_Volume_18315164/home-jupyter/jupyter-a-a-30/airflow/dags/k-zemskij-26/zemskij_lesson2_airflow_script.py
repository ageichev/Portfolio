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


def top_zone():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['dom_zone'] = top_doms['domain'].apply(lambda x: x.split('.')[-1])
    top_10_doms_zone = top_doms                         .groupby('dom_zone', as_index=False)                         .agg(quantity=('domain', 'count'))                         .sort_values('quantity', ascending=False)                         .head(10)
    with open('top_10_doms_zone.csv', 'w') as f:
        f.write(top_10_doms_zone.to_csv(index=False, header=False))
        

def biggest_name():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['doms_len'] = top_doms.domain.apply(lambda x: len(x))
    max_len = top_doms.doms_len.max()
    large_name = top_doms.query('doms_len == @max_len').sort_values('domain').head(1).domain.iloc[0]
    with open('large_name.csv', 'w') as f:
        f.write(large_name)


def place():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_doms.query('domain == "airflow.com"').shape[0] > 0:
        dom_place = top_doms.query('domain == "airflow.com"').iloc[0][0]
    else:
        dom_place = 'This domain is not in the data'
    with open('dom_place.csv', 'w') as f:
        f.write(str(dom_place))


def print_data(ds):
    with open('top_10_doms_zone.csv', 'r') as f:
        top_10_zone = f.read()
    with open('large_name.csv', 'r') as f:
        large_name = f.read()
    with open('dom_place.csv', 'r') as f:
        dom_place = f.read()
    date = ds

    print(f'Top domains zone for date {date}:')
    print(top_10_zone)

    print(f'Domain with the longest name for date {date}:')
    print(large_name)
    
    print(f'Where is the airflow.com domain located for date {date}?')
    print(dom_place)


default_args = {
    'owner': 'k.zemskii',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 16),
    'schedule_interval': '0 1 * * *'
}

dag = DAG('k-zemskij-26_lesson2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_zone = PythonOperator(task_id='top_zone',
                    python_callable=top_zone,
                    dag=dag)

t2_biggest_name = PythonOperator(task_id='biggest_name',
                        python_callable=biggest_name,
                        dag=dag)

t2_dom_place = PythonOperator(task_id='place',
                        python_callable=place,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_zone, t2_biggest_name, t2_dom_place] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

