#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# top 10 domain zones
def top_domain_zones():
    top_doms = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_doms['domain_zone'] = top_doms['domain'].apply(lambda x: x.split('.')[-1]) 
    top_10_dom_zones_by_num = top_doms['domain_zone']                                                    .value_counts()                                                     .head(10)                                                    .to_frame()                                                     .reset_index()                                                     .rename(columns= {'index': 'domain_zone', 'domain_zone': 'num'})
    with open('top_10_dom_zones_by_num.csv', 'w') as f:
            f.write(top_10_dom_zones_by_num.to_csv(index=False, header=False))
                    
# the longest domain name
def domain_zones_lenght():
    top_doms = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    top_doms['domain_name_len'] = top_doms['domain'].apply(lambda x: len(x)) 
    longest_domain_name = top_doms.sort_values(['domain_name_len', 'domain'], ascending=[False, True]).iloc[0].domain
    with open('longest_domain_name.txt', 'w') as f:
        f.write(longest_domain_name)
                
# the rank of airflow.com
def airflow_rank():
    top_doms = pd.read_csv(TOP_1M_DOMAINS, names=['rank', 'domain'])
    if 'airflow.com' in top_doms['domain']:
        rank = int(top_doms[top_doms['domain'] == 'airflow.com']['rank'])
    else:
        rank = 'airflow.com not in the data'
    with open('rank.txt', 'w') as f:
        f.write(rank)

# print all results
def print_data(ds):
    with open('top_10_dom_zones_by_num.csv', 'r') as f:
        zones_by_num = f.read()
    with open('longest_domain_name.txt', 'r') as f:
        max_domain_name = f.read()
    with open('rank.txt', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(zones_by_num)

    print(f'Max domain lenght for date {date}')
    print(max_domain_name)
    
    print(f'Airflow.com rank for date {date}')
    print(airflow_rank)
                
default_args = {
    'owner': 't.varenichenko-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 7, 31),
}
schedule_interval = '0 */4 * 7,8 *'

dag = DAG('top_10_t_varenichenko', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id ='top_domain_zones',
                    python_callable = top_domain_zones,
                    dag=dag)

t2 = PythonOperator(task_id='domain_zones_lenght',
                    python_callable = domain_zones_lenght,
                    dag=dag)

t3 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3] >> t4                

