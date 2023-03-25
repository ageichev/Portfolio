#!/usr/bin/env python
# coding: utf-8

# In[6]:


import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[7]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[8]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])

    top_10_domain_zone = top_data_df \
                            .groupby('domain_zone', as_index=False) \
                            .agg({'domain': 'count'}) \
                            .rename(columns={'domain': 'domain_count'}) \
                            .sort_values('domain_count', ascending=False) \
                            .head(10)

    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))  
        
def max_len_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_name'] = top_data_df['domain'].apply(lambda x: len(x.split('.')[0]))
    max_len_name = top_data_df \
                        .sort_values(['len_name', 'domain'], ascending={False, True}) \
                        [['domain', 'len_name']] \
                        .head(1)
    
    with open('max_len_name.csv', 'w') as f:
        f.write(max_len_name.to_csv(index=False, header=False)) 
        
def rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    if top_data_df.query("domain == 'airflow.com'").shape[0] != 0:
        rank_airflow = top_data_df.query("domain == 'airflow.com'")
    else:
        rank_airflow = pd.DataFrame({'rank': 'not found', 'domain': ['airflow.com']})

    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False)) 
        
def print_results(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10 = f.read()
    with open('max_len_name.csv', 'r') as f:
        max_len = f.read()
    with open('rank_airflow.csv', 'r') as f:
        airflow_com = f.read()
    date = ds

    print(f'TOP-10 domain zone for date {date}')
    print(top_10)

    print(f'Domain with longer name for date {date}')
    print(max_len)
    
    print(f'Rank of airflow.com for date {date}')
    print(airflow_com)


# In[9]:


default_args = {
    'owner': 'a-gavrilik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 24),
    'schedule_interval': timedelta(days=1)
}

dag = DAG('a-gavrilik_lesson2', default_args=default_args)


# In[10]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='max_len_name',
                        python_callable=max_len_name,
                        dag=dag)

t4 = PythonOperator(task_id='rank_airflow',
                    python_callable=rank_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_results',
                    python_callable=print_results,
                    dag=dag)


# In[11]:


t1 >> [t2, t3, t4] >> t5


# In[ ]:




