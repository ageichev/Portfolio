#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[2]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[3]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[4]:


def get_top():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df['domain'].apply(lambda x: x.split('.')[-1]).value_counts().head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))    


# In[5]:


def get_max_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df['domain']     .apply(lambda x: x.split('.')[:-1]).apply(lambda x: ''.join(x)).apply(lambda x: len(str(x)))
    max_len_domain = top_data_df.sort_values(['domain_len','domain'], ascending = [False, True])     .reset_index().domain[0]
    with open('max_len_domain.txt', 'w') as f:
        f.write(str(max_len_domain))


# In[6]:


def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if len(top_data_df[top_data_df.domain == "airflow.com"]['rank']) == 0:
        airflow_stat = 'There is no "airflow.com"'
        with open('airflow_stat.txt', 'w') as f:
            f.write(str(airflow_stat))
    else:
        airflow_stat = int(top_data_df.query('domain == "airflow.com"')['rank'])
        with open('airflow_stat.txt', 'w') as f:
            f.write(str(airflow_stat))


# In[7]:


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_data = f.read()
    with open('max_len_domain.txt', 'r') as f:
        max_len_domain = f.read()
    with open('airflow_stat.txt', 'r') as f:
        airflow_stat = f.read()
        
    date = ds


    print(f'Top domains zones for date {date}')
    print(top_data)

    print(f'Domain with the longest name for date {date}')
    print(max_len_domain)

    print(f'Airflow rank for date for date {date}')
    print(airflow_stat)


# In[8]:


default_args = {
    'owner': 'd_matveev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '0 14 * * *'

dag = DAG('d_matveev_dag', default_args=default_args, schedule_interval=schedule_interval)


# In[9]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top',
                    python_callable=get_top,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len_domain',
                        python_callable=get_max_len_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[10]:


t1 >> [t2, t3, t4] >> t5

