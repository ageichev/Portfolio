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


# In[3]:


TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[6]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[10]:


def get_stat_domain_size():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_domain_size = top_data_df['domain'].apply(lambda x: x.split('.')[-1]).value_counts().head(10)
    with open('top_10_domain_size.csv', 'w') as f:
        f.write(top_10_domain_size.to_csv(index=False, header=False))


# In[12]:


def get_stat_max_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_domain_name = max(top_data_df['domain'], key=lambda i: len(i))
    with open('max_domain_name.csv', 'w') as f:
        f.write(max_domain_name.to_csv(index=False, header=False))


# In[14]:


def get_stat_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = df.query('domain == "airflow.com"')
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


# In[16]:


def print_data(ds):
    with open('top_10_domain_size.csv', 'r') as f:
        top_10_domain_size = f.read()    
    with open('max_domain_name.csv', 'r') as f:
        max_domain_name = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds
    
    print(f'Top 10 domain_zone for date {date}')
    print(top_10_domain_size)
    
    print(f'the domain with the longest name for date {date}')
    print(max_domain_name)

    print(f'Rank of airflow.com domain for date {date}')
    print(airflow_rank)


# In[21]:


default_args = {
    'owner': 'd-kosinets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 2),
}
schedule_interval = '0 13 * * *'


# In[25]:


dag = DAG('d-kosinets_lesson_2', default_args=default_args, schedule_interval=schedule_interval)


# In[ ]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_domain_size = PythonOperator(task_id='get_stat_domain_size',
                    python_callable=get_stat_domain_size,
                    dag=dag)

t2_max_domain_name = PythonOperator(task_id='get_stat_max_domain_name',
                        python_callable=get_stat_max_domain_name,
                        dag=dag)

t2_airflow_rank = PythonOperator(task_id='get_stat_airflow_rank',
                        python_callable=get_stat_airflow_rank,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_domain_size, t2_max_domain_name, t2_airflow_rank] >> t3

