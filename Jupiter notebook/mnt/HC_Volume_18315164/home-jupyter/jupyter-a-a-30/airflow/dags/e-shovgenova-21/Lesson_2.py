#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[3]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[4]:


# pd.read_csv(TOP_1M_DOMAINS_FILE)


# In[5]:


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[6]:


def largest_domains():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['extend'] = df.apply(lambda row: row.domain.split('.')[-1], axis = 1) 
    result = df.groupby('extend').agg({'domain':'count'}).sort_values(by = 'domain', ascending=False).head(10) 
    
    with open('e_shovgenova_21_largest_domains.csv', 'w') as f:
        f.write(result.to_csv(index=False, header=False))


# In[7]:


def longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df.apply(lambda row: len(row.domain), axis=1)    
    result = df.sort_values(['length','domain'], ascending=[False,True]).iloc[0].domain  

    with open('e_shovgenova_21_longest_domain.csv', 'w') as f:
        f.write(result.to_csv(index=False, header=False))


# In[8]:


def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if df[df.domain == 'airflow.com'].shape[0]:
        result = df[df.domain == 'airflow.com'].iloc[0]['rank']
    else:
        result = 'There is no such domain'
    with open('e_shovgenova_21_get_airflow.csv', 'w') as f:
        f.write(str(result))


# In[9]:


def print_data(ds):
    with open('e_shovgenova_21_largest_domains.csv', 'r') as f:
        largest_domains = f.read()
    with open('e_shovgenova_21_longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('e_shovgenova_21_get_airflow.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top10 largest zones for date {date}')
    print(largest_zones)

    print(f'Longest domain name for date {date}')
    print(longest_domain)
    
    print(f'Airflow rank for date {date}')
    print(airflow_rank)


# In[10]:


default_args = {
    'owner': 'e_shovgenova_21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 5),
    'schedule_interval': '30 16 * * *'
}


# In[11]:


dag = DAG('e_shovgenova_21_HW_2', default_args=default_args)


# In[12]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='largest_domains',
                    python_callable=largest_domains,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                    python_callable=longest_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5


# In[ ]:




