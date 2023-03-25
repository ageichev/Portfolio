#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from zipfile import ZipFile
from io import BytesIO
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
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[4]:


def get_top_10():
    top_10 = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10['domain_zone'] = top_10.apply(lambda x: x['domain'].split('.')[-1], axis=1)
    top_10_df = top_10.groupby('domain_zone', as_index = False)     .agg({'domain': 'count'})     .sort_values('domain', ascending = False)     .head(10)
    with open('top_10_df.csv', 'w') as f:
        f.write(top_10_df.to_csv(index=False, header=False))


# In[5]:


def get_long_name():
    long_name = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    long_name['long_domain'] = long_name.apply(lambda x: len(x.domain), axis=1) 
    long_name_df = long_name.sort_values(['long_domain', 'domain'], ascending = False)['domain'].head(1)
    with open('long_name_df.csv', 'w') as f:
        f.write(long_name_df.to_csv(index=False, header=False))


# In[6]:


def get_airflow():
    airflow = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_df = airflow.query('domain == "airflowsummit.org"')[['rank', 'domain']]
    with open('airflow_df.csv', 'w') as f:
        f.write(airflow_df.to_csv(index=False, header=False))


# In[7]:


def print_data(ds): 
    with open('top_10_df.csv', 'r') as f:
        top_10_data = f.read()
    with open('long_name_df.csv', 'r') as f:
        long_name_data = f.read()
    with open('airflow_df.csv', 'r') as f:
        airflow_data = f.read()
    date = ds

    print(f'Top 10 domain zones by number of domains {date}')
    print(top_10_data)

    print(f'Longest domain name {date}')
    print(long_name_data)
    
    print(f'Airflow rank {date}')
    print(airflow_data)


# In[8]:


default_args = {
    'owner': 'k-parfenova-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 7, 6),
    'schedule_interval': '0 8 * * *'
}
dag = DAG('parfenova_k_answer', default_args=default_args)


# In[9]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_name',
                        python_callable=get_long_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[10]:


t1 >> [t2, t3, t4] >> t5

