#!/usr/bin/env python
# coding: utf-8

# In[56]:


import pandas as pd 
import requests
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[57]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[58]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[94]:


def get_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain = top_data_df.domain.apply(lambda x: x.split('.')[1]).value_counts().reset_index()
    top_domain_10 = top_domain.head(10)
    
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_domain_10.to_csv(index=False, header=False))


# In[172]:


def get_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.apply(lambda x: len(x))
    top_data_len_max = top_data_df.sort_values(['len', 'domain'], ascending=[False, True]).head(1)
    with open('top_data_max_len.csv', 'w') as f:
        f.write(top_data_len_max.to_csv(index=False, header=False))


# In[168]:


def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df.query('domain=="airflow.com"').shape[0] == 0:
        print('No domain airflow')
    else:
        airflow = top_data_df.query('domain=="airflow.com"')
        with open('airflow.csv', 'w') as f:
            f.write(airflow.to_csv(index=False, header=False))


# In[169]:


def print_data(ds): # передаем глобальную переменную airflow
    
    with open('top_10_domain.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_max_len.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow.csv', 'r') as f:
        airflow_data = f.read()
        if len(airflow_data) == 0:
            airflow_data = 'has not domain'
    date = ds

    print(f'Top 10 domains zone for date {date}')
    print(all_data)

    print(f'Domain name with maximum length for date {date}')
    print(all_data_len)
    
    print(f'Airflow for date {date}')
    print(airflow_data)


# In[195]:


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domain.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_max_len.csv', 'r') as f:
        all_data_len = f.read()
    with open('airflow.csv', 'r') as f:
        airflow_data = f.read()
        if len(airflow_data) == 0:
            airflow_data = 'has not domain'
    date = ds

    print(f'Top 10 domains zone for date {date}')
    print(all_data)

    print(f'Domain name with maximum length for date {date}')
    print(all_data_len)
    
    print(f'Airflow for date {date}')
    print(airflow_data)


# In[64]:


default_args = {
    'owner': 'd-toktasyn-26',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 16),
    'schedule_interval': '0 9 * * *'
}
dag = DAG('top_10_by_d-toktasyn-26', default_args=default_args)


# In[65]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain',
                    python_callable=get_dom,
                    dag=dag)

t3 = PythonOperator(task_id='get_len',
                    python_callable=get_len,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[66]:


t1 >> [t2, t3, t4] >> t5


# In[ ]:




