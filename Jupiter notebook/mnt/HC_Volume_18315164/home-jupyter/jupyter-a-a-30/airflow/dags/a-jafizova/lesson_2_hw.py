#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[3]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[4]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_area():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_aria'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_10 = top_data_df.groupby('domain_aria', as_index = False).agg({'domain':'count'})    .sort_values('domain', ascending = False)
    top_data_10 = top_data_10.head(10)
    
    with open('top_data_10.csv', 'w') as f:
        f.write(top_data_10.to_csv(index=False, header=False))


def get_max_str():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['max_str'] = top_data_df.domain.apply(len)
    max_domain = top_data_df[['domain', 'max_str']].sort_values('max_str', ascending = False).head(1)
   
    with open('max_domain.csv', 'w') as f:
        f.write(max_domain.to_csv(index=False, header=False))
        

def airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) 
    
    with open('airflow.csv', 'w') as f:
        if top_data_df[top_data_df['domain'] == 'airflow.com'].empty:
            f.write('airflow is not found')
        else:
            airflow = top_data_df[top_data_df['domain'] == 'airflow.com']['rank']
            f.write(airflow)
        
    

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_10.csv', 'r') as f:
        top_data_10 = f.read()
    with open('max_domain.csv', 'r') as f:
        max_domain = f.read()
    with open('airflow.csv', 'r') as f:
        airflow = f.read()
    date = ds

    print(f'Top 10 domains zones for {date}')
    print(top_data_10)

    print(f'Longest domains for date {date}')
    print(max_domain)
    
    print(f'Airflow place for {date}')
    print(airflow)


# In[5]:


default_args = {
    'owner': 'a-jafizova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 21),
    'schedule_interval': '0 17 * * *'
}
dag = DAG('domains_Aigul', default_args=default_args)


# In[6]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_area',
                    python_callable=get_area,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_str',
                        python_callable=get_max_str,
                        dag=dag)

t4 = PythonOperator(task_id='airflow',
                        python_callable=airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[7]:


t1 >> [t2, t3, t4] >> t5

