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


# получаем данные
def get_data():    
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

# выделяем доменные зоны и считаем ТОП-10
def get_top_zones():    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.str.split('.').str[-1]
    top_10_zones = top_data_df.groupby('zone').agg({'domain':'count'}).sort_values('domain', ascending = False).head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))
        
# находим самый длинный домен
def get_largest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_name'] = top_data_df.domain.str.len()
    largest_name = top_data_df.sort_values('len_name', ascending = False).head(1)
    with open('largest_name.csv', 'w') as f:
        f.write(largest_name.to_csv(index=False, header=False))

# находим позицию домена airflow.com
def get_airflow():        
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])                    
    if top_data_df.query("domain == 'airflow.com'").shape[0]:
        airflow_rank = str(df[df.domain == 'airflow.com'].iloc[0]['rank'])
    else:
        airflow_rank = 'airflow.com is not listed'
    with open('airflow_rank.txt', 'w') as f:
        f.write(airflow_rank) 
    
# выводим ответы на вопросы
def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('largest_name.csv', 'r') as f:
        largest_name = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()        
    date = ds

    print(f'Top domains zones for date {date}')
    print(top_10_zones)

    print(f'Largest domains name for date {date}')
    print(largest_name)
    
    print(f'Airflow rank for date {date}')
    print(airflow_rank)


# In[5]:


# инициализируем DAG
default_args = {
    'owner': 'd-andreev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 21),
}
schedule_interval = '00 13 * * *'

dag_d_andreev = DAG('d-andreev', default_args=default_args, schedule_interval=schedule_interval)


# In[6]:


# инициализируем таски
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_d_andreev)

t2 = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag_d_andreev)

t3 = PythonOperator(task_id='get_largest_domain',
                    python_callable=get_largest_domain,
                    dag=dag_d_andreev)

t4 = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag_d_andreev)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_d_andreev)

# задаем порядок выполнения
t1 >> [t2, t3, t4] >> t5

