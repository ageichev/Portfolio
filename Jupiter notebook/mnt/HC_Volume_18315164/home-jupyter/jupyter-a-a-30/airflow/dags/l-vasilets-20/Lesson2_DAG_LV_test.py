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


# Подгружаем данные

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[3]:


# Таски

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_stat_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10_com.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_top_10_com.csv', 'r') as f:
        all_data_com = f.read()
    date = ds

    print(f'Top domains in .RU for date {date}')
    print(all_data)

    print(f'Top domains in .COM for date {date}')
    print(all_data_com)


# In[4]:


# В Airflow есть свои глобальные переменные, список которых можно посмотреть  в документации

# Инициализируем DAG

default_args = {
    'owner': 'l-vasilets-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 4),
    'schedule_interval': '0 12 * * *'
}
dag = DAG('Lesson2_DAG_LV_test', default_args=default_args)


# In[5]:


# Инициализируем таски

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat',
                    python_callable=get_stat,
                    dag=dag)

t2_com = PythonOperator(task_id='get_stat_com',
                        python_callable=get_stat_com,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[6]:


# Задаем порядок выполнения

t1 >> [t2, t2_com] >> t3
