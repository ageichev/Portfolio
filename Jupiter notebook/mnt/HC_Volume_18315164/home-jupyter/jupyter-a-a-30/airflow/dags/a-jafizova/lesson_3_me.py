#!/usr/bin/env python
# coding: utf-8

# In[10]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


# In[11]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[13]:


default_args = {
    'owner': 'a.jafizova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def top_10_airflow_Aig():

    @task()
    def get_data():
        top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
        zipfile = ZipFile(BytesIO(top_doms.content))
        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
        return top_data 


    @task()
    def get_table_ru(top_data):
        top_data_df = pd.read_csv(StringID(top_data), names=['rank', 'domain'])
        top_data_top_10_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        top_data_top_10_ru = top_data_top_10_ru.head(10)
        return top_data_top_10_ru.to_csv(index=False)


    @task()
    def get_table_com(top_data):
        top_data_df = pd.read_csv(StringID(top_data), names=['rank', 'domain'])
        top_data_top_10_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
        top_data_top_10_com = top_data_top_10_com.head(10)
        return top_data_top_10_com.to_csv(index=False)

    @task()
    def print_data(top_data_top_10_ru, top_data_top_10_com): 

        date = ''

        print(f'Top domains in .RU for date {date}')
        print(top_data_top_10_ru)

        print(f'Top domains in .COM for date {date}')
        print(top_data_top_10_com)

    top_data = get_data()
    top_data_ru = get_table_ru(top_data)
    top_data_com = get_table_com(top_data)
    print_data(top_data_ru, top_data_com)

top_10_airflow_Aig = top_10_airflow_Aig()

