#!/usr/bin/env python
# coding: utf-8

# In[8]:


import requests
from zipfile import ZipFile
from io import BytesIO
from datetime import timedelta
from datetime import datetime
from io import StringIO
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from airflow.operators.python import get_current_context


# In[2]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'


# In[3]:


TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[ ]:


default_args = {
    'owner':'s-klukvina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 23),
    'schedule_interval': '0 12 * * *' #timedelta(days=1)
}


# In[7]:


@dag(default_srgs=default_args, catchup=False)
def top_10_airflow_2():
    @task()
    def get_data():
        top_doms = requests.get(TOP_1M_DOMAINS, strem=True)
        zipfile = ZipFile(BytesIO(top_doms.content))
        top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
        return top_data
    @task()
    def get_stat_ru(top_data):
        top_data_df = pd.read(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        top_data_top_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        top_data_top_ru = top_data_top_10.head(10)
        return top_data_top_ru.to_csv(index=False)
    @task()
    def get_stat_com(top_data):
        top_data_df = pd.read(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        top_data_top_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
        top_data_top_com = top_data_top_com.head(10)
        return top_data_top_com.csv(index=False)
    @task()
    def print_data(top_data_top_ru, top_data_top_com):
        context=grt_current_context()
        date=context['ds']

        print(f'Top domains in .RU for date {date}')
        print(top_data_top_ru)

        print(f'Top domains in .COM for date {date}')
        print(top_data_top_com)


# In[13]:


top_data = get_data()
top_data_top_ru = get_stat_ru(top_data)
top_data_top_com = get_stat_com(top_data)
print_data(top_data_top_ru, top_data_top_com)


# In[1]:


top_10_airflow_2()


# In[9]:





# In[10]:





# In[11]:





# In[2]:





# In[22]:





# In[ ]:




