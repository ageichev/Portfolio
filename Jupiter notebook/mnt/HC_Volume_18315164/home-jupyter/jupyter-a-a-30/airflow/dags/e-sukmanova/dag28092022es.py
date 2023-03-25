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


# In[4]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[5]:





# In[16]:


#Найти топ-10 доменных зон по численности доменов
def get_top10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.str.rsplit(pat = '.', n = 1, expand = True).rename(columns = {1: 'zone'}).zone
    top_10 = top_10 = top_data_df.groupby('zone', as_index = False).agg({'domain': 'count'}).sort_values('domain', ascending = False).head(10).zone
    with open('top_10.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))


# In[27]:


#Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
def get_longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.domain.str.len()
    top_data_longest_domain = top_data_df.sort_values(['domain_length', 'domain'], ascending = [False, True]).head(1).domain
    with open('top_data_longest_domain.csv', 'w') as f:
        f.write(top_data_longest_domain.to_csv(index=False, header=False))


# In[44]:


#На каком месте находится домен airflow.com
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_airflow_rank = top_data_df.query('domain == "airflow.com"')
    top_data_airflow_rank['rank']
    with open('top_data_airflow_rank.csv', 'w') as f:
        f.write(top_data_airflow_rank.to_csv(index=False, header=False))


# In[46]:


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10.csv', 'r') as f:
        all_top_10 = f.read()
    with open('top_data_longest_domain.csv', 'r') as f:
        all_data_longest_domain = f.read()
    with open('top_data_airflow_rank.csv', 'r') as f:
        all_data_airflow_rank = f.read()
    date = ds
    print(f'Top 10 domain zones for date {date}')
    print(all_top_10)
    print(f'The longest domain name for date {date}')
    print(all_data_longest_domain)
    print(f'The airflow.com domain rank for date {date}')
    print(all_data_airflow_rank)


# In[48]:


default_args = {
    'owner': 'e-sukmanova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 28),
    'schedule_interval': '55 16  * * *'
}

dag_es = DAG('get_top_domains_esukmanova', default_args=default_args)

t1 = PythonOperator(task_id='get_data', python_callable=get_data, dag=dag_es)
t2 = PythonOperator(task_id='get_top10', python_callable=get_top10, dag=dag_es)
t3 = PythonOperator(task_id='get_longest', python_callable=get_longest, dag=dag_es)
t4 = PythonOperator(task_id='get_airflow_rank', python_callable=get_airflow_rank, dag=dag_es)
t5 = PythonOperator(task_id='print_data', python_callable=print_data, dag=dag_es)

t1 >> [t2, t3, t4] >> t5


