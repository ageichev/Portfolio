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


def prepare_data():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df[['domain_name', 'domain_zone']] = df['domain'].str.rsplit(pat='.', n=1, expand=True)
    df['domain_len'] = df['domain_name'].str.len()
    with open('prepared_data.csv', 'w') as f:
        f.write(df.to_csv(index=False))


# In[5]:


def print_data():
    with open('prepared_data.csv', 'r') as f:
        df = pd.read_csv(f)
    top_domain_zone = df.groupby('domain_zone', as_index=False)     .agg({'domain': 'count'})     .sort_values('domain', ascending=False)     ['domain_zone'].head(10).to_list()
    print("Топ 10 доменных зон: {}".format(top_domain_zone))
    longest_domain_name = df.sort_values(['domain_len', 'domain_name'], ascending=(False, True)).iloc[0]['domain_name']
    print("Самое длинное название домена - {}".format(longest_domain_name))
    if df[df['domain'] == 'ariflow.com'].shape[0] == 0:
        airflow_rank = 'не определено, домен ariflow.com не найден'
    else:
        airflow_rank = df[df['domain'] == 'ariflow.com'].loc[0]['rank']
    print('Ранговое место домена ariflow.com {}'.format(airflow_rank))


# In[6]:


default_args = {
    'owner': 'i-gusev-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2023, 1, 12),
    'schedule_interval': '0 18 * * *'
}
dag = DAG('i-gusev-18', default_args=default_args)


# In[7]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='prepare_data',
                    python_callable=prepare_data,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                        python_callable=print_data,
                        dag=dag)


# In[8]:


t1 >> t2 >> t3


# In[ ]:




