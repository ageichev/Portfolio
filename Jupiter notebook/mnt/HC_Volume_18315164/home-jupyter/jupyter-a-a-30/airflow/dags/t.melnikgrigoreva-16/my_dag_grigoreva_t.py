#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime


# In[3]:


from airflow import DAG
from airflow.operators.python import PythonOperator


# In[4]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[5]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[6]:


def get_top_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    acs = top_data_df['domain'].str.split('.', expand = True)
    acs.columns=['name','domain','a', 'b' 'c', 'd']
    top_domains = acs.groupby(['domain'], as_index = False).agg({'name' : 'count'}).sort_values('name', ascending = False)
    top_domains_10 = top_domains.head(10)
    with open('top_domains_10.csv', 'w') as f:
        f.write(top_domains_10.to_csv(index=False, header=False))


# In[7]:


def get_longest_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domen_len = top_data_df.assign(len = top_data_df.domain.str.len())
    lenth = top_data_df.sort_values('len', ascending = False).head(1)
    with open('lenth.csv', 'w') as f:
        f.write(lenth.to_csv(index=False, header=False))


# In[8]:


def get_airflow_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domen_len = top_data_df.assign(len = top_data_df.domain.str.len())
    airflow = top_data_df.loc[top_data_df['domain'] == 'airflow.com']
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=False, header=False))


# In[9]:


def print_stats(ds):
    with open('top_domains_10.csv', 'r') as f:
        tops = f.read()
    with open('lenth.csv', 'r') as k:
        lenth = k.read()
    with open('airflow.csv', 'r') as l:
        flows = l.read() 
    date = ds

    print(f'Top-10 domains for date {date}')
    print(tops)

    print(f'Longerst domain for date {date}')
    print(lenth)
    
    print(f'Airflow Rank for date {date}')
    print(flows)


# In[12]:


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 1, 24),
    'schedule_interval': '0 12 * * *'
}

dag = DAG('grigor_t_16', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain',
                    python_callable=get_top_domain,
                   dag=dag)

t2_long = PythonOperator(task_id='get_longest_dom',
                        python_callable=get_longest_dom,
                        dag=dag)

t2_air = PythonOperator(task_id='get_airflow_dom',
                        python_callable=get_airflow_dom,
                        dag=dag)

t3 = PythonOperator(task_id='print_stats',
                    python_callable=print_stats,
                    dag=dag)


# In[13]:


t1 >> [t2, t2_long, t2_air] >> t3

t1.set_downstream(t2)
t1.set_downstream(t2_long)
t1.set_downstream(t2_air)

t2.set_downstream(t3)
t2_long.set_downstream(t3)
t2_air.set_downstream(t3)


# In[ ]:




