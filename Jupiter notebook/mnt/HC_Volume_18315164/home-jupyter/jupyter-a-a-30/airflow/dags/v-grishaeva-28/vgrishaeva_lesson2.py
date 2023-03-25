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
        

def get_top10_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.str.split('.').str[-1]
    top10_zone = top_data_df.zone.value_counts().head(10).reset_index().rename(columns={'index':'zone', 'zone':'count'})
    with open('top10_zone.csv', 'w') as f:
        f.write(top10_zone.to_csv(index=False, header=False))

def get_max_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain']= top_data_df.domain.str.len()
    max_domain = top_data_df.sort_values('len_domain', ascending= False).head(1)
    with open('max_domain.csv', 'w') as f:
        f.write(max_domain.to_csv(index=False, header=False))
        

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df.domain.str.len()
    top_data_df = top_data_df.sort_values('len_domain', ascending= False).reset_index()
    airflow = top_data_df.query("domain == 'airflow.com'").reset_index()
    airflow = airflow[['domain', 'level_0']].rename(columns = {'level_0':'place'})
    if airflow.empty:
        result = 'Not in the list'
    else:
        result = airflow.place
    print(result)
    with open('airflow.txt', 'w') as f:
        f.write(str(result))


def print_data(ds): 
    with open('top10_zone.csv', 'r') as f:
        all_data = f.read()
    with open('max_domain.csv', 'r') as f:
        all_data_com = f.read()
    with open('airflow.txt', 'r') as f:
        airflow = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)

    print(f'The longest domain name for date {date}')
    print(all_data_com)
       
    print(f'The rank of airflow.com for date  {date}')
    print(airflow)


# In[4]:


default_args = {
    'owner': 'v.grishaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 12),
    'schedule_interval': '00 13 * * *'
}
dag = DAG('top_10_ru_vgrishaeva', default_args=default_args)


# In[5]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_zone',
                    python_callable=get_top10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_domain',
                    python_callable=get_max_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[6]:


t1 >> [t2, t3, t4] >> t5

