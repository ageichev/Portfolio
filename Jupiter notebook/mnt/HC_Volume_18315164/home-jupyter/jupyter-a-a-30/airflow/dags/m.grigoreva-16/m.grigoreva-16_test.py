#!/usr/bin/env python
# coding: utf-8

# In[80]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from datetime import date

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[4]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[25]:


def get_top_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.str.split('.').str[1]
    top_10_domain_zones = top_data_df.groupby('domain_zone', as_index=False)                             .agg({'domain':'count'})                             .rename(columns={'domain':'domain_count'})                             .sort_values('domain_count', ascending=False)                             .head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))


# In[44]:


def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.domain.str.split('.').str[0].str.len()
    top_data_df.sort_values(['domain_length', 'domain'], ascending=[False, True], inplace=True)
    longest_domain = top_data_df.head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))


# In[105]:


def get_airflow_data():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_data = top_data_df.query('domain == "airflow.com"')
    with open('airflow_data.csv', 'w') as f:
        f.write(airflow_data.to_csv(index=False, header=False))


# In[110]:


def print_data():
    with open('top_10_domain_zones.csv', 'r') as f:
        top_domain_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_data.csv', 'r') as f:
        airflow = f.read()
        rank = str(airflow).split(',')[0]
    current_date = date.today()

    print(f'top_10_domain_zones for date {current_date}')
    print(top_domain_zones)

    print(f'Longest_domain for date {current_date}')
    print(longest_domain)
    
    if(len(rank) == 0):
        print('There is no data for airflow.com')
    else:
        print(f'Airflow rank for date {current_date}')
        print(rank)


# In[ ]:


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 18),
    'schedule_interval': '0 12 * * *'
}

dag = DAG('mgrigoryeva_domains_data', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain_zones',
                    python_callable=get_top_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_data',
                        python_callable=get_airflow_data,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

