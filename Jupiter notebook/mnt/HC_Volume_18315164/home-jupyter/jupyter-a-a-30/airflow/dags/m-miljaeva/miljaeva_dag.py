#!/usr/bin/env python
# coding: utf-8

# In[9]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[10]:



TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[11]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[4]:


def get_stat_10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


# In[35]:


def get_stat_long():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df['domain'].str.len()
    top_data_df = top_data_df.sort_values('domain').sort_values('len', ascending=False)
    longest_name = top_data_df.head(1)
    
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))


# In[53]:


def get_stat_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_airflow = top_data_df[top_data_df['domain']== 'airflow.com']
    with open('data_airflow.csv', 'w') as f:
        f.write(data_airflow.to_csv(index=False, header=False))


# In[6]:


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('data_airflow.csv', 'r') as f:
        data_airflow = f.read()
    date = ds

    print(f'Top domains for date {date}')
    print(top_10)

    print(f'The longest name for date {date}')
    print(longest_name)
    
    if len(data_airflow) >=1:   
        print(f'The airflow.com rank for date {date}')
        print(data_airflow)
    else:
        print('airflow.com is not in the list')


# In[7]:


default_args = {
    'owner': 'm-miljaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 24),
}
schedule_interval = '15 11 * * *'

dag = DAG('domains_miljaeva', default_args=default_args, schedule_interval=schedule_interval)


# In[54]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_10',
                    python_callable=get_stat_10,
                    dag=dag)

t2_long = PythonOperator(task_id='get_stat_long',
                        python_callable=get_stat_long,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_stat_airflow',
                        python_callable=get_stat_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_long, t2_airflow] >> t3

