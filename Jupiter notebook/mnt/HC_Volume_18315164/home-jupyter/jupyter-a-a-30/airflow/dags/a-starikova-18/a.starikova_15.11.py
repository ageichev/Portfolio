#!/usr/bin/env python
# coding: utf-8

# In[4]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime


# In[3]:


from airflow import DAG
from airflow.operators.python import PythonOperator


# In[1]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[4]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# In[5]:


def get_stat_zones():
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df['domain'].str.split('.').apply(lambda x: x[1]).value_counts()
    top_data_top_10 = pd.DataFrame(top_data_top_10).reset_index().rename(columns={'index':'domain_zone', 'domain':'number'})
    top_data_top_10 = top_data_top_10.head(10)
    
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


# In[6]:


def get_stat_length():
    
    top_length = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_length['name_length'] = top_length['domain'].apply(lambda x: len(x))
    top_length = top_length.sort_values('domain', ascending=True).sort_values('name_length', ascending=False)
    top_length = top_length.head(1)
    
    with open('top_length.csv', 'w') as f:
        f.write(top_length.to_csv(index=False, header=False))


# In[7]:


def get_airflow_rank():
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'].str.startswith('airflow.com')]
    if airflow_rank.shape[0] == 0:
        airflow_rank = 'No rank'
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


# In[8]:


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_data_top_10 = f.read()
    with open('top_data_length.csv', 'r') as f:
        top_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(top_data_top_10)

    print(f'The longest domain name for date {date}')
    print(top_length)
    
    print(f'Airflow domain rank for date {date}')
    print(airflow_rank)


# In[9]:


default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 23),
}
schedule_interval = '0 8 * * *'

dag = DAG('a.starikova_15.11', default_args=default_args, schedule_interval=schedule_interval)


# In[10]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_stat_zones',
                    python_callable=get_stat_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_stat_length',
                        python_callable=get_stat_length,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

