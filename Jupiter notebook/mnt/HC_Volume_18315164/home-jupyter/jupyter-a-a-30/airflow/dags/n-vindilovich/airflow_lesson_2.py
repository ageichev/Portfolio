#!/usr/bin/env python
# coding: utf-8

# In[10]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[11]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[12]:


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_domain():
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_df['tail'] = top_domain_df['domain'].str.split('.').str[-1]
    top_domain_top_10 = top_domain_df.groupby('tail', as_index = False)                     .count().sort_values('domain', ascending = False).head(10)['tail'].to_frame()
    with open('top_domain_top_10.csv', 'w') as f:
        f.write(top_domain_top_10.to_csv(index=False, header=False))


def get_longest_domain():
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_df['length'] = top_domain_df['domain'].str.len()
    df_longest_domain = top_domain_df.sort_values(['length', 'domain'], ascending = [False, True])['domain'].head(1).to_frame()
    with open('df_longest_domain.csv', 'w') as f:
        f.write(df_longest_domain.to_csv(index=False, header=False))
        
def airflow_place():
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    air_place = top_domain_df[top_domain_df['domain']=='airflow.com']['rank']
    with open('air_place.csv', 'w') as f:
        f.write(air_place.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_domain_top_10.csv', 'r') as f:
        all_data_top = f.read()
    with open('df_longest_domain.csv', 'r') as f:
        all_data_long = f.read()
    with open('air_place.csv', 'r') as f:
        all_data_air = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data_top)

    print(f'The longest domain for date {date}')
    print(all_data_long)
    
    print(f'Position of "airflow.com" for date {date}')
    print(all_data_air)


# In[14]:


default_args = {
    'owner': 'n-vindilovich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 17),
}
schedule_interval = '0 11 * * *'

dag = DAG('nvindilovich_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top = PythonOperator(task_id='get_top_domain',
                    python_callable=get_top_domain,
                    dag=dag)

t2_long = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_air = PythonOperator(task_id='airflow_place',
                        python_callable=airflow_place,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top, t2_long, t2_air] >> t3


# In[ ]:




