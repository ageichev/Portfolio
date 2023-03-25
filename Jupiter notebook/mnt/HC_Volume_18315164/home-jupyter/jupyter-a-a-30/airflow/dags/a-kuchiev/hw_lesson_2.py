#!/usr/bin/env python
# coding: utf-8

# In[7]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_data_top_10 = top_data_df.domain.value_counts().sort_values(ascending=False).head(10).reset_index()
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_top_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].apply(lambda x: len(x.split('.')[0]))
    max_len = top_data_df.sort_values(by='domain_length', ascending=False).domain_length.max()
    top_len = top_data_df.loc[top_data_df['domain_length']==max_len]
    with open('top_len.csv', 'w') as f:
        f.write(top_len.to_csv(index=False, header=False))

        
def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df.domain == 'airflow.com']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        top_10_domains = f.read()
    with open('top_len.csv', 'r') as f:
        top_len_domain = f.read()
    with open('airflow_rank.csv','r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 domains zone for date {date} :')
    print(top_10_domains)

    print(f'The longest domain for date {date} :')
    print(top_len_domain)

    print(f'The airflow rank for date {date} :')
    if len(airflow_rank) > 0:
        print(airflow_rank)
    else:
        print('No airflow.com domain found')


default_args = {
    'owner': 'a-kuchiev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 17),
}
schedule_interval = '0 14 * * *'

dag = DAG('airflow_hw_a_kuchiev', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain',
                    python_callable=get_top_10_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_len_domain',
                        python_callable=get_top_len_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5


# In[ ]:




