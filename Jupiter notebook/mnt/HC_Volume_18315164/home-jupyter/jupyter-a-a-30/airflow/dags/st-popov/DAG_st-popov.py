#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[ ]:



TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_ten_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_top_10_domain_zone = top_data_df.groupby('domain_zone', as_index=False)         .agg({'domain': 'count'})         .sort_values('domain', ascending=False)         .head(10)
    
    with open('top_ten_domain_zones.csv', 'w') as f:
        f.write(top_ten_domain_zones.to_csv(index=False, header=False))


def longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].apply(len)
    top_data_df[['domain', 'domain_length']].sort_values('domain_length', ascending=False)         .reset_index()         .loc[0]['domain']
    
    
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))

def rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    with open('rank_airflow.csv', 'w') as f:
        if top_data_df[top_data_df['domain'] == 'airflow.com'].empty
            f.write('airflow.com is not found')
        else:
            rank_airflow = top_data_df[top_data_df['domain'] == 'airflow.com']['rank']
            f.write(rank_airflow)

def print_data(ds):
    with open('top_ten_domain_zones.csv', 'r') as f:
        all_data = f.read()
    with open('longest_name.csv', 'r') as f:
        all_data_com = f.read()
    with open('rank_airflow.csv', 'r') as f:
        rank_airflow = f.read()        
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_ten_domain_zones)

    print(f'Longest domain name for date {date}')
    print(longest_name)

    print(f'Rank of Airflow.com for date {date}')
    print(rank_airflow)    

default_args = {
    'owner': 's.petrov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2023, 1, 3),
}
schedule_interval = '0 12 * * *'

dag = DAG('st-petrov_new_DAG', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_ten_domain_zones',
                    python_callable=get_stat,
                    dag=dag)

t2_length = PythonOperator(task_id='longest_name',
                        python_callable=get_stat_com,
                        dag=dag)

t2_rank = PythonOperator(task_id='rank_airflow',
                        python_callable=get_stat_com,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_length, t2_rank] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)

