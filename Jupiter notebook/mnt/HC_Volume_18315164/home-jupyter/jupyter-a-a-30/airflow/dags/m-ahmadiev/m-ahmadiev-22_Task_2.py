#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[2]:



TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['zone'] = [val[-1] for val in top_doms.domain.str.split('.')]
    top_10_domain = top_doms.groupby('zone', as_index=False)     .agg({'rank': 'count'})     .rename(columns={'rank': 'count'})     .sort_values('count', ascending=False)     .head(10)
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

def get_longest_name():
    longest_name_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    longest_name_df['length_name'] = longest_name_df['domain'].apply(lambda x: x.split('.')[0]).str.len()
    longest_name = longest_name_df.sort_values('domain').sort_values('length_name', ascending = False).head(1)['domain']
    with open('top_longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header = False))
                       
def get_airflow_position():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_doms.query('domain == "airflow.com"')[['rank', 'domain']].shape[0] > 0:
        airflow_rank = top_doms.query('domain == "airflow.com"')[['rank', 'domain']]
    else:
        airflow_rank = top_doms.loc[top_doms.domain.str.contains("airflow")][['rank', 'domain']]
    with open('airflow_position.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('data_top_10.csv', 'r') as f:
        data_top_10 = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_position = f.read()
    date = ds

    print(f'Top 10 domains zones for {date}')
    print(data_top_10)

    print(f'Longest name of domain for {date}')
    print(longest_name)

    print(f'Airflow position for {date} is')
    print(airflow_position)




default_args = {
    'owner': 'm-ahmadiev-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 24),
}
schedule_interval = '0 10 * * *'

dag = DAG('m-ahmadiev-22', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)
                       
t2_longest = PythonOperator(task_id='get_longest_name',
                    python_callable=get_longest_name,
                    dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_position',
                    python_callable=get_airflow_position,
                    dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_longest, t2_airflow] >> t3


# In[ ]:





# In[ ]:




