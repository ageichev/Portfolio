#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE.decode('utf-8'))

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top10():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top10_dz_df = top_data_df.groupby('domain_zone', as_index=False).agg({'rank':'count'}).sort_values('rank', ascending=False).head(10)
    
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top10_dz_df.to_csv(index=False, header=False))


def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df['domain'].apply(len)
    longest_domain = top_data_df[['domain', 'domain_len']].sort_values('domain_len', ascending=False).reset_index().loc[0]['domain']
    
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

def airflow_pos():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    
    with open('airflow_pos.csv', 'w') as f:
        if top_data_df[top_data_df['domain']=='airflow.com'].empty:
            f.write('airflow domain not found')
        else:
            airflow_pos = top_data_df[top_data_df['domain']=='airflow.com']['rank']
            f.write(airflow_rank)

def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top10_dz_df = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_pos.csv', 'r') as f:
        airflow_pos = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(top10_dz_df)

    print(f'The longest domain for date {date}')
    print(longest_domain)

    
    print(f'The position of airflow domain for date {date}')
    print(airflow_pos)

default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 20),
}
schedule_interval = '0 12 * * *'

dag = DAG('IvanH_Lesson2HW', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top10',
                    python_callable=top10,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_pos',
                        python_callable=airflow_pos,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

