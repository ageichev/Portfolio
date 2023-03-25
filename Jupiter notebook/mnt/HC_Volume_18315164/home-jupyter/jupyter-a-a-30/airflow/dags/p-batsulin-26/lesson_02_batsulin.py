import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
   
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.str.split('.', 1).str.get(1)
    top_data_zones = top_data_df.groupby(['domain_zone'], as_index=False)\
                                .agg({'rank':'count'})\
                                .sort_values('rank', ascending=False).head(10)\
                                .rename(columns={"rank": "number"})
    with open('top_data_zones.csv', 'w') as f:
        f.write(top_data_zones.to_csv(index=False, header=False))

def get_longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_name']=top_data_df.domain.str.split('.', 1).str.get(0)
    top_data_longest = top_data_df[top_data_df.domain_name.str.len() == max(top_data_df.domain_name.str.len())]['domain'].values[0]
    with open('top_data_longest.csv', 'w') as f:
        f.write(top_data_longest)

def get_airflow_com():
    try:
        top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        top_data_airflow = str(top_data_df[top_data_df.domain == 'airflow.com']['rank'].values[0])
    except:
        top_data_airflow = 'Not in the list'
    with open('top_data_airflow.csv', 'w') as f:
        f.write(top_data_airflow)

def print_data(ds):
    with open('top_data_zones.csv', 'r') as f:
        all_data_zones = f.read()
    with open('top_data_longest.csv', 'r') as f:
        all_data_longest = f.read()
    with open('top_data_airflow.csv', 'r') as f:
        all_data_airflow = f.read()   
        
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(all_data_zones)

    print(f'The longest domain name for date {date}')
    print(all_data_longest)
    
    print(f'The rank of airflow.com for date {date}')
    print(all_data_airflow)

default_args = {
    'owner': 'pbatsulin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 14),
}
schedule_interval = '15 9 * * *'

dag = DAG('airflow_pbatsulin', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zones = PythonOperator(task_id='get_zones',
                    python_callable=get_zones,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_com',
                        python_callable=get_airflow_com,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zones, t2_longest, t2_airflow] >> t3