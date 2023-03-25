import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_domains = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_domains.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top10_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) # сразу называем колонки
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1]) # делаем колонку с зоной домена
    top_10_zone = top_data_df.groupby('zone', as_index=False) \
        .agg({'domain': 'count'}) \
        .rename(columns={'domain': 'count'}) \
        .sort_values('count', ascending=False)\
        .head(10)
    
    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10_zone.to_csv(index=False, header=False))


def get_longest_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df['domain'].apply(lambda x: len(x))
    longest_domain_name = top_data_df[top_data_df['length'] == top_data_df['length'].max()] \
        .sort_values('domain').head(1)
        
    with open('longest_domain_name.csv', 'w') as f:
        f.write(longest_domain_name.domain.to_csv(index=False, header=False))
        

def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')
    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.domain.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_zone.csv', 'r') as f:
        all_data_top_zone = f.read()
        
    with open('longest_domain_name.csv', 'r') as f:
        all_data_longest_name = f.read()
        
    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow = f.read()
        
    date = ds

    print(f'Top zone by domain counts for date {date}')
    print(all_data_top_zone)

    print(f'Longest domain name for {date}')
    print(all_data_longest_name)
    
    if len(all_data_airflow) != 0:
        print(f'Airflow has rank for {date}')
        print(all_data_airflow)
    else:
        print('There is no airflow.com in data')


default_args = {
    'owner': 'a-nosach',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 25),
}
schedule_interval = '0 12 * * *'

dag = DAG('a-nosach_lesson2HW', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='get_top10_zone',
                    python_callable=get_top10_zone,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_longest_domain_name',
                        python_callable=get_longest_domain_name,
                        dag=dag)

t2_airflow = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top10, t2_longest, t2_airflow] >> t3
