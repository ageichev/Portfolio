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

def top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1].lower())
    top_10_domain_zone = top_data_df.groupby('domain_zone', as_index=False) \
                                    .agg({'domain': 'count'}) \
                                    .rename(columns={'domain': 'domain_num'}) \
                                    .sort_values('domain_num', ascending=False) \
                                    .head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))

def longest_dom_name(ds):
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name_length'] = top_data_df['domain'].apply(lambda x: len(x))
    the_longest_domain_name = top_data_df.sort_values(by=['name_length', 'domain'], ascending=[False, True])['domain'].values[0]
    
    date = ds
    
    print(f'The longest domain name for date {date}: {the_longest_domain_name}')

def airflow_rank(ds):
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[(top_data_df['domain'].str.startswith('airflow')) & 
                               (top_data_df['domain'].str.endswith('com'))]['rank'].values.min()
    
    date = ds
    
    print(f'The "Airflow" rank for date {date}: {airflow_rank}')

def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_domain_data = f.read()
        
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_domain_data)

default_args = {
    'owner': 'a.sahabutdinov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 10, 28),
}

schedule_interval = '40 0 * * *'

dag = DAG('Domain_analysis_by_Aidar', 
          default_args=default_args, 
          schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zone',
                    python_callable=top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='longest_dom_name',
                    python_callable=longest_dom_name,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t3 >> t4 >> t2 >> t5