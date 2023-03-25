import requests
import pandas as pd
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
		
def get_domain_split():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df[['zone', 'domain']] = top_data_df['domain'].str.rsplit('.', 1, expand=True)
    with open('top_data_domain.csv', 'w') as f:
        f.write(top_data_df.to_csv(index=False))

def get_domain_split():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df[['domain','zone']] = top_data_df['domain'].str.rsplit('.', 1, expand=True)
    with open('top_data_domain.csv', 'w') as f:
        f.write(top_data_df.to_csv(index=False))

def get_top_zone():
    top_data_df = pd.read_csv('top_data_domain.csv', low_memory = False)
    top_zone  = top_data_df\
                    .groupby('zone',as_index = False)\
                    .agg({'domain':'count'})\
                    .sort_values(by = 'domain', ascending = False)\
                    .zone.iloc[0]
    with open('top_zone.txt', 'w') as f:
        f.write(top_zone)
		
def get_longest_domain():
    top_data_df = pd.read_csv('top_data_domain.csv', low_memory = False)
    longest_d = top_data_df['domain'].str.len().max()
    top_data_df['domain_len'] = top_data_df['domain'].str.len()
    longest_df = top_data_df.query('domain_len == @longest_d').domain.to_list()
    string = ', '.join([str(longest_df) for domain in longest_df])
    with open('longest_domain.txt', 'w') as f:
        f.write(string)
		
def get_airflow_pos():
    top_data_df = pd.read_csv('top_data_domain.csv', low_memory = False)
    aiflow_pos = top_data_df.query('domain == "airflow" and zone == "com" ')['rank'].to_list()
    with open('aiflow_pos.txt', 'w') as f:
        f.write(str(aiflow_pos))
		
def print_data(ds):
    with open('aiflow_pos.txt', 'r') as f:
        airflow_pos = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('top_zone.csv', 'r') as f:
        top_zone = f.read()
    date = ds

    print(f'Airflow-group position to date {date} is')
    print(airflow_pos)

    print(f'longest domain name to date {date} is')
    print(longest_domain)
    
    print(f'Most used domain zones to date {date} are')
    print(top_zone)

default_args = {
    'owner': 'e-sjakaev-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '0 12 * * *'

dag = DAG('e-sjakaev-22_dag1', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_domain_split',
                    python_callable=get_domain_split,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_zone',
                        python_callable=get_top_zone,
                        dag=dag)

t4 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t5 = PythonOperator(task_id='get_airflow_pos',
                        python_callable=get_airflow_pos,
                        dag=dag)

t6 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> [t3, t4, t5] >> t6