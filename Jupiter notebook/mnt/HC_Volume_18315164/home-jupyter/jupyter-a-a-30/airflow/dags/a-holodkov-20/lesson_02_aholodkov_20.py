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

def get_top_10_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain_zones = top_data_df['domain_zone'].value_counts().reset_index().head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))

def get_longest_name_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length_name'] = top_data_df['domain'].str.len()
    longest_name_domain = top_data_df.sort_values('domain') \
                        .sort_values('length_name', ascending=False) \
                        .head(1)['domain']
    with open('longest_name_domain.csv', 'w') as f:
        f.write(longest_name_domain.to_csv(index=False, header=False))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query("domain.str.contains('airflow.com')")['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))  
        
def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_name_domain.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10)

    print(f'Longest domain name for date {date}')
    print(longest_name)

    print(f'Airflow.com rank for date {date}:')
    print(airflow)

    
    

default_args = {
    'owner': 'a.holodkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 12),
}
schedule_interval = '0 12 * * *'

dag_holodkov = DAG('domain_stats_holodkov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_holodkov)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag_holodkov)

t3 = PythonOperator(task_id='get_longest_len_domain',
                        python_callable=get_longest_name_domain,
                        dag=dag_holodkov)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag_holodkov)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_holodkov)

t1 >> [t2, t3, t4] >> t5