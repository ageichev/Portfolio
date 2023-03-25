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
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zones'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    domain_zones_top_10 = top_data_df.groupby('domain_zones', as_index=0).agg({'domain': 'count'}) \
                                     . rename(columns={'domain': 'domain_amount'}) \
                                     .sort_values('domain_amount', ascending=False) \
                                     .head(10)
    with open('domain_zones_top_10.csv', 'w') as f:
        f.write(domain_zones_top_10.to_csv(index=False, header=False))

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df.domain.apply(len)
    max_length = top_data_df.domain_length.max()
    longest_domain = top_data_df[top_data_df.domain_length == max_length].sort_values('domain').head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df.query("domain == 'airflow.com'").shape[0] != 0:
        rank_airflow = top_data_df.query("domain == 'airflow.com'")
    else:
        rank_airflow = pd.DataFrame({'rank': 'not found', 'domain': ['airflow.com']})
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False)) 

def print_data(ds):
    with open('domain_zones_top_10.csv', 'r') as f:
        zones_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain_data = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(zones_data)

    print(f'Longest domain name for date {date}')
    print(longest_domain_data)
    
    print(f'The rank of "airflow.com" for date {date}')
    print(airflow_rank_data)


default_args = {
    'owner': 'n-muravev-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 28),
    'schedule_interval': timedelta(days=1)
}

dag = DAG('les_2_ex_4', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t2_3 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3

#t1.set_downstream(t2_1)
#t1.set_downstream(t2_2)
#t1.set_downstream(t2_3)
#t2_1.set_downstream(t3)
#t2_2.set_downstream(t3)
#t2_3.set_downstream(t3)