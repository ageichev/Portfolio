import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import re

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

        
def get_domain_zone_stat():
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    pattern = re.compile('\w+$')
    top_domain_df['zone'] = top_domain_df['domain'].apply(lambda x: pattern.findall(x)[0])
    top_10_domain_zone = top_domain_df \
                            .groupby('zone', as_index=False) \
                            .agg({'domain': 'count'}) \
                            .sort_values('domain', ascending=False) \
                            .head(10)
    
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))

        
def get_longest_domain_name():
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_df['domain_length'] = top_domain_df['domain'].apply(lambda x: len(x))
    longest_domain_name = top_domain_df.sort_values(['domain_length', 'domain'], ascending=[False, True]).head(1).reset_index().domain
    
    with open('longest_domain_name.csv', 'w') as f:
        f.write(str(longest_domain_name))
        
def get_airflow_rank():
    top_domain_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_domain_df.query('domain == "airflow.com"')
    
    if airflow_rank.shape[0] == 0:
        printed_str = 'Site "airflow.com" not in this list'
    else:
        printed_str = airflow_rank.rank

    with open('airflow_position.csv', 'w') as f:
        f.write(str(printed_str))

def print_data():
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_domain_name.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_position.csv', 'r') as f:
        airflow_rank = f.read()    

    print(f'Top 10 domain zones')
    print(top_10)

    print(f'The longest domain name')
    print(longest_name)

    print(f'"airflow.com" domain rank')
    print(airflow_rank)

default_args = {
    'owner': 'd-soziashvili-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 30),
}
schedule_interval = '0 14 * * *'

dag = DAG('dag_by_david_soziashvili', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_domain_zone_stat',
                    python_callable=get_domain_zone_stat,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_longest_domain_name',
                        python_callable=get_longest_domain_name,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_1, t2_2, t2_3] >> t3