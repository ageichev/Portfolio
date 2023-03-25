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

def get_top_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.replace(".*\.", "")
    top_10_domain_zones = top_data_df.groupby('domain_zone', as_index=False) \
                                .agg({'domain':'count'}) \
                                .sort_values('domain', ascending = False) \
                                .head(10)['domain_zone'].to_list()
    with open('top_10_domain_zones.txt', 'w') as f:
        for i in top_10_domain_zones:
            f.write("%s\n" % i)

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.map(lambda x: len(x))
    longest_domain = top_data_df.sort_values(by=['length', 'domain'], ascending = [False, True]) \
                                .head(1)['domain'] \
                                .item()
    with open('longest_domain.txt', 'w') as f:
        f.write(longest_domain)
        
def get_airflow_com_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_domain = top_data_df.query("domain == 'airflow.com'")
    airflow_rank = airflow_domain['rank'].item() if airflow_domain.shape[0] > 0 else 'There\'s no domain airflow.com'
    with open('airflow_com_rank.txt', 'w') as f:
        f.write(airflow_rank)

def print_data(ds):
    with open('top_10_domain_zones.txt', 'r') as f:
        top_10_domain_zones = f.read()
    with open('longest_domain.txt', 'r') as f:
        longest_domain = f.read()
    with open('airflow_com_rank.txt', 'r') as f:
        airflow_com_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_domain_zones)

    print(f'Longest domain name for date {date}')
    print(longest_domain, '\n')

    print(f'airflow.com rank for date {date}')
    print(airflow_com_rank)

default_args = {
    'owner': 'o-chemjakina-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 29),
}
schedule_interval = '30 17 * * *'

dag = DAG('o-chemjakina-25-domains', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zones = PythonOperator(task_id='get_top_zones',
                    python_callable=get_top_zones,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t2_airflow = PythonOperator(task_id='get_airflow_com_rank',
                        python_callable=get_airflow_com_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zones, t2_longest, t2_airflow] >> t3

