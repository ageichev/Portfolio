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


def get_top_10_domain_zones():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])

    def get_domain_zone(line):
        domain = line.split('.')
        return domain[1]

    top_doms['domain_zone'] = top_doms.domain.apply(get_domain_zone)
    top_zones = top_doms.groupby('domain_zone').agg({'domain_zone': 'count'}).rename(columns={'domain_zone': 'count_domain_zone'}).sort_values('count_domain_zone', ascending=False).reset_index().head(10)

    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))


def get_longest_domain():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['domain_len'] = top_doms.domain.apply(lambda x: len(x))
    top_doms = top_doms.sort_values('domain')
    max_len = top_doms.domain_lenght.max()
    longest_domain = top_doms.query('domain_len == @max_len')

    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))


def get_airflow_rank():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_doms.query('domain == "airflow.com"')

    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
    with open('longest_domain', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank', 'r') as f:
        airflow_rank = f.read()
    date = ds

    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

    print(f'Top 10 domain zones for date {date}:')
    print(top_10_domain_zones)

    print(f'Longest domain name for date {date}:')
    print(longest_domain)

    print(f'Airflow.com rank for date {date}:')
    print(airflow_rank)

default_args = {
    'owner': 'k.eremina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 26, 3),
}
schedule_interval = '0 12 * * *'

dag = DAG('k-eremina', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag)

t2 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t4 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4

