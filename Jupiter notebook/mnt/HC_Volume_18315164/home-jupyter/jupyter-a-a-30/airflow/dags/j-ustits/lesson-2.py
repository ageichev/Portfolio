import requests
import pandas as pd
from datetime import timedelta, datetime

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


def get_top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_region'] = top_data_df \
        .domain \
        .apply(lambda x: x.split('.')[-1])

    top_10_domains = top_data_df \
        .groupby('domain_region', as_index=False) \
        .agg({'domain': 'nunique'}) \
        .sort_values('domain', ascending=False) \
        .head(10) \
        .domain_region

    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))


def get_max_len_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df \
        .domain \
        .apply(lambda x: len(x))

    max_len_domains = top_data_df \
        .query("len_domain == len_domain.max()") \
        .sort_values('domain')[['rank', 'domain']] \
        .reset_index() \
        .domain

    with open('max_len_domains.csv', 'w') as f:
        f.write(max_len_domains.to_csv(index=False, header=False))


def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domain'] = top_data_df \
        .domain \
        .apply(lambda x: len(x))

    airflow = top_data_df \
        .sort_values(['len_domain', 'domain'], ascending=[False, True]) \
        .reset_index(drop=True) \
        .query("domain == 'airflow.com'") \
        .index

    with open('airflow.txt', 'w') as f:
        f.write(str(-1 if len(airflow) == 0 else airflow[0]))


def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        top_10_domains = f.read()

    with open('max_len_domains.csv', 'r') as f:
        max_len_domains = f.read()
    max_len_domains = max_len_domains.split('\n')[0]

    with open('airflow.txt', 'r') as f:
        airflow = int(f.read())

    date = ds

    print(f'Top domains region for date {date}')
    print(top_10_domains.replace('\n', ', ')[:-2])

    print(f'Domain with max len for date {date}')
    print(max_len_domains)

    print(f'Position airflow.com for date {date}')
    print('not in domains' if airflow < 0 else airflow)


default_args = {
    'owner': 'j.ustits',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 14),
}
schedule_interval = '10 13 * * *'

dag = DAG('j_ustits', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len_domains',
                    python_callable=get_max_len_domains,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
