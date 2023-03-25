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


def get_top_10_tld():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['TLD'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top_10_tld = top_data_df.groupby('TLD', as_index=False)\
                            .agg({'domain': 'count'})\
                            .sort_values('domain', ascending=False).head(10)
    with open('top_10_tld.csv', 'w') as f:
        f.write(top_10_tld.to_csv(index=False, header=False))


def get_longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    leight = top_data_df.domain.str.len().sort_values(ascending = False).index
    the_longest_domain =top_data_df.reindex(leight).head(1)
    with open('the_longest_domain.csv', 'w') as f:
        f.write(the_longest_domain.to_csv(index=False, header=False))


def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_tld.csv', 'r') as f:
        top_10_tld = f.read()
    with open('the_longest_domain.csv', 'r') as f:
        the_longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 top-level domains for date {date}')
    print(top_10_tld)

    print(f'The longest domain for date {date}')
    print(the_longest_domain)

    print(f'Airflow.com rank for date {date}')
    print(airflow_rank)

default_args = {
    'owner': 'a-nikonova-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 17),
}
schedule_interval = '10 12 * * *'

dag = DAG('a-nikonova-24', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_tld',
                    python_callable=get_top_10_tld,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest',
                        python_callable=get_longest,
                        dag=dag)
t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)                        

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

