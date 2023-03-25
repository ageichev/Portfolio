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


def top10_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain'] = df['domain'].apply(lambda x: x.split('.')[-1])
    top10_zone = df.groupby('domain', as_index = False)\
                    .agg({'rank': 'count'})\
                    .sort_values('rank', ascending = False)\
                    .head(10)
    with open('top10_zone.csv', 'w') as f:
        f.write(top10_zone.to_csv(index=False, header=False))


def longest_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df['domain'].apply(lambda x: len(x))
    longest_name = df.sort_values('length', ascending=False).head(1).domain
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))


def airflow_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df1 = df.query('domain == "airflow.com"')
    if df1.empty:
        airflow_position = 'is not in the top 1M domains'
    else:
        airflow_position = f'position in the top 1M domains {df1.index[0]+1}'
    with open('airflow_domain.txt', 'w') as f:
        f.write(str(airflow_position))


def print_data(ds):
    with open('top10_zone.csv', 'r') as f:
        top10_zone = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_domain.txt', 'r') as f:
        airflow_domain = f.read()
    date = ds

    print(f'Top 10 domains zones for date {date}')
    print(top10_zone)

    print(f'Longest name domain for date {date}')
    print(longest_name)
    
    print(f'airflow.com {date}')
    print(airflow_domain)


default_args = {
    'owner': 'd-donisov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 29),
}
schedule_interval = '30 21 * * *'

dag = DAG('DAG_ddonisov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top10_zone',
                    python_callable=top10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='longest_name',
                    python_callable=longest_name,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_domain',
                    python_callable=airflow_domain,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

