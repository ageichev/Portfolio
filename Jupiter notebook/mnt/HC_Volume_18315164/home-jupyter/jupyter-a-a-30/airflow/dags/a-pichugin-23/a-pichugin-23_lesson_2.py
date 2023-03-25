import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_domains():
    top_dom = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_dom['top_domains'] = top_dom.apply(lambda row: row.domain.split('.')[-1], axis=1)
    top_10_dom = top_dom.groupby('top_domains', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10)
    with open('a_pichugin_23_top_10_domains.csv', 'w') as f:
        f.write(top_10_dom.to_csv(index=False, header=False))


def get_longest_domain():
    top_dom = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_dom['domain_length'] = top_dom.apply(lambda row: len(row.domain), axis=1)
    domain_length = top_dom.sort_values(['domain_length', 'domain'], ascending=[False, True]).iloc[0].domain
    with open('a_pichugin_23_longest_domain.txt', 'w') as f:
        f.write(domain_length)


def get_airflow_rank():
    try:
        top_dom = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        airflow_rank = str(top_dom[top_dom.domain == 'airflow.com']['rank'].values[0])
    except:
        airflow_rank = 'No domain'
    with open('a_pichugin_23_airflow_rank.txt', 'w') as f:
         f.write(str(airflow_rank))


def print_data(ds):
    with open('a_pichugin_23_top_10_domains.csv', 'r') as f:
        top_10_dom = f.read()

    with open('a_pichugin_23_longest_domain.txt', 'r') as f:
        longest_domain = f.read()

    with open('a_pichugin_23_airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()

    date = ds

    print(f'Top10 domain for date {date}')
    print(top_10_dom)

    print(f'Longest domain for date {date}')
    print(longest_domain)

    print(f'Airflow rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'a-pichugin-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 21),
    'schedule_interval': '0 8 * * *'
}

dag = DAG('a-pichugin-23_lesson_02', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_longest_domain,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5

