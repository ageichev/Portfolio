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


def get_largest_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df.apply(lambda row: row.domain.split('.')[-1], axis=1)
    res = df.groupby('zone', as_index=False).agg({'domain': 'count'}).sort_values('domain', ascending=False).head(10)
    with open('v_dvoeglazov_23_largest_zones.csv', 'w') as f:
        f.write(res.to_csv(index=False, header=False))


def get_longest_name():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['length'] = df.apply(lambda row: len(row.domain), axis=1)
    res = df.sort_values(['length', 'domain'], ascending=[False, True]).iloc[0].domain
    with open('v_dvoeglazov_23_longest_domain.txt', 'w') as f:
        f.write(res)


def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if df[df.domain == 'airflow.com'].shape[0]:
        result = df[df.domain == 'airflow.com'].iloc[0]['rank']
    else:
        result = 'No domain in file'
    with open('v_dvoeglazov_23_airflow_rank.txt', 'w') as f:
        f.write(str(result))


def print_data(ds):
    with open('v_dvoeglazov_23_largest_zones.csv', 'r') as f:
        largest_zones = f.read()

    with open('v_dvoeglazov_23_longest_domain.txt', 'r') as f:
        longest_domain = f.read()

    with open('v_dvoeglazov_23_airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()

    date = ds

    print(f'Top10 largest zones for date {date}')
    print(largest_zones)

    print(f'Longest domain name for date {date}')
    print(longest_domain)

    print(f'Airflow rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'v-dvoeglazov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 19),
    'schedule_interval': '0 10 * * *'
}

dag = DAG('v-dvoeglazov-23_lesson_02', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_largest_zones',
                    python_callable=get_largest_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                    python_callable=get_longest_name,
                    dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
