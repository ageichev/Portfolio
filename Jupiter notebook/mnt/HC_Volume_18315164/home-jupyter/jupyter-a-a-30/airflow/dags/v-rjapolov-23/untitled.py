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


def get_top_10_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_group'] = top_data_df['domain'].str.split('.')[-1]
    top_data_top_10 = top_data_df.groupby('domain_group', as_index = False).agg({'domain': 'count'}).sort_values('domain', ascending=False)
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df['domain'].str.split('').apply(lambda x: len(x))
    longest_domain = top_data_df.sort_values('domain_len', ascending=False).head(1).domain
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_top_10_com.csv', 'r') as f:
        all_data_com = f.read()
    date = ds

    print(f'Top domains in .RU for date {date}')
    print(all_data)

    print(f'Top domains in .COM for date {date}')
    print(all_data_com)

default_args = {
    'owner': 'v-rjapolov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 19),
    'schedule_interval': '0 2 * * *'
}

dag = DAG('v-rjapolov-lesson_2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_dom',
                    python_callable=get_stat,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_longest_domain',
                    python_callable=get_stat,
                    dag=dag)


t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_1] >> t3