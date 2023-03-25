import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def read_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_domains = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_domains.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_stat():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.ru')]
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def get_stat_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df[top_data_df['domain'].str.endswith('.com')]
    top_data_top_10 = top_data_top_10.head(10)
    with open('top_data_top_10_com.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


def print_data(ds):
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
    'owner': 'a-salomennikov-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 29),
}
schedule_interval = '0 12 * * *'

dag = DAG('a-salomennikov-22_01', default_args=default_args, schedule_interval=schedule_interval)

t1_asalomennikov = PythonOperator(task_id='read_data',
                                  python_callable=read_data,
                                  dag=dag)

t2_asalomennikov = PythonOperator(task_id='get_stat',
                                  python_callable=get_stat,
                                  dag=dag)

t2_com_asalomennikov = PythonOperator(task_id='get_stat_com',
                                      python_callable=get_stat_com,
                                      dag=dag)

t3_asalomennikov = PythonOperator(task_id='print_data',
                                  python_callable=print_data,
                                  dag=dag)

t1_asalomennikov >> [t2_asalomennikov, t2_com_asalomennikov] >> t3_asalomennikov

# t1.set_downstream(t2)
# t1.set_downstream(t2_com)
# t2.set_downstream(t3)
# t2_com.set_downstream(t3)
