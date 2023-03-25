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


def get_top_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_zones_df = top_data_df['domain'].str.split('.', -1, expand=True).groupby(1, as_index=False).agg({0: 'count'}).sort_values(0, ascending=False).head(10).rename(columns={1: 'zone', 0: 'count'})
    with open('top_zones_df.csv', 'w') as f:
        f.write(top_zones_df.to_csv(index=False, header=False))


def get_longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len = int(top_data_df['domain'].str.len().max())
    longest_name_df = top_data_df[top_data_df['domain'].str.len() == max_len]
    with open('longest_name_df.csv', 'w') as f:
        f.write(longest_name_df.to_csv(index=False, header=False))

def air_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    air_rank_df = top_data_df[top_data_df['domain']=='airflow.com']
    with open('air_rank_df.csv', 'w') as f:
        f.write(air_rank_df.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_zones_df.csv', 'r') as f:
        data_top_zones = f.read()
    with open('longest_name_df.csv', 'r') as f:
        data_longest_name = f.read()
    with open('air_rank_df.csv', 'r') as f:
        data_air_rank = f.read()
    date = ds

    print(f'Top domain zones for date {date}')
    print(data_top_zones)

    print(f'Longest name for date {date}')
    print(data_longest_name)
    
    print(f'Rank airflow.com for date {date}')
    print(data_air_rank)


default_args = {
    'owner': 'e_muraveva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 25),
}
schedule_interval = '0 09 * * *'

dag = DAG('e_muraveva_dag', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data_e_muraveva',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_zones_e_muraveva',
                    python_callable=get_top_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name_e_muraveva',
                        python_callable=get_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='air_rank_e_muraveva',
                    python_callable=air_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data_e_muraveva',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
