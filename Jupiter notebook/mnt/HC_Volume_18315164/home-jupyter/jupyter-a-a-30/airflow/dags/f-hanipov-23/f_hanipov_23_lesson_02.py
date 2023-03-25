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


def get_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_zones = top_data_df.groupby('zone', as_index=False) \
                           .agg({'domain':'count'}) \
                           .sort_values('domain', ascending=False).head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))

def find_longest():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_id = [top_data_df.domain.str.len().idxmax()]
    max_length_domain = sorted(top_data_df.domain[max_id])[0]
    with open('longest_domain.csv', 'w') as f:
            f.write(max_length_domain)

def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        rank = top_data_df.query('domain == "airflow.com"')['rank'].values[0]
        message = f'airflow.com is ranked {rank}'
    except:
        message = 'Sorry, airflow.com is not in top domains list'
    with open('airflow_rank.csv', 'w') as f:
        f.write(message)


def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        all_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds
    
    print(f'Top 10 domain zones for date {date}')
    print(all_zones)

    print(f'The longest domain is {longest_domain} for date {date}')
    
    print(f'{airflow_rank} for date {date}')


default_args = {
    'owner': 'f-hanipov-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 24),
}
schedule_interval = '0 15 * * *'

dag = DAG('f_hanipov_23_lesson_02', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zones',
                    python_callable=get_zones,
                    dag=dag)

t3 = PythonOperator(task_id='find_longest',
                        python_callable=find_longest,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5