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


def get_top_10_domains():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['domain_zone'] = data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domains = data_df.groupby('domain_zone', as_index=False).agg({'rank':'count'}).sort_values('rank', ascending=False).head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))

def get_longest_name():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['len'] = data_df['domain'].apply(lambda x: len(x))
    longest_domain_name = data_df.sort_values('len', ascending=False).head(1)['domain']
    with open('longest_domain_name.csv', 'w') as f:
        f.write(longest_domain_name.to_csv(index=False, header=False))
        
def get_airflow_rank():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = data_df[data_df['domain']=='airflow.com']['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        data_domains = f.read()
    with open('longest_domain_name.csv', 'r') as f:
        data_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        data_airflow = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(data_domains)

    print(f'The longest domain name for date {date}')
    print(data_length)
    
    print(f'The rank of domain "airflow.com" for date {date}')
    print(data_airflow)


default_args = {
    'owner': 'v_misjura_20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 6)
}
schedule_interval = '0 16 * * *'

dag_v_misjura_lesson_2 = DAG('dag_v_misjura_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

task1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_v_misjura_lesson_2)

task2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    dag=dag_v_misjura_lesson_2)

task3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag_v_misjura_lesson_2)

task4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag_v_misjura_lesson_2)

task5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_v_misjura_lesson_2)

task1 >> [task2, task3, task4] >> task5
