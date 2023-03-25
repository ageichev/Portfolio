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


def get_top_10_domain_zones():
    # Найти топ-10 доменных зон по численности доменов
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain_zones = top_data_df['zone'].value_counts().reset_index().head(10)
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))


def get_longest_length_domain():
    # Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length_name'] = top_data_df['domain'].str.len()
    longest_length_domain = top_data_df.sort_values(['length_name', 'domain'], ascending=[False, True])[['domain', 'length_name']].head(1)['domain']
    with open('longest_length_domain.csv', 'w') as f:
        f.write(longest_length_domain.to_csv(index=False, header=False))

def get_airflow_rank():
    # На каком месте находится домен airflow.com?
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query("domain.str.contains('airflow.com')")['rank']
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('longest_length_domain.csv', 'r') as f:
        longest_length = f.read()
    with open('airflow_rank.csv', 'r') as f:
        rank_airflow = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_zones)

    print(f'Longest domain name for date {date}')
    print(longest_length)

    print(f'Airflow.com rank for date {date}:')
    print(rank_airflow)


default_args = {
    'owner': 'a-bajagantaev-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 4),
}
schedule_interval = '0 17 * * *'

a_bajagantaev_21 = DAG('domain_info_a-bajagantaev-21', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=a_bajagantaev_21)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=a_bajagantaev_21)

t3 = PythonOperator(task_id='get_longest_length_domain',
                    python_callable=get_longest_length_domain,
                    dag=a_bajagantaev_21)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=a_bajagantaev_21)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=a_bajagantaev_21)

t1 >> t2 >> t3 >> t4 >> t5 