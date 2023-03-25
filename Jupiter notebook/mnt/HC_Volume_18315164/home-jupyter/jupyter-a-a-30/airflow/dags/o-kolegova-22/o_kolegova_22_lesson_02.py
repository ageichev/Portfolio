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


def get_stat_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.str.split('.').apply(lambda x: x[1])
    top_10_domain_zone = top_data_df.groupby('zone', as_index=False).domain.count().rename(
        columns={'domain':'domain_count'}
    ).sort_values(by='domain_count', ascending=False).head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))


def get_stat_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['length'] = top_data_df.domain.str.len()
    longest_domain = top_data_df.sort_values(by=['length', 'domain'], ascending=[False, True]).head(1).iloc[0, 1]
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain)


def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')
    if len(airflow_rank) == 0:
        result = 'airflow.com not exist in data'
    else:
        result = 'airflow.com rank is {}'.format(airflow_rank.loc[0, 'rank'])
    with open('airflow_rank.csv', 'w') as f:
        f.write(result)


def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_10_domain_zone = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()

    date = ds

    print(f'Top domains zone for date {date}:')
    print(top_10_domain_zone)

    print(f'Longest domain for date {date}:', longest_domain)
    print()
    
    print(airflow_rank, f'for date {date}')


default_args = {
    'owner': 'o-kolegova-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 30),
}
schedule_interval = '0 23 * * *'

dag = DAG('o-kolegova-22_top_10', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_domain_zone = PythonOperator(task_id='get_stat_domain_zone',
                    python_callable=get_stat_domain_zone,
                    dag=dag)

t2_longest_domain = PythonOperator(task_id='get_stat_longest_domain',
                        python_callable=get_stat_longest_domain,
                        dag=dag)

t2_rank_airflow = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_domain_zone, t2_longest_domain, t2_rank_airflow] >> t3
