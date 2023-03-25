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


def get_top_domain_group():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_group'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_domain_group_10 = top_data_df.groupby('domain_group', as_index=False) \
        .agg({'domain': 'count'}) \
        .sort_values(by='domain', ascending=False).head(10)
    with open('top_domain_group_10.csv', 'w') as f:
        f.write(top_domain_group_10.to_csv(index=False, header=False))


def get_top_domain_max_lengh():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_lengh'] = top_data_df['domain'].apply(lambda x: len(x))
    top_domain_max_lengh = top_data_df.sort_values(by='domain_lengh', ascending=False).head(1)
    with open('top_domain_max_lengh.csv', 'w') as f:
        f.write(top_domain_max_lengh.to_csv(index=False, header=False))


def get_rank_of_airflow_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_of_airflow_com = top_data_df.query('domain == "airflow.com"')
    if rank_of_airflow_com.empty == False:
        with open('rank_of_airflow_com.csv', 'w') as f:
                f.write(rank_of_airflow_com.to_csv(index=False, header=False))
    else:
        return 'Domain airflow.com is not found!'


def print_data(ds):
    with open('top_domain_group_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_domain_max_lengh.csv', 'r') as f:
        all_data_top = f.read()
    if get_rank_of_airflow_com() != 'Domain airflow.com is not found!':
        with open('rank_of_airflow_com.csv', 'r') as f:
            all_data_rank = f.read()
    date = ds

    print(f'Top domain groups by number of domains for date {date}')
    print(all_data)

    print(f'Domain with the longest name for date {date}')
    print(all_data_top)

    if get_rank_of_airflow_com() == 'Domain airflow.com is not found!':
        print('Domain airflow.com is not found!')
    else:
        print(f'Rank of domain airflow.com for date {date}')
        print(all_data_rank)


default_args = {
    'owner': 'a-vostokova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 11, 17),
}
schedule_interval = '0 12 * * *'

dag = DAG('top_domain_a_vostokova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain_group',
                    python_callable=get_top_domain_group,
                    dag=dag)

t3 = PythonOperator(task_id='get_top_domain_max_lengh',
                        python_callable=get_top_domain_max_lengh,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_of_airflow_com',
                        python_callable=get_rank_of_airflow_com,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
