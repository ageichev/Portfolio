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


def top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domains'] = top_data_df['domain'].apply(lambda x: x.split('.')[1])
    top_10_domain_df = top_data_df \
                        .groupby('domains', as_index=False) \
                        .agg({'domain': 'count'}) \
                        .rename(columns={'domain': 'count_domains'}) \
                        .sort_values('count_domains', ascending=False) \
                        .head(10)

    with open('top_10_domain_df.csv', 'w') as f:
        f.write(top_10_domain_df.to_csv(index=False, header=False))


def longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name_length'] = top_data_df['domain'].apply(lambda x: len(x.split('.')[0]))
    longest_name_df = top_data_df \
                            .sort_values(['name_length', 'domain'], ascending={False, True}) \
                            [['domain', 'name_length']] \
                            .head(1)

    with open('longest_name_df.csv', 'w') as f:
        f.write(longest_name_df.to_csv(index=False, header=False))

def airflowcom_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df.query("domain == 'airflow.com'").shape[0] != False:
        airflowcom_rank_df = top_data_df.query("domain == 'airflow.com'")
    else:
        airflowcom_rank_df = pd.DataFrame({'rank': 'Not on the list', 'domain': ['airflow.com']})

    with open('airflowcom_rank_df.csv', 'w') as f:
        f.write(airflowcom_rank_df.to_csv(index=False, header=False))

def print_results(ds):
    with open('top_10_domain_df.csv', 'r') as f:
        top_10_domains_result = f.read()
    with open('longest_name_df.csv', 'r') as f:
        longest_name_result = f.read()
    with open('airflowcom_rank_df.csv', 'r') as f:
        airflowcom_rank_result = f.read()
    date = ds

    print(f'TOP-10 domains for date {date}')
    print(top_10_domains_result)

    print(f'The longest domain name for date {date}')
    print(longest_name_result)

    print(f'airflow.com rate for date {date}')
    print(airflowcom_rank_result)


default_args = {
    'owner': 't-romanenko-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 4)
    }
schedule_interval = '0 10 * * *'


dag = DAG('t-romanenko-21_dag1', default_args=default_args, schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domains',
                    python_callable=top_10_domains,
                    dag=dag)

t3 = PythonOperator(task_id='longest_name',
                    python_callable=longest_name,
                    dag=dag)

t4 = PythonOperator(task_id='airflowcom_rank',
                    python_callable=airflowcom_rank,
                    dag=dag)


t5 = PythonOperator(task_id='print_results',
                    python_callable=print_results,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5