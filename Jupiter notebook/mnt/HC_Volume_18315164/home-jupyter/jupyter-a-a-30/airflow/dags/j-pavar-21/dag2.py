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


def get_top10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df.domain.apply(lambda x: x.split('.')[-1])
    top10_dz = top_data_df.domain_zone.value_counts().head(10)
#     print(top10_dz)
    with open('top10_dz.csv', 'w') as f:
            f.write(top10_dz.to_csv(index=False, header=False))


def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.str.len()
    max_len = top_data_df.len.max()
    longest_domains = top_data_df.loc[top_data_df.len == max_len].sort_values('domain').domain
#     print(longest_domains)
    with open('longest_domains.csv', 'w') as f:
            f.write(longest_domains.to_csv(index=False, header=False))


def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_airflow = top_data_df.loc[top_data_df.domain == 'airflow.com'] 
    if len(rank_airflow) != 0:
        answ = str(rank_airflow['rank'])
    else:
        answ = 'There is no such domain for today'
    return answ


def print_data(ds):
    with open('top10_dz.csv', 'r') as f:
        top10_dz_data = f.read()
    with open('longest_domains.csv', 'r') as f:
        longest_domains_data = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top10_dz_data)

    print(f'Longest domains for date {date}')
    print(longest_domains_data)
    
    print(f'Airflow rank for date {date}')
    print(get_airflow_rank())



default_args = {
    'owner': 'y.pavar',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 3),
}
schedule_interval = '0 8 * * *'

dag = DAG('domains_info_j_pavar_21', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top10_domain_zone',
                    python_callable=get_top10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5