import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_and_add_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS,names=['rank', 'domain'], compression='zip') # Считали данные, распаковали, назвали колонки 
    top_doms['domain_zone'] = top_doms.domain.apply(lambda x: x.split('.')[1]) # добавили колонку с названием доменной зоны
    top_doms.insert(3,"domain_length", top_doms.domain.apply(lambda x: len(x))) # добавили колонку с длиной названия домена
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_TOP10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    top10_domains_df = top_data_df.groupby('domain_zone', as_index=False)\
                                  .agg({'domain':'count'})\
                                  .sort_values('domain', ascending=False)\
                                  .rename(columns={'domain':'frequency'})\
                                  .head(10)
    with open('top10_domains.csv', 'w') as f:
        f.write(top10_domains_df.to_csv(index=False, header=False))

def get_longest_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    longest_domain = top_data_df.loc[top_data_df.domain_length == top_data_df.domain_length.max()]\
                                .sort_values('domain')\
                                .head(1)\
                                .domain\
                                .to_csv(index=False, header=False)\
                                .replace('\r','')\
                                .replace('\n','')
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain)
    
def get_rank_for_airflow_dot_com():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE)
    rank_airflow = top_data_df.query('domain=="airflow.com"')['rank'].to_csv(index=False, header=False).replace('\r','').replace('\n','')
    if len(rank_airflow)==0:
        rank_airflow='Not in Top-1m'
    with open('rank_airflow_dot_com.csv', 'w') as f:
        f.write(rank_airflow)

def print_data(ds):
    with open('top10_domains.csv', 'r') as f:
        all_data_top10_domains = f.read()
    with open('longest_domain.csv', 'r') as f:
        all_data_longest_domain = f.read()
    with open('rank_airflow_dot_com.csv', 'r') as f:
        all_data_rank_airflow_dot_com = f.read()
    date = ds

    print(f'Top 10 domains for date {date} are:')
    print(all_data_top10_domains)

    print(f'The longest domain name for date {date} is')
    print(all_data_longest_domain)

    print(f'Rank for Airflow.com for date {date} is')
    print(all_data_rank_airflow_dot_com)

default_args = {
    'owner': 'i-grebenjuk-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 29),
}

schedule_interval = '00 08 * * *'

dag = DAG('Lesson_2_task_igorG', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_and_add_data',
                    python_callable=get_and_add_data,
                    dag=dag)

t2_top10 = PythonOperator(task_id='get_TOP10_domains',
                    python_callable=get_TOP10_domains,
                    dag=dag)

t2_longest = PythonOperator(task_id='get_longest_domain_name',
                        python_callable=get_longest_domain_name,
                        dag=dag)

t2_rank = PythonOperator(task_id='get_rank_for_airflow_dot_com',
                        python_callable=get_rank_for_airflow_dot_com,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top10, t2_longest, t2_rank] >> t3
