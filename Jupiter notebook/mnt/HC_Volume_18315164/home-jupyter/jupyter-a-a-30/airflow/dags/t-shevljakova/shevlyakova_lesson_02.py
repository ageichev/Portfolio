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


def get_len_domen():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_domen']=top_data_df['domain'].str.len()
    max_len=top_data_df.sort_values('domain').sort_values('len_domen', ascending=False).head(1)['domain']
    with open('max_len_name_domen.csv', 'w') as f:
        f.write(max_len.to_csv(index=False, header=False))

def get_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone']=top_data_df.domain.str.split('.').str[1:].apply(lambda x: '.'.join(x))
    top_zone=top_data_df.groupby('zone', as_index=False)\
                .agg({'domain':'count'}).rename(columns={'domain':'count'})\
                .sort_values('count', ascending=False).head(10)['zone']
    with open('top_data_zone_10.csv', 'w') as f:
        f.write(top_zone.to_csv(index=False, header=False))

def get_rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank_airflow=top_data_df.query('domain == "airflow.com"')['rank']
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False))

def print_data(ds):
    with open('max_len_name_domen.csv', 'r') as f:
        all_data_len = f.read()   
    with open('top_data_zone_10.csv', 'r') as f:
        all_data_zone = f.read()
    with open('rank_airflow.csv', 'r') as f:
        all_rank_airflow = f.read()
    date = ds

    print(f'Max len domen for date {date}')
    print(all_data_len)

    print(f'Top zones domens for date {date}')
    print(all_data_zone)
    
    print(f'Rank airflow for date {date}')
    print(all_rank_airflow)


default_args = {
    'owner': 't-shevljakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 17),
}
schedule_interval = '0 9 * * *'

dag = DAG('t-shevljakova_lesson_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_len_domen',
                    python_callable=get_len_domen,
                    dag=dag)

t3 = PythonOperator(task_id='get_zone',
                        python_callable=get_zone,
                        dag=dag)

t4 = PythonOperator(task_id='get_rank_airflow',
                        python_callable=get_rank_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

