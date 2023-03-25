import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS_addr = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_domains = pd.read_csv(TOP_1M_DOMAINS_addr, names=['ranking', 'domain'])
    top_data = top_domains.to_csv(index=False)
    
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top10_numerous():
    df_numerous = pd.read_csv(TOP_1M_DOMAINS_FILE)
    df_numerous['domain_zone'] = df_numerous.domain.apply(lambda x: x.split('.')[-1])
    df_numerous_gr = df_numerous.groupby('domain_zone', as_index=False)            .agg({'ranking' : 'count'})            .sort_values(['ranking', 'domain_zone'], ascending=[False, True])            .reset_index(drop=True)            .head(10)

    with open('d_top_domain_zones.csv', 'w') as f:
        f.write(df_numerous_gr.to_csv(index=False, header=False))

def get_longest_name():
    df_long = pd.read_csv(TOP_1M_DOMAINS_FILE)
    df_long['domain_len'] = df_long.domain.str.len()
    df_long=df_long.sort_values(['domain_len', 'domain'], ascending = [False, True]).head(1)
    df_long=df_long['domain']
    
    with open('d_longest_name.csv', 'w') as f:
        f.write(df_long.to_csv(index=False, header=False))

def get_airflow_rank():
    df_about_airflow = pd.read_csv(TOP_1M_DOMAINS_FILE)
    df_about_airflow.head(10)
    df_about_airflow = df_about_airflow[df_about_airflow.domain=='airflow.com']
    if df_about_airflow.shape[0] == 0:
        new_row = {'ranking':'no data about airflow.com', 'domain':''}
        df_about_airflow = df_about_airflow.append(new_row, ignore_index=True)
    df_about_airflow = df_about_airflow['ranking']
    
    with open('d_airflow_rank.csv', 'w') as f:
        f.write(df_about_airflow.to_csv(index=False, header=False))

def print_data(ds):
    with open('d_top_domain_zones.csv', 'r') as f:
        top_domain_data = f.read()
    with open('d_longest_name.csv', 'r') as f:
        longest_name_data = f.read()
    with open('d_airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()
    date = ds

    print(f'Top domain zones by numerous for date {date}')
    print(top_domain_data)

    print(f'Top longest domain name for date {date}')
    print(longest_name_data)

    print(f'Airflow.com rank for date {date}')
    print(airflow_rank_data)


default_args = {
    'owner': 'd-badrtdinova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 23)
}
schedule_interval = '00 03 * * *'


badrt_dag = DAG('d-badrtdinova_lesson2', default_args=default_args, schedule_interval=schedule_interval)


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=badrt_dag)

t2_numerous = PythonOperator(task_id='get_top10_numerous',
                    python_callable=get_top10_numerous,
                    dag=badrt_dag)

t2_longest = PythonOperator(task_id='get_longest_name',
                    python_callable=get_longest_name,
                    dag=badrt_dag)

t2_airflow_rank = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=badrt_dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=badrt_dag)

t1 >> [t2_numerous, t2_longest, t2_airflow_rank] >> t3


