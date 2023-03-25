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


def top10_regions():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_region'] = top_data_df['domain'].str.split('.').str[-1]
    top10_domain_reg=top_data_df\
        .groupby('domain_region', as_index=False)\
        .agg(domains_in_region=('domain','count'))\
        .sort_values(by='domains_in_region', ascending=False)\
        .head(10)
    with open('top10_domain_reg.csv', 'w') as f:
        f.write(top10_domain_reg.to_csv(index=False, header=False))


def longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name_length']=top_data_df['domain'].str.len()
    longest_domain_name=top_data_df.sort_values(by=['name_length','domain'], ascending=False).head(1)
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain_name.to_csv(index=False, header=False))

def airflow_place():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_data_df.query('domain=="airflow.com"').shape[0]!=0:
        rank=top_data_df.query('domain=="airflow.com"')['rank'].max()
        airflow_rank = f'Airflow rank is {rank}'
    else:
        airflow_rank = 'There is no airflow in top :('
    with open('airflow_rank.txt', 'w') as f:
        f.write(airflow_rank)


def print_data(ds):
    with open('top10_domain_reg.csv', 'r') as f:
        top10_domain_reg = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 domain regions for date {date}')
    print(top10_domain_reg)

    print(f'Domain with longest name in top for date {date}')
    print(longest_domain)
    
    print(f'For date {date} {airflow_rank}')


default_args = {
    'owner': 's-fomenko-22',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 7, 31),
}
schedule_interval = '10 6 * * *'

dag = DAG('s_fomenko_22', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top10_regions',
                    python_callable=top10_regions,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain',
                        python_callable=longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_place',
                        python_callable=airflow_place,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5