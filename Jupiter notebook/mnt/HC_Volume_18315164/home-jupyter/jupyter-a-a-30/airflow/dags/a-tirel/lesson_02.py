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


## Найти топ-10 доменных зон по численности доменов 

def get_top_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_top_10 = top_data_df.domain.str.rsplit('.', n=1, expand=True).rename(columns={1: 'domain'}) \
    .groupby('domain', as_index=False) \
    .agg({0: 'count'}) \
    .sort_values(0, ascending=False) \
    .reset_index()
    top_data_top_10 = top_data_top_10.domain.head(10)
    with open('top_data_top_10.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))


## Найти домен с самым длинным именем (если их несколько, то взять только первый в алфавитном порядке)

def longest_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name_lenght'] = top_data_df['domain'].str.split('.', n=1).str[0].str.len()
    longest_name = top_data_df.loc[top_data_df['name_lenght'] == top_data_df['name_lenght'].max()].sort_values('domain').reset_index().domain[0]
    with open('longest_name.txt', 'w') as f:
        f.write(longest_name)
        
        
## На каком месте находится домен airflow.com

def airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    rank = str(top_data_df.loc[top_data_df.domain == 'airflow.com', 'rank']).split(' ')[0]
    airflow = ''
    if rank.startswith("S"):
        airflow = airflow + 'not available today'
    else:
        airflow = airflow + rank
    with open('airflow.txt', 'w') as f:
        f.write(airflow)


def print_data(ds):
    with open('top_data_top_10.csv', 'r') as f:
        all_data = f.read()
    with open('longest_name.txt', 'r') as f:
        all_data_longest_name = f.read()
    with open('airflow.txt', 'r') as f:
        all_data_airflow = f.read()    
    date = ds

    print(f'Top domains for date {date}')
    print(all_data)

    print(f'Longest domain name for date {date}')
    print(all_data_longest_name)
    
    print(f'Airflow rank in list for date {date}')
    print(all_data_airflow)


default_args = {
    'owner': 'a.tirel',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 6, 30),
}
schedule_interval = '0 20 * * *'

dag = DAG('a_tirel_stats', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domains',
                    python_callable=get_top_domains,
                    dag=dag)

t3 = PythonOperator(task_id='longest_name',
                    python_callable=longest_name,
                    dag=dag)

t4 = PythonOperator(task_id='airflow',
                    python_callable=airflow,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5