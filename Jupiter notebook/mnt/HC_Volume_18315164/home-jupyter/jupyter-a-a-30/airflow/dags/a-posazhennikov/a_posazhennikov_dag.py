# ---importing libraries

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# --- data loading

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

# --- Tasks

def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def count_top10_zone():
    all_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['place', 'domain'])
    all_data_df['zone'] = all_data_df.domain.apply(lambda x: x.split('.')[-1])
    biggest_10_domain = all_data_df.groupby('zone', as_index = False)\
        .agg({'domain':'count'})\
        .sort_values('domain', ascending = False)\
        .rename(columns = {'domain':'amount'})\
        .head(10)
   
    with open('biggest_10_domain.csv', 'w') as f:
        f.write(biggest_10_domain.to_csv(index=False, header = False))
        
def find_longest_name():
    all_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['place', 'domain'])
    all_data_df['length'] = all_data_df.domain.apply(lambda x: len(x.split('.')[0]))
    longest_domen = all_data_df.loc[all_data_df.length == all_data_df.length.max()].domain.min()
    return longest_domen
    


def get_airfolw_rank():
    all_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['place', 'domain'])
    return all_data_df.loc[all_data_df.domain == "airflow.com"]['place'].values


def print_data(ds): # передаем глобальную переменную airflow
    with open('biggest_10_domain.csv', 'r') as f:
        all_data = f.read()
    
    date = ds

    print(f'Top ten domain zones for date {date}:')
    print()
    print(all_data)

    print(f'The longest domain name for date {date}:')
    print()
    print(find_longest_name())
    print()
    
    print(f'Airflow rank for date {date}:')
    print()
    print(*get_airfolw_rank())

# --- Initialising DAG

default_args = {
    'owner': 'a-posazhennikov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 10)
##    'schedule_interval': '*/5 * * * *'
}
dag = DAG('a_pos_request', default_args=default_args, schedule_interval = '0 10 * * *') 

# --- Initialising tasks

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='count_top10_zone',
                    python_callable=count_top10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='find_longest_name',
                        python_callable=find_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airfolw_rank',
                        python_callable=get_airfolw_rank,
                        dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

# --- Making order of execution

t1 >> [t2, t3, t4] >> t5

