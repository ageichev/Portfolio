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


def get_top_dom_zones():
    zones = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    zones['zone'] = zones.domain.str.split(pat=".").str[-1]
    top_zones = zones.groupby('zone', as_index = False).agg({'domain': 'count'}).rename(columns = {'domain': 'domain_count'}).sort_values('domain_count', ascending = False)
    top_zones = top_zones.head(10)
    with open('top_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(index=False, header=False))


def get_longest_name():
    df_long = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df_long['len'] = df_long.domain.str.len()
    df_long = df_long.sort_values('domain').reset_index()
    longest_dm = df_long.iloc[df_long.len.idxmax()]['domain']
    with open('longest_dm.csv', 'w') as f:
        f.write(longest_dm.to_csv(index=False, header=False))
  

def get_airflow():
    top_domain = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_com = top_domain[top_domain['domain'].str.fullmatch('airflow.com')]
    with open('airflow_com.csv', 'w') as f:
        f.write(airflow_com.to_csv(index=False, header=False))


def print_data(ds):
    with open('top_zones.csv', 'r') as f:
        zones_data = f.read()
    with open('longest_dm.csv', 'r') as f:
        long_data = f.read()
    with open('airflow_com.csv', 'r') as f:
        airflow_data = f.read()
    date = ds

    print(f'Top domains by zone for date {date}')
    print(zones_data)

    print(f'Longest domain name for date {date}')
    print(long_data)
    
    print(f'Rank for airflow.com for date {date}')
    print(airflow_data)


default_args = {
    'owner': 'a-antonova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 24),
}
schedule_interval = '30 11 * * *'

dag = DAG('top_domains_a_antonova', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_dom_zones',
                    python_callable=get_top_dom_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
