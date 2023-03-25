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


def get_top_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_domain_10 = top_data_df.domain.apply(lambda x: x.split('.')[-1]).value_counts().head(10)
    
    with open('top_domain_10.csv', 'w') as f:
        f.write(top_domain_10.to_csv(header=False))


def get_long_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    long_name = top_data_df[top_data_df.domain.str.len() == max(top_data_df.domain.str.len())]['domain'].values[0]
    
    with open('long_name.txt', 'w') as f:
        f.write(long_name)

def get_airflow():
    try:
        top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
        airflow_rank = top_data_df.query('domain == "airflow.com"')['rank'].values[0]
    except:
        airflow_rank = 'No domain'    
    
    with open('airflow_rank.txt', 'w') as f:
        f.write(airflow_rank)   


def print_data(ds):
    with open('top_domain_10.csv', 'r') as f:
        top_domain_10 = f.read()
    with open('long_name.txt', 'r') as f:
        long_name = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top domains for date {date}')
    print(top_domain_10)

    print(f'Long name domain for date {date}')
    print(long_name)
    
    print(f'Airflow rank for date {date}')
    print(airflow_rank)


default_args = {
    'owner': 'v.sokerin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=7),
    'start_date': datetime(2022, 10, 15),
}
schedule_interval = '0 10 * * *'

dag = DAG('dag_v_sokerin', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_domain',
                    python_callable=get_top_domain,
                    dag=dag)

t3 = PythonOperator(task_id='get_long_name',
                        python_callable=get_long_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow',
                        python_callable=get_airflow,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
