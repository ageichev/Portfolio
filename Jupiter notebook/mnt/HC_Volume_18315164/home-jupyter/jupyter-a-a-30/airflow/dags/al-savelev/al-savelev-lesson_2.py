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

def get_top_10():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10 = data_df.domain.head(10)
    with open('top_10.csv', 'w') as f:
        f.write(top_10.to_csv(index=False, header=False))

def get_longest_name():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    data_df['name_len'] = data_df.domain.apply(len)
    longest_name = data_df.query('name_len == name_len.max()').domain
    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))

def get_airflow_rank():
    data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if data_df.query('domain == "airflow.com"').empty:
        airflow_rank = 'airflow.com is not in top domains'
    else:
        airflow_rank = data_df.query('domain == "airflow.com"').rank
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank)

def print_data(ds):
    with open('top_10.csv', 'r') as f:
        top_10 = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()   
        
    date = ds

    print(f'Top 10 domains for date {date}')
    print(top_10)

    print(f'The longest domain name for date {date}')
    print(longest_name)

    print(f'The airflow.com rank in top domains for date {date}')
    print(airflow_rank)
    
default_args = {
    'owner': 'al-savelev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 20),
}
schedule_interval = '0 12 * * *'

dag = DAG('dag_savelev_les_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_name,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5
