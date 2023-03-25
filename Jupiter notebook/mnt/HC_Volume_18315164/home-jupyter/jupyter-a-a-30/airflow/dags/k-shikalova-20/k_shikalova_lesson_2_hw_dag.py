import requests 
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True) 
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')
    
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_10_zones():  
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    top_data_df['zone'] = top_data_df.domain.apply(lambda x: x.split('.')[1])
    top_10_zones = top_data_df \
                        .groupby('zone', as_index=False) \
                        .agg({'rank': 'count'}) \
                        .sort_values('rank', ascending=False) \
                        .head(10)['zone']
    
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))
        
def domain_long():
    
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: len(x))
    max_len = top_data_df.domain_len.max()
    domain_long = top_data_df.query('domain_len == @max_len').sort_values('domain').head(1).domain
    
    with open('domain_long.csv', 'w') as f:
        f.write(domain_long.to_csv(index=False, header=False))
        
def airflow_rank():
        
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    airflow_rank = top_data_df.query('domain == "airflow.com"')['rank']
    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds):
    
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
        
    with open('domain_long.csv', 'r') as f:
        domain_long = f.read()
        
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
        
    date = ds

    print(f'Top-10 domain zones for date {date}')
    print(top_10_zones)

    print(f'The longest domain for date {date}')
    print(domain_long)

    print(f'Airflow.com rank for date {date}')
    print(airflow_rank)    

default_args = {
    'owner': 'k-shikalova-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 23),
    'schedule_interval': '30 23 * * *'
}

dag = DAG('k_shikalova_lesson_2_hw', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_zones',
                    python_callable=top_10_zones,
                    dag=dag)

t3 = PythonOperator(task_id='domain_long',
                    python_callable=domain_long,
                    dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5