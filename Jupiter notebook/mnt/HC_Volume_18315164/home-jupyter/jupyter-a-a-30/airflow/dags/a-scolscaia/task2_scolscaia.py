import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m-a-scolscaia.csv'

# Lesson 2

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

        
def get_stat_top10_numbers():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df['domain'].str.split('.').str[-1]
    top10_numbers = df['zone'].value_counts().head(10)
    with open('scolscaia_top10_numbers.csv', 'w') as f:
        f.write(top10_numbers.to_csv(index=False, header=False))


def get_stat_long():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_long = iloc[df['zone'].str.len().idxmax()]
    with open('scolscaia_top_long.csv', 'w') as f:
        f.write(top_long.to_csv(index=False, header=False))


def get_stat_airflow():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domain_airflow = df.query('domain == "airflow.com"').index[0]
    with open('scolscaia_domain_airflow.csv', 'w') as f:
        f.write(domain_airflow.to_csv(index=False, header=False))


def print_data(ds): # передаем глобальную переменную airflow
    with open('scolscaia_top10_numbers.csv', 'r') as f:
        scolscaia_top10_numbers = f.read()
    with open('scolscaia_top_long.csv', 'r') as f:
        scolscaia_top_long = f.read()
    with open('scolscaia_domain_airflow.csv', 'r') as f:
        scolscaia_domain_airflow = f.read()
    date = ds

    print(f'Top 10 domains by numbers for date {date}')
    print(scolscaia_top10_numbers)

    print(f'Top domain with longest name for date {date}')
    print(scolscaia_top_long)
    
    print(f'Place of airflow.com for date {date}')
    print(scolscaia_domain_airflow)


default_args = {
    'owner': 'a_scolscaia',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 1),
    'schedule_interval': '0 15 * * *'
}

dag = DAG('a_scolscaia_task_2', default_args=default_args,
         schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_stat_top10_numbers',
                    python_callable=get_stat,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_stat_long',
                        python_callable=get_stat_com,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_stat_airflow',
                        python_callable=get_stat_com,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> t2_1 >> [t2_2, t2_3] >> t3
