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


def get_top_10_domains():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    domains = top_data_df.domain.apply(lambda x: x.split('.')[-1]).reset_index()
    top_10_domains = domains.groupby('domain', as_index = False)\
                            .agg({'index' : 'count'})\
                            .rename(columns={'index' : 'count'})\
                            .sort_values('count', ascending = False).head(10)
    with open('top_10_domains.csv', 'w') as f:
        f.write(top_10_domains.to_csv(index=False, header=False))


def get_top_length():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: len(x))
    top_length = top_data_df.sort_values('domain_len', ascending = False)[['domain', 'domain_len']].head(1)
    with open('top_length.csv', 'w') as f:
        f.write(top_length.to_csv(index=False, header=False))


def get_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_len'] = top_data_df.domain.apply(lambda x: len(x))
    airflow = top_data_df.sort_values('domain_len', ascending = False)\
                         .reset_index()\
                         .query('domain == "airflow.com"')[['domain', 'domain_len']]
    with open('airflow.csv', 'w') as f:
        f.write(airflow.to_csv(index=True, header=False))


def print_data(ds):
    with open('top_10_domains.csv', 'r') as f:
        all_data = f.read()
    with open('top_length.csv', 'r') as f:
        all_data_long = f.read()
    with open('airflow.csv', 'r') as f:
        airflow = f.read()
    date = ds

    print(f'Top 10 domains for date {date}')
    print(all_data)

    print(f'The longest domain for date {date}')
    print(all_data_long)
    
    print(f'Airflow place for date {date}')
    print(airflow)
    
    


default_args = {
    'owner': 'a-kebekbaeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 25),
}
schedule_interval = '5 10 * * *'

dag_a_kebekbaeva = DAG('dag_a_kebekbaeva_top_10_longest_airflow', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag_a_kebekbaeva)

t2 = PythonOperator(task_id='get_top_10_domains',
                    python_callable=get_top_10_domains,
                    dag=dag_a_kebekbaeva)

t3 = PythonOperator(task_id='get_top_length',
                    python_callable=get_top_length,
                    dag=dag_a_kebekbaeva)

t4 = PythonOperator(task_id='get_airflow',
                    python_callable=get_airflow,
                    dag=dag_a_kebekbaeva)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag_a_kebekbaeva)

t1 >> [t2, t3, t4] >> t5