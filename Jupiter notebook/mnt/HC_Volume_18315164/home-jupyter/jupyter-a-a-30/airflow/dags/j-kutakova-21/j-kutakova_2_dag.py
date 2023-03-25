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

# 1. top-10 zone
def get_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df['domain'].str.split('.').str[-1]
    df = (
        df
            .groupby('zone', as_index=False)
            .agg({'domain': 'count'}).sort_values(by='domain', ascending=False).head(10)['zone']
    )
    df.reset_index(drop=True, inplace=True)
    with open('count_domain_in_zone.csv', 'w') as f:
        f.write(df.to_csv(index=False, header=False))

# 2. max_length name of domain
def get_max_length_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_length'] = (
        df['domain']
            .str.split('.')
            .str[0]
            .apply(lambda x: len(x))
    )
    df = (
        df[df['domain_length'] == df['domain_length'].max()]
            .sort_values(by='domain', ascending=False).head(1)
    )['domain']
    with open('max_length_domain.csv', 'w') as f:
        f.write(df.to_csv(index=False, header=False))

# 3. airflow_rank
def get_airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = df[df['domain'] == 'airflow.com']
    if airflow_rank.shape[0] > 0:
        airflow_rank = airflow_rank['rank']
        with open('airflow_rank.csv', 'w') as f:
            f.write(airflow_rank.to_csv(index=False, header=False))
    else:
        airflow_rank = 'no data for airflow.com'
        with open('airflow_rank.csv', 'w') as f:
            f.write(airflow_rank)

def print_data(ds):  # передаем глобальную переменную airflow
    with open('count_domain_in_zone.csv', 'r') as f:
        count_domain_file = f.read()
    with open('max_length_domain.csv', 'r') as f:
        max_length_file = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_file = f.read()
    date = ds
    print(f'Top 10 domains for date {date} is')
    print(count_domain_file)
    print(f'The longest domain name for date {date} is')
    print(max_length_file)
    print(f'The rank of airflow.com for {date} is')
    print(airflow_rank_file)

# create dag
default_args = {
    'owner': 'j-kutakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': '0 9 * * *'
}
dag = DAG('j-kutakova-21', default_args=default_args)

# create tasks
t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
t2_zone = PythonOperator(task_id='get_zone',
                         python_callable=get_zone,
                         dag=dag)
t2_length = PythonOperator(task_id='get_max_length_domain',
                           python_callable=get_max_length_domain,
                           dag=dag)
t2_airflow = PythonOperator(task_id='get_airflow_rank',
                            python_callable=get_airflow_rank,
                            dag=dag)
t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

# create queue
t1 >> [t2_zone, t2_length, t2_airflow] >> t3
