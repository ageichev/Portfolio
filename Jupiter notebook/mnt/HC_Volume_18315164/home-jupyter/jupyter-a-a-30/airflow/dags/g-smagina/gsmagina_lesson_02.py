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

def get_top_10_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].str.split('.').str[-1]
    top_10_domain_zones = top_data_df\
        .groupby('domain_zone', as_index=False)\
        .agg({'rank':'count'})\
        .sort_values('rank', ascending=False)\
        .head(10)
    
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))

def get_longest_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['letters'] = top_data_df.domain.apply(lambda x: len(x))
    max_letters = top_data_df.letters.max()
    longest_domain = top_data_df \
        .sort_values('letters', ascending=False)[['domain', 'letters']] \
        .query('letters == @max_letters') \
        .sort_values('domain', ignore_index=True) \
        .head(1)['domain'][0]

    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain)

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']

    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))
 
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_10_domain_zones)

    print(f'Longest_domain for date {date}')
    print(longest_domain)

    print(f'Rank "airflow.com" {airflow_rank} for date {date}')


default_args = {
    'owner': 'g-smagina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 15)
}
dag = DAG('top_10_ru_new_gsmagina', schedule_interval='0 12 * * *', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_domain_zones',
                    python_callable=get_top_10_domain_zones,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest_domain',
                        python_callable=get_longest_domain,
                        dag=dag)

t4 = PythonOperator(task_id='get_airflow_rank',
                    python_callable=get_airflow_rank,
                    dag=dag)

t5= PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


t1 >> [t2, t3, t4] >> t5

