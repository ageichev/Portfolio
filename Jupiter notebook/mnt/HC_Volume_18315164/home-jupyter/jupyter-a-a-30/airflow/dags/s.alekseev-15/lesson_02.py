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
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-08')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
    f.write(top_data)

def get_top_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[1]
    top_data_df = top_data_df.groupby('zone', as_index = False).agg({'domain':'count'}.sort_values('domain', ascending = False)
    top_zone_df = top_data_df['zone'].head(10)
    with open('top_zone_csv', 'w') as f:
        f.write(top_zone_df.to_csv(index=False, header=False))

def get_long_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len'] = top_data_df.domain.str.len()
    the_longest_domain = top_data_df.sort_values('len', ascending = False)
    the_longest_domain = the_longest_domain['domain'].head(1)
    with open('the_longest_domain.csv', 'w') as f:
        f.write(the_longest_domain.to_csv(index=False, header=False))

def get_airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_rank = top_data_df[top_data_df['domain'].str.startswith('airflow.ie')]
    airflow_rank =  airflow_rank.head(1)
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

def print_data(ds): # передаем глобальную переменную airflow
    with open('top_zone_csv.csv', 'r') as f:
        all_data_zone = f.read()
    with open('the_longest_domain.csv', 'r') as f:
        all_data_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow = f.read()
    date = ds

    print(f'Top domains in .RU for date {date}')
    print(all_data_zone)

    print(f'Top domains in .COM for date {date}')
    print(all_data_domain)

    print(f'Top domains in .COM for date {date}')
    print(all_data_airflow)


default_args = {
    'owner': 's.alekseev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 02, 28),
    'schedule_interval': '0 01 * * *'
}
dag = DAG('s.alekseev-15sorry_dag', default_args=default_args)

t1 = PythonOperator(task_id='s.alekseev15get_data',
                    python_callable=get_data,
                    dag=dag)

t2_zone = PythonOperator(task_id='s.alekseev15get_top_zone',
                    python_callable=get_top_zone,
                    dag=dag)

t2_domain = PythonOperator(task_id='s.alekseev15get_long_domain',
                        python_callable=get_long_domain,
                        dag=dag)

t2_airflow = PythonOperator(task_id='s.alekseev15get_airflow_rank',
                        python_callable=get_airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='s.alekseev15print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_zone, t2_domain, t2_airflow] >> t3


